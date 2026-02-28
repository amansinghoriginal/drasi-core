
# AGENTS.md: `index-garnet` Crate

## Architectural Intent
A **persistent storage backend** implementing the core interfaces (`ElementIndex`, `ResultIndex`, `FutureQueue`) using a Redis-compatible server (Garnet).

## Architectural Rules
*   **Protobuf Serialization**: All complex structs MUST be serialized using Protocol Buffers (`prost`) before storage.
*   **Session-Scoped Transactions**: All index writes MUST go through the `WriteBuffer` when a session is active. On commit, the buffer is drained into a single atomic Redis pipeline (`MULTI/EXEC`). There is no auto-commit fallback — writes without an active session error.
*   **Shared Connection**: All index types for a query share a single `MultiplexedConnection`, created once in `create_index_set()`.

## Session Architecture
*   **`GarnetSessionState`**: Holds a `MultiplexedConnection` (for commit) and `Mutex<Option<WriteBuffer>>`. All three index types share a single `Arc<GarnetSessionState>` so that one session spans all indexes atomically.
*   **`WriteBuffer`**: In-memory write buffer modeling Redis data types (String, Hash, Set, SortedSet) with delta-based tracking. Reads check the buffer first (`BufferReadResult::Found`/`KeyDeleted`/`NotInBuffer`) and fall through to Redis on miss. On commit, `drain_into_pipeline()` emits all buffered operations into one `MULTI/EXEC` pipeline.
*   **`BufferReadResult<T>`**: Three-state enum: `Found(T)` (buffer has it), `KeyDeleted` (explicitly deleted this session), `NotInBuffer` (fall through to Redis).
*   **`cleared_keys` tracking**: When a key is DEL'd then rewritten within one session, `cleared_keys` ensures `DEL` is emitted before the new state on commit, removing stale Redis members.
*   **`buffered_sadd` / `buffered_srem` pattern**: Index methods like `set_element` use helper methods that check the buffer, drop the `Mutex` guard for async Redis reads if needed, then re-acquire. This is safe because `ContinuousQuery::change_lock` serializes all processing for a given query.
*   **`GarnetSessionControl`**: Implements `SessionControl` trait. `begin` creates a `WriteBuffer`, `commit` drains it into an atomic pipeline, `rollback` drops the buffer.
*   **`peek_due_time()`**: Reads directly from Redis (not through the buffer) since it is a read-only polling operation used outside the change-processing lock.

## Data Mapping Strategy
*   **Cluster Compatibility**: All keys use Redis hash tags `{query_id}` to ensure all data for a query hashes to the same cluster slot.
*   **Elements**: Stored as Redis **Hashes** (`HSET`).
    *   Key: `ei:{<query_id>}:{source_id}:{element_id}`
    *   Fields: `e` (Protobuf data), `slots` (BitSet of affinities).
*   **Graph Structure**: Modeled with Redis **Sets** (`SADD`/`SREM`).
    *   Inbound/Outbound Keys: `ei:{<query_id>}:$in:...` / `ei:{<query_id>}:$out:...`.
*   **Result Index**: Stored as Redis key-value pairs and **Sorted Sets**.
    *   Accumulator Key: `ari:{<query_id>}:{set_id}`
    *   Metadata Keys: `metadata:{<query_id>}:sequence`, `metadata:{<query_id>}:source_change_id`
*   **Future Queue**: Stored as a Redis **Sorted Set** (`ZADD`/`ZPOPMIN`).
    *   Key: `fqi:{<query_id>}`
    *   Score: `due_time` (timestamp).
    *   Member: Protobuf `StoredFutureElementRef`.