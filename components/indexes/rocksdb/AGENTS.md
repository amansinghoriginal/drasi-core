# AGENTS.md: `index-rocksdb` Crate

## Architectural Intent
A **high-performance, embedded persistent storage backend** using RocksDB. It enables durable, serverless state management by mapping Drasi's storage interfaces to RocksDB's Key-Value model.

## Architectural Rules
*   **Async/Sync Bridge**: RocksDB API is synchronous. All interaction MUST be wrapped in `tokio::task::spawn_blocking` to avoid blocking the async runtime.
*   **Session-Scoped Transactions**: All index writes MUST occur within an active session transaction. The `with_txn()` helper errors if no session is active — there is no auto-commit fallback.
*   **Data Partitioning**: Data types MUST be segregated into distinct **Column Families** for performance tuning and isolation.

## Session Architecture
*   **`RocksDbSessionState`**: Holds `Arc<OptimisticTransactionDB>` and a `Mutex<Option<Transaction<'static>>>`. All three index types (`ElementIndex`, `ResultIndex`, `FutureQueue`) share a single `Arc<RocksDbSessionState>` so that one session transaction spans all indexes atomically.
*   **`with_txn()` pattern**: Every index read and write calls `session_state.with_txn(|txn| { ... })` which locks the mutex, passes the active transaction, and errors if none is active.
*   **Lifetime transmute**: The `Transaction` borrows the DB, but the struct needs to own both. The lifetime is transmuted to `'static` with a `Drop` impl that clears the transaction before the DB `Arc` is decremented. See safety comments in `session_state.rs`.
*   **`RocksDbSessionControl`**: Implements `SessionControl` trait, delegates to `RocksDbSessionState`. `begin`/`commit` use `spawn_blocking`; `rollback` is synchronous (safe for `Drop`).
*   **`peek_due_time()`**: Reads directly from the DB (not through the transaction) since it is a read-only polling operation used outside the change-processing lock.

## Data Mapping Strategy
*   **Serialization**: Uses **Protocol Buffers (`prost`)** for efficient, schema-aware serialization of storage structs (`StoredElement`, `StoredValue`).
*   **Column Families**: All index types share a single `OptimisticTransactionDB` per query (opened via `open_unified_db()`), with 11 Column Families for data partitioning:
    *   **ElementIndex**: `elements`, `slots`, `inbound`, `outbound`, `partial`, `archive` (optional)
    *   **ResultIndex**: `values`, `sets`, `metadata`
    *   **FutureQueue**: `fqueue`, `findex`
*   **Concurrency**: Uses `OptimisticTransactionDB` with a shared `Arc` across all index types. The active session transaction is shared via `Arc<RocksDbSessionState>` and accessed through `Mutex` locking.