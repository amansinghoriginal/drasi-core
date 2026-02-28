# AGENTS.md: `core/src/interface` Module

## Architectural Intent
This module defines the **abstract contracts** (traits) that decouple the core engine from its dependencies. It embodies **Interface-Driven Design**, creating stable seams for pluggable backends (storage, timing, middleware).

## Architectural Rules
*   **Stability**: These traits are the "spine" of the system. Changes here are high-impact breaking changes.
*   **Abstraction**: Traits must define *capabilities*, not implementation details. They must be implementable by diverse backends (in-memory, RocksDB, Redis).
*   **Async First**: All I/O-bound operations must be `async` to support high-throughput, non-blocking runtimes.

## Core Contracts

### Storage Abstractions
*   **`ElementIndex`**: Abstract access to the graph structure (Nodes & Relations).
    *   *Constraint*: Must support efficient O(1) lookups for graph traversal (e.g., `get_slot_elements_by_inbound`).
*   **`ResultIndex`**: Abstract storage for query results and aggregation state.
    *   *Composition*: Composed of `AccumulatorIndex` (for aggregation values) and `ResultSequenceCounter` (for logical time).
*   **`ElementArchiveIndex`**: **Optional** capability for retrieving historical versions of elements, enabling temporal queries.
*   **`FutureQueue`**: Abstract scheduling for time-based operations.
    *   *Constraint*: Must support strict ordering by `due_time`.
    *   `pop()` atomically removes and returns the next due item; `peek_due_time()` is read-only for polling.
*   **`FutureQueueConsumer`**: Callback trait for the future queue polling loop. `on_items_due()` is called when items are due — the consumer calls `ContinuousQuery::process_due_futures()` to atomically pop and process within a session.

### Backend Plugin
*   **`IndexSet`**: Groups all index types (`ElementIndex`, `ElementArchiveIndex`, `ResultIndex`, `FutureQueue`) and a `SessionControl` into a single unit, enabling backends to create them from a shared resource.
*   **`IndexBackendPlugin`**: Trait that external storage backends implement. Exposes a single `create_index_set(query_id)` factory method to produce all indexes from one shared backend instance (e.g., one RocksDB database or one Redis connection).

### Session Lifecycle
*   **`SessionControl`**: Trait for backend-provided session/transaction control (`begin`/`commit`/`rollback`). In-memory backends use `NoOpSessionControl`.
*   **`SessionGuard`**: RAII wrapper over `SessionControl` that auto-rollbacks on drop if not committed. Consuming `commit()` prevents double-commit and use-after-commit.

### Extension Points
*   **`SourceMiddleware`**: Contract for data transformation plugins.
    *   *Pattern*: A pipeline stage that transforms one `SourceChange` into many.
*   **`SourceMiddlewareFactory`**: The "Builder" contract used by the registry to instantiate middleware from configuration.

### System Services
*   **`QueryClock`**: Abstracts "time" (transactional & real-world) to ensure deterministic execution and testing.
