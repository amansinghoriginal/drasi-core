# AGENTS.md: `core/src/query` Module

## Architectural Intent
The **runtime orchestration layer** for the engine. It binds together the disjointed components (solver, evaluator, storage) into a cohesive, running query instance.

## Architectural Rules
*   **Builder-First Construction**: `ContinuousQuery` is complex to instantiate. Users MUST use `QueryBuilder` to ensure all components are correctly wired and defaults are applied.
*   **Dependency Injection**: The builder serves as the composition root, allowing external implementations of `interface` traits (like storage backends) to be injected.
*   **Session-Scoped Atomicity**: Both `process_source_change()` and `process_due_futures()` wrap all index writes in a `SessionGuard` (begin/commit). A crash before commit rolls back all writes, ensuring persistent indexes remain consistent.
*   **Change Processing Logic**: While it delegates graph traversal and expression evaluation, `ContinuousQuery` IS RESPONSIBLE for:
    1.  **Delta Calculation**: Computing "Before" vs "After" states for updates/deletes.
    2.  **Concurrency Control**: Managing locking (`change_lock`) and state transitions for incoming changes.

## Core Components
*   **`QueryBuilder`**: Implements the Builder pattern to construct `ContinuousQuery` instances, handling the registration of built-in functions and default storage backends.
*   **`ContinuousQuery`**: The primary runtime object. It receives `SourceChange` events, orchestrates them through the middleware → solver → evaluator pipeline, and manages the lifecycle of the query. Key entry points:
    *   `process_source_change()` — processes a single source change within an atomic session.
    *   `process_due_futures()` — atomically pops a due future and processes it. Returns `DueFutureResult` containing results and the originating `source_id`.
*   **`DueFutureResult`**: Return type of `process_due_futures()`, carrying evaluation results and the `source_id` from the popped future's element reference (used by the `lib` crate for provenance metadata).
*   **`AutoFutureQueueConsumer`**: A background task that polls the `FutureQueue` via `peek_due_time()` and triggers `process_due_futures()` when items are due. Holds a `Weak<ContinuousQuery>` reference to avoid reference cycles.
