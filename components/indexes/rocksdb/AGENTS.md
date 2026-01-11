# AGENTS.md: `index-rocksdb` Crate

## Architectural Intent
A **high-performance, embedded persistent storage backend** using RocksDB. It enables durable, serverless state management by mapping Drasi's storage interfaces to RocksDB's Key-Value model.

## Architectural Rules
*   **Async/Sync Bridge**: RocksDB API is synchronous. All interaction MUST be wrapped in `tokio::task::spawn_blocking` to avoid blocking the async runtime.
*   **Transactional Integrity**: Multi-key updates (e.g., Element + Indexes) MUST be wrapped in a RocksDB Transaction to ensure atomicity.
*   **Data Partitioning**: Data types MUST be segregated into distinct **Column Families** for performance tuning and isolation.

## Data Mapping Strategy
*   **Serialization**: Uses **Protocol Buffers (`prost`)** for efficient, schema-aware serialization of storage structs (`StoredElement`, `StoredValue`).
*   **Column Families**: All index types share a single `OptimisticTransactionDB` per query (opened via `open_unified_db()`), with 11 Column Families for data partitioning:
    *   **ElementIndex**: `elements`, `slots`, `inbound`, `outbound`, `partial`, `archive` (optional)
    *   **ResultIndex**: `values`, `sets`, `metadata`
    *   **FutureQueue**: `fqueue`, `findex`
*   **Concurrency**: Uses `OptimisticTransactionDB` with a shared `Arc` across all index types to manage concurrent updates without heavy locking.