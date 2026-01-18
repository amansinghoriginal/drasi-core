# Redb Index Backend for Drasi

A pure-Rust, embedded storage backend using [Redb](https://github.com/cberner/redb) for Drasi's index system.

## When to Use This Backend

**Choose Redb when:**
- Cross-compilation simplicity is required (pure `cargo build`, no C++ toolchain)
- Binary size must be minimal (~1-2MB vs ~15-30MB for RocksDB)
- Deploying to Edge/IoT devices with limited resources
- Power consumption matters (no background compaction threads)

**Choose RocksDB when:**
- High-throughput ingestion is required (cloud deployments, Kafka streams)
- Multi-writer concurrency is needed
- Maximum performance is the priority

## Trade-offs

| Aspect | Redb | RocksDB |
|--------|------|---------|
| Binary size | ~1-2 MB | ~15-30 MB |
| Cross-compilation | Pure `cargo build` | Requires C++ toolchain |
| Write model | Copy-on-Write B-tree | LSM tree (append-only) |
| Concurrency | Single-writer | Multi-writer |
| Background work | None (idle when app is idle) | Compaction threads |
| Best for | Edge/IoT, low-throughput | Cloud, high-throughput |

## Key Design Decisions

### 1. Async/Sync Bridge

Redb's API is synchronous, but Drasi's index traits are async. All operations are wrapped in `tokio::task::spawn_blocking`:

```rust
let task = task::spawn_blocking(move || {
    let txn = db.begin_write()?;
    // ... synchronous Redb operations
    txn.commit()
});
task.await?
```

### 2. Big-Endian Key Encoding

All numeric keys use Big-Endian encoding to preserve B-tree sort order. This is critical for timestamp-based range queries (archive index, future queue).

```rust
due_time.to_be_bytes()  // NOT to_le_bytes()
```

### 3. Range Queries Instead of Prefix Iterators

Redb lacks prefix iterators. We calculate explicit range bounds by incrementing the prefix:

```rust
let (start, end) = prefix_range(&prefix);
table.range(start..end)
```

### 4. Transaction-Aware Helpers for Partial Joins

Redb only allows one write transaction at a time. For partial join index operations that create virtual relations recursively, internal helpers accept `&WriteTransaction` to share a single transaction:

```rust
fn set_element_internal(context: &Context, txn: &WriteTransaction, ...) {
    // Can be called recursively within the same transaction
    update_source_joins(context, txn, ...)?;
}
```

### 5. Read-Modify-Write for Increments

Unlike RocksDB's merge operators, Redb requires explicit read-modify-write for atomic increments. This is safe due to the single-writer model.

## Usage

```rust
use drasi_index_redb::RedbIndexProvider;

// Standard configuration (256MB cache, archive enabled)
let provider = RedbIndexProvider::new("/data/drasi", true);

// IoT-optimized (64MB cache, no archive)
let provider = RedbIndexProvider::iot_preset("/data/drasi");

// Custom configuration
let provider = RedbIndexProvider::with_config(
    "/data/drasi",
    true,   // enable_archive
    128 * 1024 * 1024,  // 128MB cache
);
```

## Known Limitations

1. **Single-Writer Concurrency**: Only one write transaction at a time. Not suitable for high-throughput multi-writer scenarios.

2. **Higher Write Amplification**: Copy-on-Write B-tree design has higher write costs than RocksDB's LSM tree.

3. **32-bit mmap Exhaustion**: On 32-bit devices, opening many database files may exhaust virtual address space.

## Building

```bash
cargo build -p drasi-index-redb

# Run tests (requires libjq)
JQ_LIB_DIR=/opt/homebrew/lib cargo test -p drasi-index-redb
```

## Test Status

All 36 shared tests pass (100% compliance with other index backends).
