# Drasi-Lib: Comprehensive Overview

This document provides a comprehensive overview of the `drasi-lib` library and all work done on the `feature-lib` branch. It is intended to help new contributors quickly understand the architecture, components, and design decisions.

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [What is Drasi-Lib?](#what-is-drasi-lib)
3. [Branch History](#branch-history)
4. [Architecture Overview](#architecture-overview)
5. [Core Components](#core-components)
6. [Plugin System](#plugin-system)
7. [Data Flow](#data-flow)
8. [Developer Guides](#developer-guides)
9. [Key Design Decisions](#key-design-decisions)
10. [Getting Started](#getting-started)
11. [File Structure](#file-structure)

---

## Executive Summary

**Drasi-lib** transforms drasi-core from a query engine library into a **fully embeddable, production-ready runtime** that can be integrated into any Rust application. It provides:

- **Complete runtime orchestration** for sources, queries, and reactions
- **Plugin architecture** for extensible sources, reactions, bootstrap providers, and storage backends
- **Channel-based event routing** with backpressure support
- **Persistent state storage** for plugins that need to survive restarts
- **Fluent builder API** for ergonomic configuration

The `feature-lib` branch contains **88,455 lines of new code** across **337 files**, representing approximately 2 months of intensive development by the team.

---

## What is Drasi-Lib?

Drasi-lib is a Rust library that enables **real-time change detection** using continuous queries. Unlike traditional event-driven systems where you write custom code to detect changes, drasi-lib lets you declare *what changes matter* using Cypher queries.

### The Core Concept

```
Sources (Data In) --> Continuous Queries (Change Detection) --> Reactions (Actions Out)
```

- **Sources**: Ingest data from external systems (PostgreSQL, HTTP, gRPC, Redis Streams)
- **Queries**: Cypher queries that maintain live result sets and detect meaningful changes
- **Reactions**: Output handlers that respond to query results (HTTP webhooks, gRPC, SSE, logging)

### Key Benefits

| Feature | Description |
|---------|-------------|
| **Declarative** | Define change detection with Cypher queries, not custom code |
| **Precise** | Get before/after states for every change, not just raw events |
| **Pluggable** | Use existing plugins or build your own |
| **Scalable** | Built-in backpressure, priority queues, and dispatch modes |
| **Embeddable** | Runs in-process, no external services required |

---

## Branch History

The `feature-lib` branch was forked from main after commit `7a62e56cce4037bb6ad963aa949c958184b5e517`. Here is the chronological development:

### Phase 1: Initial Creation (Dec 10, 2025)
| Commit | Description |
|--------|-------------|
| `20bfd4a` | **Creation and integration of drasi-lib into drasi-core** - The foundational commit establishing the entire lib crate |

### Phase 2: Core Features (Dec 14-20, 2025)
| Commit | Description |
|--------|-------------|
| `1cbdaae` | Fix versions of toolchain when running workflows |
| `3f2fbb5` | Add feature-lib branch to CI workflow triggers |
| `a6df245` | Create drasi-issue-researcher definition |
| `ed1a6fc` | Fix non-events for DrasiLib |
| `7117b92` | Run code coverage workflow on larger disk runner |
| `186ec56` | Add release flag to cargo tarpaulin |
| `99dcd70` | Update GitHub Actions runner to ubuntu-latest |
| `4cf54b0` | **Add per-query template support** and direct console output to LogReaction |
| `a63d381` | Migrate clippy lint configuration to Cargo.toml |
| `e824b2d` | **Pass query context to sources and bootstrap providers** via SourceSubscriptionSettings |
| `254c6a2` | **Add per-query configuration with validation, default templates, multi-path SSE support** |

### Phase 3: Developer Guides & Plugins (Dec 21-24, 2025)
| Commit | Description |
|--------|-------------|
| `011eab7` | **Developer Guides** for Bootstrap Provider, Source, and Reaction plugins |
| `2ecf305` | **Make RocksDB and Garnet Indexes independent plugins** |
| `3e4462f` | Updated runner for generating code coverage |
| `dba42f1` | **Implement StateStore Plugins** |

### Phase 4: Plugin Refactoring & Database Reactions (Dec 29, 2025 - Jan 4, 2026)
| Commit | Description |
|--------|-------------|
| `299d15b` | **Add PostgreSQL storedproc reaction** |
| `dcc9d2b` | **Refactor plugin traits into respective plugin folders** |
| `7202d24` | Cleaning up runtime integration between drasi-lib and plugins |
| `889f128` | Updated cargo toml files for cargo publish |
| `5db2c10` | **Add MySQL storedproc reaction** |
| `5960a0e` | Added rust-toolchain.toml |
| `d3a79f6` | **Add MSSQL storedproc reaction** |

---

## Architecture Overview

### High-Level Architecture

```
                          ┌─────────────────────────────────────────┐
                          │              DrasiLib                    │
                          │                                          │
                          │  ┌──────────┐  ┌────────┐  ┌──────────┐ │
                          │  │ Sources  │  │Queries │  │Reactions │ │
                          │  └────┬─────┘  └───┬────┘  └────┬─────┘ │
                          │       │            │            │        │
                          │       ▼            ▼            ▼        │
                          │  ┌────────────────────────────────────┐ │
                          │  │      Event Routing Layer           │ │
                          │  │  (Channels, Priority Queues,       │ │
                          │  │   Dispatchers)                     │ │
                          │  └────────────────────────────────────┘ │
                          │                    │                     │
                          │                    ▼                     │
                          │  ┌────────────────────────────────────┐ │
                          │  │    drasi-core Query Engine          │ │
                          │  │  (QueryBuilder, ContinuousQuery)    │ │
                          │  └────────────────────────────────────┘ │
                          └─────────────────────────────────────────┘
                                              │
                    ┌─────────────────────────┼─────────────────────────┐
                    │                         │                         │
           ┌────────▼────────┐      ┌────────▼────────┐      ┌─────────▼────────┐
           │ Index Plugins   │      │ State Stores    │      │ Bootstrap        │
           │ (RocksDB,       │      │ (Memory, Redb)  │      │ Providers        │
           │  Garnet)        │      │                 │      │                  │
           └─────────────────┘      └─────────────────┘      └──────────────────┘
```

### Crate Dependencies

```
drasi-lib
├── drasi-core (query engine)
├── drasi-query-cypher (Cypher parser)
├── drasi-query-gql (GQL parser)
├── drasi-functions-cypher (Cypher functions)
├── drasi-functions-gql (GQL functions)
├── drasi-query-ast (AST types)
└── drasi-middleware (data transformations)

components/
├── sources/
│   ├── drasi-source-application
│   ├── drasi-source-grpc
│   ├── drasi-source-http
│   ├── drasi-source-mock
│   ├── drasi-source-platform
│   └── drasi-source-postgres
├── reactions/
│   ├── drasi-reaction-application
│   ├── drasi-reaction-grpc
│   ├── drasi-reaction-grpc-adaptive
│   ├── drasi-reaction-http
│   ├── drasi-reaction-http-adaptive
│   ├── drasi-reaction-log
│   ├── drasi-reaction-platform
│   ├── drasi-reaction-profiler
│   ├── drasi-reaction-sse
│   ├── drasi-reaction-storedproc-postgres
│   ├── drasi-reaction-storedproc-mysql
│   └── drasi-reaction-storedproc-mssql
├── bootstrappers/
│   ├── drasi-bootstrap-application
│   ├── drasi-bootstrap-noop
│   ├── drasi-bootstrap-platform
│   ├── drasi-bootstrap-postgres
│   └── drasi-bootstrap-scriptfile
├── indexes/
│   ├── drasi-index-rocksdb
│   └── drasi-index-garnet
└── state_stores/
    └── drasi-state-store-redb
```

---

## Core Components

### 1. DrasiLib (lib/src/lib_core.rs)

The main entry point and orchestrator. Manages the lifecycle of all components.

```rust
use drasi_lib::{DrasiLib, Query};

let core = DrasiLib::builder()
    .with_id("my-app")
    .with_source(my_source)
    .with_reaction(my_reaction)
    .with_query(
        Query::cypher("high-temp")
            .query("MATCH (s:Sensor) WHERE s.temperature > 75 RETURN s")
            .from_source("sensors")
            .build()
    )
    .build()
    .await?;

core.start().await?;
```

### 2. Sources (lib/src/sources/)

Data ingestion components that emit graph elements (nodes and relationships).

**Key Traits:**
- `Source`: Main trait for all sources
- `SourceBase`: Common functionality (dispatchers, subscriptions, lifecycle)

**Available Plugins:**
| Plugin | Description |
|--------|-------------|
| `application` | Programmatic/in-memory for embedded use |
| `grpc` | gRPC streaming sources |
| `http` | HTTP polling with adaptive batching |
| `mock` | Test data generator |
| `platform` | Redis Streams consumer |
| `postgres` | PostgreSQL WAL replication |

### 3. Queries (lib/src/queries/)

Cypher/GQL queries that maintain live result sets.

**Key Components:**
- `QueryManager`: Orchestrates query lifecycle
- `QueryBase`: Common query functionality
- `LabelExtractor`: Extracts required labels from queries
- `SubscriptionBuilder`: Manages source subscriptions

**Query Results:**
```rust
// Results include change type
pub struct QueryResult {
    pub query_id: String,
    pub timestamp: DateTime<Utc>,
    pub results: Vec<serde_json::Value>,  // Adding, Updating, Removing
}
```

### 4. Reactions (lib/src/reactions/)

Output handlers that receive query results.

**Key Traits:**
- `Reaction`: Main trait for all reactions
- `ReactionBase`: Common functionality (priority queue, subscriptions)
- `QueryProvider`: Decoupled query access

**Available Plugins:**
| Plugin | Description |
|--------|-------------|
| `application` | Programmatic for embedded use |
| `grpc` | gRPC streaming delivery |
| `grpc-adaptive` | gRPC with adaptive batching |
| `http` | HTTP POST to webhooks |
| `http-adaptive` | HTTP with adaptive batching |
| `log` | Console logging with templates |
| `platform` | Redis Streams publisher |
| `profiler` | Performance profiling |
| `sse` | Server-Sent Events |
| `storedproc-postgres` | PostgreSQL stored procedures |
| `storedproc-mysql` | MySQL stored procedures |
| `storedproc-mssql` | MSSQL stored procedures |

### 5. Bootstrap Providers (lib/src/bootstrap/)

Deliver initial data when queries subscribe to sources.

**Key Principle:** Bootstrap providers are **independent from sources**. Any source can use any bootstrap provider.

**Available Providers:**
| Provider | Description |
|----------|-------------|
| `noop` | Returns no data (default) |
| `scriptfile` | Reads JSONL files |
| `postgres` | PostgreSQL COPY snapshot |
| `application` | Replays in-memory events |
| `platform` | HTTP streaming from remote Drasi |

### 6. Index Plugins (components/indexes/)

Persistent storage backends for query state.

| Plugin | Description |
|--------|-------------|
| `rocksdb` | Embedded RocksDB storage |
| `garnet` | Redis/Garnet compatible storage |

### 7. State Store Providers (lib/src/state_store/)

Persistent key-value storage for plugin state.

| Provider | Description |
|----------|-------------|
| `memory` | In-memory (built-in, non-persistent) |
| `redb` | ACID-compliant embedded database |

---

## Plugin System

### Design Philosophy

**DrasiLib has ZERO awareness of which plugins exist.** All plugins implement traits and are passed to DrasiLib at runtime.

### Creating Plugins

Each plugin crate:
1. Defines its own typed configuration
2. Creates a `SourceBase`/`ReactionBase` instance
3. Implements the required trait
4. Is passed to DrasiLib via the builder

### Plugin Lifecycle

```
Stopped → Starting → Running → Stopping → Stopped
              ↓
            Error
```

### Runtime Context

Plugins receive runtime context via `initialize()`:

**SourceRuntimeContext:**
- `source_id`: Unique identifier
- `status_tx`: Status event channel
- `state_store`: Optional persistent storage

**ReactionRuntimeContext:**
- `reaction_id`: Unique identifier
- `status_tx`: Status event channel
- `state_store`: Optional persistent storage
- `query_provider`: Access to queries for subscription

---

## Data Flow

### Event Types

```rust
// Source events
pub enum SourceChange {
    Insert { element: Element },
    Update { element: Element },
    Delete { metadata: ElementMetadata },
}

// Query results
pub struct QueryResult {
    pub query_id: String,
    pub results: Vec<serde_json::Value>,  // type: "add"|"update"|"remove"
}
```

### Dispatch Modes

| Mode | Backpressure | Message Loss | Best For |
|------|--------------|--------------|----------|
| **Channel** (default) | Yes | None | Different subscriber speeds |
| **Broadcast** | No | Possible | High fanout (10+ subscribers) |

### Priority Queues

Results are processed in timestamp order with backpressure support:

```rust
pub struct PriorityQueueMetrics {
    pub total_enqueued: u64,
    pub total_dequeued: u64,
    pub current_depth: usize,
    pub drops_due_to_capacity: u64,
    pub blocked_enqueue_count: u64,
}
```

---

## Developer Guides

Comprehensive guides are available in `components/`:

| Guide | Location | Description |
|-------|----------|-------------|
| Source Developer Guide | `components/sources/README.md` | Creating custom sources |
| Reaction Developer Guide | `components/reactions/README.md` | Creating custom reactions |
| Bootstrap Provider Guide | `components/bootstrappers/README.md` | Creating bootstrap providers |
| State Store Guide | `components/state_stores/README.md` | Creating state store providers |

---

## Key Design Decisions

### 1. Plugin Architecture

**Decision:** Plugins are external crates that implement traits, not built-in types.

**Rationale:**
- Maximum flexibility for users
- No need to modify drasi-lib for new plugins
- Clear separation of concerns
- Easier testing and maintenance

### 2. Ownership Transfer

**Decision:** `with_source()` and `with_reaction()` take ownership of plugin instances.

**Rationale:**
- Clear ownership semantics
- Plugins are managed by DrasiLib
- Prevents use-after-move bugs

### 3. Context-Based Dependency Injection

**Decision:** Plugins receive runtime services via an `initialize()` method.

**Rationale:**
- Clean separation between construction and initialization
- Plugins don't need to know about DrasiLib internals
- Easy to test with mock contexts

### 4. Independent Bootstrap Providers

**Decision:** Bootstrap providers are separate from sources.

**Rationale:**
- Enables mix-and-match scenarios (e.g., HTTP source + PostgreSQL bootstrap)
- Cleaner separation of concerns
- Easier to test each component independently

### 5. Dual Dispatch Modes

**Decision:** Support both Channel (backpressure) and Broadcast modes.

**Rationale:**
- Different use cases have different requirements
- Channel mode for critical data where loss is unacceptable
- Broadcast mode for high-fanout scenarios where some loss is acceptable

---

## Getting Started

### Basic Usage

```rust
use drasi_lib::{DrasiLib, Query};
use drasi_source_mock::{MockSource, MockSourceConfig};
use drasi_reaction_log::{LogReaction, LogReactionConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create source
    let source = MockSource::new("sensors", MockSourceConfig {
        data_type: "sensor".to_string(),
        interval_ms: 1000,
    })?;

    // Create reaction
    let reaction = LogReaction::new(
        "alerts",
        vec!["high-temp".to_string()],
        LogReactionConfig::default(),
    );

    // Build and start
    let core = DrasiLib::builder()
        .with_id("my-app")
        .with_source(source)
        .with_reaction(reaction)
        .with_query(
            Query::cypher("high-temp")
                .query("MATCH (s:Sensor) WHERE s.temperature > 75 RETURN s")
                .from_source("sensors")
                .build()
        )
        .build()
        .await?;

    core.start().await?;

    // Run until shutdown
    tokio::signal::ctrl_c().await?;
    core.stop().await?;

    Ok(())
}
```

### With Persistent Storage

```rust
use drasi_index_rocksdb::RocksDbIndexProvider;
use drasi_state_store_redb::RedbStateStoreProvider;

let index_provider = RocksDbIndexProvider::new("/data/indexes", true, false);
let state_store = RedbStateStoreProvider::new("/data/state.redb")?;

let core = DrasiLib::builder()
    .with_id("my-app")
    .with_index_provider(Arc::new(index_provider))
    .with_state_store_provider(Arc::new(state_store))
    // ... sources, reactions, queries ...
    .build()
    .await?;
```

### With Mix-and-Match Bootstrap

```rust
use drasi_source_http::HttpSource;
use drasi_bootstrap_postgres::PostgresBootstrapProvider;

// HTTP source with PostgreSQL bootstrap
let source = HttpSource::builder("my-source")
    .with_host("localhost")
    .with_port(9000)
    .with_bootstrap_provider(
        PostgresBootstrapProvider::builder()
            .with_host("db.example.com")
            .with_database("mydb")
            .build()?
    )
    .build()?;
```

---

## File Structure

```
lib/
├── Cargo.toml                    # Crate manifest
├── CLAUDE.md                     # AI assistant context
├── README.md                     # User documentation
├── src/
│   ├── lib.rs                    # Public API exports
│   ├── lib_core.rs               # DrasiLib implementation
│   ├── builder.rs                # Fluent builders
│   ├── error.rs                  # Error types
│   ├── bootstrap/                # Bootstrap provider system
│   │   └── mod.rs
│   ├── channels/                 # Event routing
│   │   ├── mod.rs
│   │   ├── events.rs             # Event types
│   │   ├── dispatcher.rs         # Dispatch modes
│   │   └── priority_queue.rs     # Priority queue
│   ├── config/                   # Configuration types
│   │   ├── mod.rs
│   │   ├── schema.rs             # Config schemas
│   │   └── runtime.rs            # Runtime config
│   ├── context/                  # Runtime contexts
│   │   └── mod.rs
│   ├── indexes/                  # Index backend plugins
│   │   ├── mod.rs
│   │   ├── config.rs
│   │   └── factory.rs
│   ├── queries/                  # Query management
│   │   ├── mod.rs
│   │   ├── base.rs
│   │   ├── manager.rs
│   │   ├── label_extractor.rs
│   │   └── subscription_builder.rs
│   ├── reactions/                # Reaction system
│   │   ├── mod.rs
│   │   ├── traits.rs
│   │   ├── manager.rs
│   │   └── common/
│   │       ├── base.rs
│   │       ├── config.rs
│   │       └── templates.rs
│   ├── sources/                  # Source system
│   │   ├── mod.rs
│   │   ├── traits.rs
│   │   ├── base.rs
│   │   ├── manager.rs
│   │   └── future_queue_source.rs
│   ├── state_store/              # State store system
│   │   └── mod.rs
│   ├── state_guard.rs            # Initialization guard
│   ├── component_ops.rs          # Component helpers
│   ├── inspection.rs             # Inspection APIs
│   ├── lifecycle.rs              # Lifecycle management
│   └── profiling/                # Performance profiling
│       └── mod.rs

components/
├── sources/                      # Source plugins
│   ├── README.md                 # Developer guide
│   ├── application/
│   ├── grpc/
│   ├── http/
│   ├── mock/
│   ├── platform/
│   └── postgres/
├── reactions/                    # Reaction plugins
│   ├── README.md                 # Developer guide
│   ├── application/
│   ├── grpc/
│   ├── grpc-adaptive/
│   ├── http/
│   ├── http-adaptive/
│   ├── log/
│   ├── platform/
│   ├── profiler/
│   ├── sse/
│   ├── storedproc-postgres/
│   ├── storedproc-mysql/
│   └── storedproc-mssql/
├── bootstrappers/                # Bootstrap providers
│   ├── README.md                 # Developer guide
│   ├── application/
│   ├── noop/
│   ├── platform/
│   ├── postgres/
│   └── scriptfile/
├── indexes/                      # Index plugins
│   ├── rocksdb/
│   └── garnet/
└── state_stores/                 # State store plugins
    ├── README.md                 # Developer guide
    └── redb/
```

---

## Summary

The `feature-lib` branch represents a major evolution of drasi-core:

| Aspect | Before (main) | After (feature-lib) |
|--------|---------------|---------------------|
| **Scope** | Query engine library | Complete embeddable runtime |
| **Sources** | None | 6 plugins (postgres, http, grpc, mock, platform, application) |
| **Reactions** | None | 12 plugins including database stored procedures |
| **Bootstrap** | None | 5 providers with mix-and-match support |
| **Storage** | Direct dependency | Pluggable backends (RocksDB, Garnet) |
| **State** | None | Pluggable state stores (Memory, Redb) |
| **API** | Low-level QueryBuilder | High-level fluent builders |
| **Documentation** | Minimal | Comprehensive developer guides |

This work establishes drasi-lib as a production-ready library for building real-time, change-driven applications in Rust.

---

*Document generated: January 11, 2026*
*Branch: feature-lib*
*Commits analyzed: 22 (20bfd4a..d3a79f6)*
