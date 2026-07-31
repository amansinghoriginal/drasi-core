// Copyright 2024 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unexpected_cfgs)]

//! RocksDB Index Backend for Drasi
//!
//! This crate provides a persistent storage backend for Drasi queries using RocksDB.
//!
//! # Usage
//!
//! ```ignore
//! use drasi_index_rocksdb::RocksDbIndexProvider;
//! use drasi_lib::DrasiLib;
//! use std::sync::Arc;
//!
//! let provider = RocksDbIndexProvider::new("/data/drasi", true, false);
//! let drasi = DrasiLib::builder()
//!     .with_index_provider("rocksdb", Arc::new(provider))
//!     .build()?;
//! ```

/// The transactional RocksDB database type used for all query indexes.
///
/// Pessimistic `TransactionDB` rather than `OptimisticTransactionDB`: index
/// sessions are single-writer and short, so commit-time OCC validation buys
/// nothing, while every OCC DB pays a fixed lock-bucket allocation at open
/// (`MakeSharedOccLockBuckets` — jemalloc-profiled at 1.57 GB live across a
/// 28-query workload) plus retained write-buffer history for validation.
/// Pessimistic row locks are sized by locks actually held (near zero for a
/// single writer) and need no history at all.
pub type IndexDb = rocksdb::TransactionDB;

mod budget_monitor;
pub mod checkpoint;
#[cfg(feature = "plugin-descriptor")]
mod descriptor;
pub mod element_index;
pub mod future_queue;
pub mod live_results;
pub mod outbox;
mod plugin;
pub mod result_index;
mod session_state;
mod storage_models;
pub mod tuning;

// Re-export the plugin provider and unified DB opener for easy access
pub use checkpoint::RocksDbCheckpointStore;
pub use live_results::RocksDbLiveResultsWriter;
pub use outbox::RocksDbOutboxWriter;
pub use plugin::open_unified_db;
pub use plugin::RocksDbIndexProvider;
pub use tuning::RocksDbTuning;

#[cfg(feature = "plugin-descriptor")]
pub use descriptor::{RocksDbIndexConfigDto, RocksDbIndexDescriptor};

// Re-export session types
pub use session_state::RocksDbSessionControl;
pub use session_state::RocksDbSessionState;
