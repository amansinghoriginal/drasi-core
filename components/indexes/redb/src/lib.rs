// Copyright 2025 The Drasi Authors.
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

//! # Drasi Redb Index Backend
//!
//! A pure-Rust, embedded storage backend for Drasi using [Redb](https://github.com/cberner/redb).
//!
//! ## Use Case
//!
//! This backend is optimized for **Edge/IoT deployments** where:
//! - Cross-compilation simplicity is required (no C++ toolchain needed)
//! - Binary size must be minimal (~90% smaller than RocksDB)
//! - Power consumption matters (no background compaction threads)
//! - Operational simplicity is valued over maximum throughput
//!
//! ## Trade-offs
//!
//! Compared to RocksDB:
//! - **Lower write throughput**: Redb uses Copy-on-Write B-Trees vs LSM trees
//! - **Single-writer model**: Only one write transaction at a time
//! - **Better for low-to-medium throughput**: Ideal for edge sensors, not Kafka streams
//!
//! ## Example
//!
//! ```ignore
//! use drasi_index_redb::RedbIndexProvider;
//! use drasi_lib::DrasiLib;
//! use std::sync::Arc;
//!
//! // Standard configuration
//! let provider = RedbIndexProvider::new("/data/drasi", true);
//!
//! // IoT-optimized configuration (smaller cache, no archive)
//! let provider = RedbIndexProvider::iot_preset("/data/drasi");
//!
//! let drasi = DrasiLib::builder()
//!     .with_index_provider(Arc::new(provider))
//!     .build()
//!     .await?;
//! ```

pub mod element_index;
pub mod future_queue;
pub mod keys;
pub mod plugin;
pub mod result_index;
pub mod storage_models;

// Re-export main types
pub use element_index::{RedbElementIndex, RedbIndexOptions};
pub use future_queue::RedbFutureQueue;
pub use plugin::RedbIndexProvider;
pub use result_index::RedbResultIndex;
