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

//! Redb Index Backend Plugin
//!
//! This module provides the `RedbIndexProvider` which implements the
//! `IndexBackendPlugin` trait for Redb-based persistent storage.
//!
//! # Use Case
//!
//! Redb is optimized for **Edge/IoT deployments** where:
//! - Cross-compilation simplicity is required (pure Rust, no C++ toolchain)
//! - Binary size must be minimal (~90% smaller than RocksDB)
//! - Power consumption matters (no background compaction threads)
//!
//! # Example
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

use async_trait::async_trait;
use drasi_core::interface::{
    ElementArchiveIndex, ElementIndex, FutureQueue, IndexBackendPlugin, IndexError, ResultIndex,
};
use std::path::PathBuf;
use std::sync::Arc;

use crate::element_index::{RedbElementIndex, RedbIndexOptions};
use crate::future_queue::RedbFutureQueue;
use crate::result_index::RedbResultIndex;

/// Default cache size: 256 MB (IoT-safe, compared to Redb's 1GB default)
const DEFAULT_CACHE_SIZE: u64 = 256 * 1024 * 1024;

/// IoT preset cache size: 64 MB
const IOT_CACHE_SIZE: u64 = 64 * 1024 * 1024;

/// Redb index backend provider.
///
/// This provider creates Redb-backed indexes for persistent storage.
/// Data survives restarts, so queries using this backend do not require
/// re-bootstrapping.
///
/// # Configuration
///
/// - `path`: Base directory for Redb database files
/// - `enable_archive`: Enable archive index for `past()` function support
/// - `cache_size_bytes`: Maximum cache size (default: 256MB, IoT-safe)
///
/// # Trade-offs vs RocksDB
///
/// | Aspect | RocksDB | Redb |
/// |--------|---------|------|
/// | Binary size | 15-30 MB | 1-2 MB |
/// | Cross-compilation | Requires C++ toolchain | Pure `cargo build` |
/// | Write throughput | High (LSM tree) | Lower (CoW B-tree) |
/// | Concurrency | Multi-writer | Single-writer |
/// | Best for | Cloud/High-throughput | Edge/IoT |
///
/// # Directory Structure
///
/// Redb creates the following directory structure:
/// ```text
/// {path}/
///   {query_id}/
///     elements.redb    - Element index data
///     results.redb     - Result index data
///     futures.redb     - Future queue data
/// ```
pub struct RedbIndexProvider {
    path: PathBuf,
    enable_archive: bool,
    cache_size_bytes: u64,
}

impl RedbIndexProvider {
    /// Create a new Redb index provider with default settings.
    ///
    /// # Arguments
    ///
    /// * `path` - Base directory for Redb data files
    /// * `enable_archive` - Enable archive index for point-in-time queries
    ///
    /// # Example
    ///
    /// ```ignore
    /// let provider = RedbIndexProvider::new("/data/drasi", true);
    /// ```
    pub fn new<P: Into<PathBuf>>(path: P, enable_archive: bool) -> Self {
        Self {
            path: path.into(),
            enable_archive,
            cache_size_bytes: DEFAULT_CACHE_SIZE,
        }
    }

    /// Create a new Redb index provider with full configuration.
    ///
    /// # Arguments
    ///
    /// * `path` - Base directory for Redb data files
    /// * `enable_archive` - Enable archive index for point-in-time queries
    /// * `cache_size_bytes` - Maximum cache size in bytes
    ///
    /// # Example
    ///
    /// ```ignore
    /// // 128 MB cache, archive enabled
    /// let provider = RedbIndexProvider::with_config(
    ///     "/data/drasi",
    ///     true,
    ///     128 * 1024 * 1024,
    /// );
    /// ```
    pub fn with_config<P: Into<PathBuf>>(
        path: P,
        enable_archive: bool,
        cache_size_bytes: u64,
    ) -> Self {
        Self {
            path: path.into(),
            enable_archive,
            cache_size_bytes,
        }
    }

    /// Create a Redb index provider optimized for IoT/Edge deployments.
    ///
    /// This preset uses:
    /// - 64 MB cache size (instead of default 256 MB)
    /// - Archive disabled (to reduce storage overhead)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let provider = RedbIndexProvider::iot_preset("/data/drasi");
    /// ```
    pub fn iot_preset<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            enable_archive: false,
            cache_size_bytes: IOT_CACHE_SIZE,
        }
    }

    /// Get the configured path.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Check if archive is enabled.
    pub fn is_archive_enabled(&self) -> bool {
        self.enable_archive
    }

    /// Get the configured cache size in bytes.
    pub fn cache_size_bytes(&self) -> u64 {
        self.cache_size_bytes
    }
}

#[async_trait]
impl IndexBackendPlugin for RedbIndexProvider {
    async fn create_element_index(
        &self,
        query_id: &str,
    ) -> Result<Arc<dyn ElementIndex>, IndexError> {
        let path = self.path.to_string_lossy().to_string();
        let options = RedbIndexOptions {
            archive_enabled: self.enable_archive,
            cache_size_bytes: self.cache_size_bytes,
        };

        let index = RedbElementIndex::new(query_id, &path, options).map_err(|e| {
            log::error!(
                "Failed to create Redb element index for query '{query_id}' at path '{path}': {e}"
            );
            e
        })?;

        Ok(Arc::new(index))
    }

    async fn create_archive_index(
        &self,
        query_id: &str,
    ) -> Result<Arc<dyn ElementArchiveIndex>, IndexError> {
        // Redb shares the element index and archive index in the same instance
        // The archive functionality is enabled/disabled via RedbIndexOptions
        let path = self.path.to_string_lossy().to_string();
        let options = RedbIndexOptions {
            archive_enabled: self.enable_archive,
            cache_size_bytes: self.cache_size_bytes,
        };

        let index = RedbElementIndex::new(query_id, &path, options).map_err(|e| {
            log::error!(
                "Failed to create Redb archive index for query '{query_id}' at path '{path}': {e}"
            );
            e
        })?;

        Ok(Arc::new(index))
    }

    async fn create_result_index(
        &self,
        query_id: &str,
    ) -> Result<Arc<dyn ResultIndex>, IndexError> {
        let path = self.path.to_string_lossy().to_string();

        let index =
            RedbResultIndex::new(query_id, &path, self.cache_size_bytes).map_err(|e| {
                log::error!(
                    "Failed to create Redb result index for query '{query_id}' at path '{path}': {e}"
                );
                e
            })?;

        Ok(Arc::new(index))
    }

    async fn create_future_queue(
        &self,
        query_id: &str,
    ) -> Result<Arc<dyn FutureQueue>, IndexError> {
        let path = self.path.to_string_lossy().to_string();

        let queue = RedbFutureQueue::new(query_id, &path, self.cache_size_bytes).map_err(|e| {
            log::error!(
                "Failed to create Redb future queue for query '{query_id}' at path '{path}': {e}"
            );
            e
        })?;

        Ok(Arc::new(queue))
    }

    fn is_volatile(&self) -> bool {
        false // Redb is persistent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_redb_index_provider_new() {
        let provider = RedbIndexProvider::new("/data/drasi", true);
        assert_eq!(provider.path(), &PathBuf::from("/data/drasi"));
        assert!(provider.is_archive_enabled());
        assert_eq!(provider.cache_size_bytes(), DEFAULT_CACHE_SIZE);
    }

    #[test]
    fn test_redb_index_provider_with_config() {
        let provider = RedbIndexProvider::with_config("/data/drasi", false, 128 * 1024 * 1024);
        assert_eq!(provider.path(), &PathBuf::from("/data/drasi"));
        assert!(!provider.is_archive_enabled());
        assert_eq!(provider.cache_size_bytes(), 128 * 1024 * 1024);
    }

    #[test]
    fn test_redb_index_provider_iot_preset() {
        let provider = RedbIndexProvider::iot_preset("/data/drasi");
        assert_eq!(provider.path(), &PathBuf::from("/data/drasi"));
        assert!(!provider.is_archive_enabled());
        assert_eq!(provider.cache_size_bytes(), IOT_CACHE_SIZE);
    }

    #[test]
    fn test_redb_index_provider_is_volatile() {
        let provider = RedbIndexProvider::new("/tmp/test", false);
        assert!(!provider.is_volatile());
    }

    #[tokio::test]
    async fn test_redb_create_element_index() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let provider = RedbIndexProvider::new(temp_dir.path(), false);

        let result = provider.create_element_index("test_query").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_redb_create_archive_index() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let provider = RedbIndexProvider::new(temp_dir.path(), true);

        let result = provider.create_archive_index("test_query").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_redb_create_result_index() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let provider = RedbIndexProvider::new(temp_dir.path(), false);

        let result = provider.create_result_index("test_query").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_redb_create_future_queue() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let provider = RedbIndexProvider::new(temp_dir.path(), false);

        let result = provider.create_future_queue("test_query").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_redb_create_all_indexes() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let provider = RedbIndexProvider::new(temp_dir.path(), true);

        let element_result = provider.create_element_index("query_element").await;
        let archive_result = provider.create_archive_index("query_archive").await;
        let result_result = provider.create_result_index("query_result").await;
        let queue_result = provider.create_future_queue("query_queue").await;

        assert!(element_result.is_ok());
        assert!(archive_result.is_ok());
        assert!(result_result.is_ok());
        assert!(queue_result.is_ok());
    }
}
