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

//! Redb Future Queue Implementation
//!
//! This module provides the `RedbFutureQueue` which implements the
//! `FutureQueue` trait for managing time-ordered scheduled operations.
//!
//! # Table Layout
//!
//! - `future_queue`: due_time(u64 BE) + hash64 -> StoredFutureElementRef
//! - `future_index`: position(u32 BE) + group_sig(u64 BE) + hash64 -> due_time(u64 BE)

use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use async_trait::async_trait;
use drasi_core::{
    interface::{FutureElementRef, FutureQueue, IndexError, PushType},
    models::{ElementReference, ElementTimestamp},
};
use hashers::jenkins::spooky_hash::SpookyHasher;
use prost::Message;
use redb::{Database, ReadableTable};
use tokio::task;

use crate::keys::{
    encode_findex_key, encode_findex_prefix, encode_fqueue_key, findex_prefix_range,
    hash_future_ref, FUTURE_INDEX, FUTURE_QUEUE,
};
use crate::storage_models::{StoredElementReference, StoredFutureElementRef};

/// Redb future queue
pub struct RedbFutureQueue {
    db: Arc<Database>,
}

impl RedbFutureQueue {
    /// Create a new Redb future queue.
    ///
    /// # Arguments
    ///
    /// * `query_id` - Unique identifier for the query
    /// * `path` - Base directory for database files
    /// * `cache_size_bytes` - Maximum cache size (currently unused, reserved for future)
    pub fn new(query_id: &str, path: &str, _cache_size_bytes: u64) -> Result<Self, IndexError> {
        let db_path = std::path::PathBuf::from(path)
            .join(query_id)
            .join("futures.redb");

        // Create parent directories if they don't exist
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(IndexError::other)?;
        }

        let db = Database::create(&db_path).map_err(IndexError::other)?;

        // Initialize tables
        {
            let write_txn = db.begin_write().map_err(IndexError::other)?;
            {
                let _ = write_txn
                    .open_table(FUTURE_QUEUE)
                    .map_err(IndexError::other)?;
                let _ = write_txn
                    .open_table(FUTURE_INDEX)
                    .map_err(IndexError::other)?;
            }
            write_txn.commit().map_err(IndexError::other)?;
        }

        Ok(Self { db: Arc::new(db) })
    }
}

#[async_trait]
impl FutureQueue for RedbFutureQueue {
    async fn push(
        &self,
        push_type: PushType,
        position_in_query: usize,
        group_signature: u64,
        element_ref: &ElementReference,
        original_time: ElementTimestamp,
        due_time: ElementTimestamp,
    ) -> Result<bool, IndexError> {
        let db = self.db.clone();
        let stored_element_ref: StoredElementReference = element_ref.into();
        let position_in_query = position_in_query as u32;

        let task = task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(IndexError::other)?;

            let should_push = {
                let mut queue_table = write_txn
                    .open_table(FUTURE_QUEUE)
                    .map_err(IndexError::other)?;
                let mut index_table = write_txn
                    .open_table(FUTURE_INDEX)
                    .map_err(IndexError::other)?;

                let future_ref = StoredFutureElementRef {
                    element_ref: stored_element_ref,
                    original_time,
                    due_time,
                    position_in_query,
                    group_signature,
                };

                let hash = {
                    let mut h = SpookyHasher::default();
                    future_ref.hash(&mut h);
                    h.finish()
                };

                let prefix = encode_findex_prefix(position_in_query, group_signature);
                let (range_start, range_end) = findex_prefix_range(&prefix);

                let should_push = match push_type {
                    PushType::Always => true,
                    PushType::IfNotExists => {
                        // Check if any entry with this prefix exists
                        let iter = index_table
                            .range(range_start..range_end)
                            .map_err(IndexError::other)?;
                        let mut has_entry = false;
                        for item in iter {
                            if item.is_ok() {
                                has_entry = true;
                                break;
                            }
                        }
                        !has_entry
                    }
                    PushType::Overwrite => {
                        // Remove existing entries with this prefix
                        remove_by_prefix(
                            &mut queue_table,
                            &mut index_table,
                            position_in_query,
                            group_signature,
                        )?;
                        true
                    }
                };

                if should_push {
                    let index_key = encode_findex_key(position_in_query, group_signature, hash);
                    let queue_key = encode_fqueue_key(due_time, hash);

                    index_table
                        .insert(&index_key, &due_time.to_be_bytes())
                        .map_err(IndexError::other)?;

                    queue_table
                        .insert(&queue_key, future_ref.encode_to_vec().as_slice())
                        .map_err(IndexError::other)?;
                }

                should_push
            };

            write_txn.commit().map_err(IndexError::other)?;
            Ok(should_push)
        });

        match task.await {
            Ok(v) => v,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    async fn remove(
        &self,
        position_in_query: usize,
        group_signature: u64,
    ) -> Result<(), IndexError> {
        let db = self.db.clone();
        let position_in_query = position_in_query as u32;

        let task = task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(IndexError::other)?;

            {
                let mut queue_table = write_txn
                    .open_table(FUTURE_QUEUE)
                    .map_err(IndexError::other)?;
                let mut index_table = write_txn
                    .open_table(FUTURE_INDEX)
                    .map_err(IndexError::other)?;

                remove_by_prefix(
                    &mut queue_table,
                    &mut index_table,
                    position_in_query,
                    group_signature,
                )?;
            }

            write_txn.commit().map_err(IndexError::other)?;
            Ok(())
        });

        match task.await {
            Ok(v) => v,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    async fn pop(&self) -> Result<Option<FutureElementRef>, IndexError> {
        let db = self.db.clone();

        let task = task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(IndexError::other)?;

            let result = {
                let mut queue_table = write_txn
                    .open_table(FUTURE_QUEUE)
                    .map_err(IndexError::other)?;
                let mut index_table = write_txn
                    .open_table(FUTURE_INDEX)
                    .map_err(IndexError::other)?;

                // Get the first (lowest due_time) entry
                let first = {
                    let mut iter = queue_table.iter().map_err(IndexError::other)?;
                    match iter.next() {
                        Some(Ok((key, value))) => Some((key.value(), value.value().to_vec())),
                        Some(Err(e)) => return Err(IndexError::other(e)),
                        None => None,
                    }
                };

                match first {
                    Some((queue_key, value_bytes)) => {
                        // Decode the future ref
                        let stored_ref = StoredFutureElementRef::decode(value_bytes.as_slice())
                            .map_err(IndexError::other)?;

                        // Build index key to remove
                        let hash = {
                            let mut h = SpookyHasher::default();
                            stored_ref.hash(&mut h);
                            h.finish()
                        };

                        let index_key = encode_findex_key(
                            stored_ref.position_in_query,
                            stored_ref.group_signature,
                            hash,
                        );

                        // Remove from both tables
                        let _ = queue_table.remove(&queue_key);
                        let _ = index_table.remove(&index_key);

                        Ok(Some(stored_ref.into()))
                    }
                    None => Ok(None),
                }
            };

            write_txn.commit().map_err(IndexError::other)?;
            result
        });

        match task.await {
            Ok(v) => v,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    async fn peek_due_time(&self) -> Result<Option<ElementTimestamp>, IndexError> {
        let db = self.db.clone();

        let task = task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(IndexError::other)?;
            let queue_table = read_txn
                .open_table(FUTURE_QUEUE)
                .map_err(IndexError::other)?;

            let mut iter = queue_table.iter().map_err(IndexError::other)?;
            let result = match iter.next() {
                Some(Ok((key, _))) => {
                    let key_bytes: [u8; 16] = key.value();
                    let mut due_time_bytes = [0u8; 8];
                    due_time_bytes.copy_from_slice(&key_bytes[0..8]);
                    Ok(Some(u64::from_be_bytes(due_time_bytes)))
                }
                Some(Err(e)) => Err(IndexError::other(e)),
                None => Ok(None),
            };
            drop(iter);
            result
        });

        match task.await {
            Ok(v) => v,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    async fn clear(&self) -> Result<(), IndexError> {
        let db = self.db.clone();

        let task = task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(IndexError::other)?;

            {
                let mut queue_table = write_txn
                    .open_table(FUTURE_QUEUE)
                    .map_err(IndexError::other)?;
                let mut index_table = write_txn
                    .open_table(FUTURE_INDEX)
                    .map_err(IndexError::other)?;

                // Clear queue table
                let queue_keys: Vec<[u8; 16]> = queue_table
                    .iter()
                    .map_err(IndexError::other)?
                    .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                    .collect();
                for key in queue_keys {
                    let _ = queue_table.remove(&key);
                }

                // Clear index table
                let index_keys: Vec<[u8; 20]> = index_table
                    .iter()
                    .map_err(IndexError::other)?
                    .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                    .collect();
                for key in index_keys {
                    let _ = index_table.remove(&key);
                }
            }

            write_txn.commit().map_err(IndexError::other)?;
            Ok(())
        });

        match task.await {
            Ok(v) => v,
            Err(e) => Err(IndexError::other(e)),
        }
    }
}

/// Remove all entries matching a position_in_query + group_signature prefix
fn remove_by_prefix(
    queue_table: &mut redb::Table<[u8; 16], &[u8]>,
    index_table: &mut redb::Table<[u8; 20], [u8; 8]>,
    position_in_query: u32,
    group_signature: u64,
) -> Result<(), IndexError> {
    let prefix = encode_findex_prefix(position_in_query, group_signature);
    let (range_start, range_end) = findex_prefix_range(&prefix);

    // Collect entries to remove
    let entries_to_remove: Vec<([u8; 20], [u8; 8])> = {
        let iter = index_table
            .range(range_start..range_end)
            .map_err(IndexError::other)?;
        iter.filter_map(|r| r.ok().map(|(k, v)| (k.value(), v.value())))
            .collect()
    };

    // Remove from both tables
    for (index_key, due_time_bytes) in entries_to_remove {
        let due_time = u64::from_be_bytes(due_time_bytes);

        // Extract hash from index key (bytes 12..20)
        let mut hash_bytes = [0u8; 8];
        hash_bytes.copy_from_slice(&index_key[12..20]);
        let hash = u64::from_be_bytes(hash_bytes);

        let queue_key = encode_fqueue_key(due_time, hash);

        let _ = queue_table.remove(&queue_key);
        let _ = index_table.remove(&index_key);
    }

    Ok(())
}
