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

//! Redb Result Index Implementation
//!
//! This module provides the `RedbResultIndex` which implements the
//! `ResultIndex` composite trait (AccumulatorIndex + LazySortedSetStore + ResultSequenceCounter).
//!
//! # Table Layout
//!
//! - `result_values`: hash(owner, key) -> StoredValueAccumulator
//! - `sorted_sets`: set_id(u64) + sortable_f64(12 bytes) -> count(i64)
//! - `result_metadata`: "sequence" -> u64, "source_change_id" -> String

use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use async_trait::async_trait;
use drasi_core::{
    evaluation::functions::aggregation::ValueAccumulator,
    interface::{
        AccumulatorIndex, IndexError, LazySortedSetStore, ResultIndex, ResultKey, ResultOwner,
        ResultSequence, ResultSequenceCounter,
    },
};
use hashers::jenkins::spooky_hash::SpookyHasher;
use ordered_float::OrderedFloat;
use prost::Message;
use redb::{Database, ReadableTable};
use tokio::task;

use crate::keys::{
    encode_sorted_set_key, encode_sorted_set_prefix, sorted_set_range_from, RESULT_METADATA,
    RESULT_VALUES, SORTED_SETS,
};
use crate::storage_models::{StoredValueAccumulator, StoredValueAccumulatorContainer};

const SEQUENCE_KEY: &str = "sequence";
const SOURCE_CHANGE_ID_KEY: &str = "source_change_id";

/// Redb result index store
pub struct RedbResultIndex {
    db: Arc<Database>,
}

impl RedbResultIndex {
    /// Create a new Redb result index.
    ///
    /// # Arguments
    ///
    /// * `query_id` - Unique identifier for the query
    /// * `path` - Base directory for database files
    /// * `cache_size_bytes` - Maximum cache size (currently unused, reserved for future)
    pub fn new(query_id: &str, path: &str, _cache_size_bytes: u64) -> Result<Self, IndexError> {
        let db_path = std::path::PathBuf::from(path)
            .join(query_id)
            .join("results.redb");

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
                    .open_table(RESULT_VALUES)
                    .map_err(IndexError::other)?;
                let _ = write_txn
                    .open_table(SORTED_SETS)
                    .map_err(IndexError::other)?;
                let _ = write_txn
                    .open_table(RESULT_METADATA)
                    .map_err(IndexError::other)?;
            }
            write_txn.commit().map_err(IndexError::other)?;
        }

        Ok(RedbResultIndex { db: Arc::new(db) })
    }
}

impl ResultIndex for RedbResultIndex {}

#[async_trait]
impl AccumulatorIndex for RedbResultIndex {
    #[tracing::instrument(name = "ari::get", skip_all, err)]
    async fn get(
        &self,
        key: &ResultKey,
        owner: &ResultOwner,
    ) -> Result<Option<ValueAccumulator>, IndexError> {
        let db = self.db.clone();
        let set_id = get_hash_key(owner, key);

        let task = task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(IndexError::other)?;
            let values_table = read_txn
                .open_table(RESULT_VALUES)
                .map_err(IndexError::other)?;

            let key_bytes = set_id.to_be_bytes();
            match values_table
                .get(key_bytes.as_slice())
                .map_err(IndexError::other)?
            {
                Some(v) => {
                    let container = StoredValueAccumulatorContainer::decode(v.value())
                        .map_err(IndexError::other)?;
                    match container.value {
                        Some(value) => Ok(Some(value.into())),
                        None => Err(IndexError::CorruptedData),
                    }
                }
                None => Ok(None),
            }
        });

        match task.await {
            Ok(v) => v,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    #[tracing::instrument(name = "ari::set", skip_all, err)]
    async fn set(
        &self,
        key: ResultKey,
        owner: ResultOwner,
        value: Option<ValueAccumulator>,
    ) -> Result<(), IndexError> {
        let db = self.db.clone();
        let set_id = get_hash_key(&owner, &key);

        let task = task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(IndexError::other)?;
            {
                let mut values_table = write_txn
                    .open_table(RESULT_VALUES)
                    .map_err(IndexError::other)?;

                let key_bytes = set_id.to_be_bytes();
                match value {
                    None => {
                        let _ = values_table.remove(key_bytes.as_slice());
                    }
                    Some(v) => {
                        let stored: StoredValueAccumulator = v.into();
                        let serialized = stored.serialize();
                        values_table
                            .insert(key_bytes.as_slice(), serialized.as_ref())
                            .map_err(IndexError::other)?;
                    }
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

    async fn clear(&self) -> Result<(), IndexError> {
        let db = self.db.clone();

        let task = task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(IndexError::other)?;
            {
                // Clear values table
                let mut values_table = write_txn
                    .open_table(RESULT_VALUES)
                    .map_err(IndexError::other)?;
                let keys: Vec<Vec<u8>> = values_table
                    .iter()
                    .map_err(IndexError::other)?
                    .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                    .collect();
                for key in keys {
                    let _ = values_table.remove(key.as_slice());
                }

                // Clear sorted sets table
                let mut sets_table = write_txn
                    .open_table(SORTED_SETS)
                    .map_err(IndexError::other)?;
                let set_keys: Vec<[u8; 20]> = sets_table
                    .iter()
                    .map_err(IndexError::other)?
                    .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                    .collect();
                for key in set_keys {
                    let _ = sets_table.remove(&key);
                }

                // Clear metadata table
                let mut metadata_table = write_txn
                    .open_table(RESULT_METADATA)
                    .map_err(IndexError::other)?;
                let _ = metadata_table.remove(SEQUENCE_KEY);
                let _ = metadata_table.remove(SOURCE_CHANGE_ID_KEY);
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

#[async_trait]
impl LazySortedSetStore for RedbResultIndex {
    #[tracing::instrument(name = "lss::get_next", skip_all, err)]
    async fn get_next(
        &self,
        set_id: u64,
        value: Option<OrderedFloat<f64>>,
    ) -> Result<Option<(OrderedFloat<f64>, isize)>, IndexError> {
        let db = self.db.clone();

        let task = task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(IndexError::other)?;
            let sets_table = read_txn
                .open_table(SORTED_SETS)
                .map_err(IndexError::other)?;

            let (range_start, range_end) =
                sorted_set_range_from(set_id, value.map(|v| v.into_inner()));

            let mut iter = sets_table
                .range(range_start..range_end)
                .map_err(IndexError::other)?;

            match iter.next() {
                Some(Ok((key_guard, value_guard))) => {
                    let key_bytes: [u8; 20] = key_guard.value();
                    let count_bytes: [u8; 8] = value_guard.value();

                    // Decode value from key bytes (bytes 8..20 is the sortable f64)
                    let mut f64_bytes = [0u8; 12];
                    f64_bytes.copy_from_slice(&key_bytes[8..20]);
                    let decoded_value = decode_sortable_f64(&f64_bytes);

                    // Decode count
                    let count = i64::from_be_bytes(count_bytes) as isize;

                    Ok(Some((OrderedFloat(decoded_value), count)))
                }
                Some(Err(e)) => Err(IndexError::other(e)),
                None => Ok(None),
            }
        });

        match task.await {
            Ok(v) => v,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    #[tracing::instrument(name = "lss::get_value_count", skip_all, err)]
    async fn get_value_count(
        &self,
        set_id: u64,
        value: OrderedFloat<f64>,
    ) -> Result<isize, IndexError> {
        let db = self.db.clone();

        let task = task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(IndexError::other)?;
            let sets_table = read_txn
                .open_table(SORTED_SETS)
                .map_err(IndexError::other)?;

            let key = encode_sorted_set_key(set_id, value.into_inner());
            match sets_table.get(&key).map_err(IndexError::other)? {
                Some(v) => {
                    let count_bytes: [u8; 8] = v.value();
                    Ok(i64::from_be_bytes(count_bytes) as isize)
                }
                None => Ok(0),
            }
        });

        match task.await {
            Ok(v) => v,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    #[tracing::instrument(name = "lss::increment_value_count", skip_all, err)]
    async fn increment_value_count(
        &self,
        set_id: u64,
        value: OrderedFloat<f64>,
        delta: isize,
    ) -> Result<(), IndexError> {
        let db = self.db.clone();

        let task = task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(IndexError::other)?;
            {
                let mut sets_table = write_txn
                    .open_table(SORTED_SETS)
                    .map_err(IndexError::other)?;

                let key = encode_sorted_set_key(set_id, value.into_inner());

                // Read current value
                let current = match sets_table.get(&key).map_err(IndexError::other)? {
                    Some(v) => {
                        let count_bytes: [u8; 8] = v.value();
                        i64::from_be_bytes(count_bytes)
                    }
                    None => 0,
                };

                let new_value = current + delta as i64;

                if new_value == 0 {
                    // Remove entry when count reaches 0
                    let _ = sets_table.remove(&key);
                } else {
                    sets_table
                        .insert(&key, &new_value.to_be_bytes())
                        .map_err(IndexError::other)?;
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

#[async_trait]
impl ResultSequenceCounter for RedbResultIndex {
    #[tracing::instrument(name = "rsc::apply_sequence", skip_all, err)]
    async fn apply_sequence(&self, sequence: u64, source_change_id: &str) -> Result<(), IndexError> {
        let db = self.db.clone();
        let source_change_id = source_change_id.to_string();

        let task = task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(IndexError::other)?;
            {
                let mut metadata_table = write_txn
                    .open_table(RESULT_METADATA)
                    .map_err(IndexError::other)?;

                metadata_table
                    .insert(SEQUENCE_KEY, sequence.to_be_bytes().as_slice())
                    .map_err(IndexError::other)?;

                metadata_table
                    .insert(SOURCE_CHANGE_ID_KEY, source_change_id.as_bytes())
                    .map_err(IndexError::other)?;
            }
            write_txn.commit().map_err(IndexError::other)?;
            Ok(())
        });

        match task.await {
            Ok(v) => v,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    #[tracing::instrument(name = "rsc::get_sequence", skip_all, err)]
    async fn get_sequence(&self) -> Result<ResultSequence, IndexError> {
        let db = self.db.clone();

        let task = task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(IndexError::other)?;
            let metadata_table = read_txn
                .open_table(RESULT_METADATA)
                .map_err(IndexError::other)?;

            let sequence = match metadata_table
                .get(SEQUENCE_KEY)
                .map_err(IndexError::other)?
            {
                Some(v) => {
                    let bytes = v.value();
                    if bytes.len() >= 8 {
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(&bytes[..8]);
                        u64::from_be_bytes(buf)
                    } else {
                        0
                    }
                }
                None => 0,
            };

            let source_change_id = match metadata_table
                .get(SOURCE_CHANGE_ID_KEY)
                .map_err(IndexError::other)?
            {
                Some(v) => {
                    String::from_utf8(v.value().to_vec()).unwrap_or_default()
                }
                None => String::new(),
            };

            Ok(ResultSequence {
                sequence,
                source_change_id: Arc::from(source_change_id),
            })
        });

        match task.await {
            Ok(v) => v,
            Err(e) => Err(IndexError::other(e)),
        }
    }
}

// Helper functions

fn get_hash_key(owner: &ResultOwner, key: &ResultKey) -> u64 {
    let mut hasher = SpookyHasher::default();
    owner.hash(&mut hasher);
    key.hash(&mut hasher);
    hasher.finish()
}

/// Decode a sortable f64 from its 12-byte representation.
fn decode_sortable_f64(key: &[u8; 12]) -> f64 {
    let mut int_buf = [0u8; 8];
    int_buf.copy_from_slice(&key[0..8]);
    let offset_int = u64::from_be_bytes(int_buf);
    let int_part = (offset_int as i128 - i64::MAX as i128) as f64;

    let mut frac_buf = [0u8; 4];
    frac_buf.copy_from_slice(&key[8..12]);
    let frac = u32::from_be_bytes(frac_buf) as f64 / 1_000_000_000.0;

    if int_part < 0.0 {
        int_part - frac
    } else {
        int_part + frac
    }
}
