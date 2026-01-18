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

//! Redb Element Index Implementation
//!
//! This module provides the `RedbElementIndex` which implements the
//! `ElementIndex` and `ElementArchiveIndex` traits using Redb.
//!
//! # Table Layout
//!
//! - `elements`: hash128(source_id, element_id) -> StoredElement
//! - `slots`: hash128(source_id, element_id) -> Slot BitSet
//! - `inbound`: hash128(in_node) + slot(u16) + hash128(relation) -> ()
//! - `outbound`: hash128(out_node) + slot(u16) + hash128(relation) -> ()
//! - `archive`: hash128(element) + effective_from(u64) -> StoredElement

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, RwLock},
};

use async_stream::stream;
use async_trait::async_trait;
use bit_set::BitSet;
use drasi_core::{
    interface::{ElementArchiveIndex, ElementIndex, ElementStream, IndexError},
    models::{
        Element, ElementReference, ElementTimestamp, QueryJoin, QueryJoinKey, TimestampBound,
        TimestampRange,
    },
    path_solver::match_path::MatchPath,
};
use hashers::jenkins::spooky_hash::SpookyHasher;
use prost::{bytes::BytesMut, Message};
use redb::{Database, ReadableTable};
use tokio::task;

use crate::keys::{
    archive_range, archive_range_up_to, decode_inout_relation_hash, decode_partial_join_key,
    encode_archive_key, encode_inout_key, encode_partial_join_key, encode_partial_join_prefix,
    hash_element_ref, hash_stored_element_ref, inout_prefix_range, partial_join_prefix_range,
    ReferenceHash, ARCHIVE, ELEMENTS, INBOUND, OUTBOUND, PARTIAL, SLOTS,
};
use crate::storage_models::{
    StoredElement, StoredElementContainer, StoredElementMetadata, StoredElementReference,
    StoredRelation, StoredValue, StoredValueMap,
};

mod archive_index;

/// Configuration options for RedbElementIndex
#[derive(Clone)]
pub struct RedbIndexOptions {
    /// Enable archive index for point-in-time queries (past() function)
    pub archive_enabled: bool,
    /// Maximum cache size in bytes
    pub cache_size_bytes: u64,
}

/// Redb element index store
pub struct RedbElementIndex {
    context: Arc<Context>,
}

struct Context {
    db: Database,
    join_spec_by_label: RwLock<JoinSpecByLabel>,
    options: RedbIndexOptions,
}

type JoinSpecByLabel = HashMap<String, Vec<(Arc<QueryJoin>, Vec<usize>)>>;

impl RedbElementIndex {
    /// Create a new Redb element index.
    ///
    /// # Arguments
    ///
    /// * `query_id` - Unique identifier for the query
    /// * `path` - Base directory for database files
    /// * `options` - Configuration options
    pub fn new(query_id: &str, path: &str, options: RedbIndexOptions) -> Result<Self, IndexError> {
        let db_path = std::path::PathBuf::from(path)
            .join(query_id)
            .join("elements.redb");

        // Create parent directories if they don't exist
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(IndexError::other)?;
        }

        let db = Database::create(&db_path).map_err(IndexError::other)?;

        // Initialize tables by opening them once
        {
            let write_txn = db.begin_write().map_err(IndexError::other)?;
            {
                let _ = write_txn.open_table(ELEMENTS).map_err(IndexError::other)?;
                let _ = write_txn.open_table(SLOTS).map_err(IndexError::other)?;
                let _ = write_txn.open_table(INBOUND).map_err(IndexError::other)?;
                let _ = write_txn.open_table(OUTBOUND).map_err(IndexError::other)?;
                let _ = write_txn.open_table(PARTIAL).map_err(IndexError::other)?;
                if options.archive_enabled {
                    let _ = write_txn.open_table(ARCHIVE).map_err(IndexError::other)?;
                }
            }
            write_txn.commit().map_err(IndexError::other)?;
        }

        Ok(RedbElementIndex {
            context: Arc::new(Context {
                db,
                join_spec_by_label: RwLock::new(HashMap::new()),
                options,
            }),
        })
    }

    /// Check if archive is enabled
    pub fn is_archive_enabled(&self) -> bool {
        self.context.options.archive_enabled
    }
}

#[async_trait]
impl ElementIndex for RedbElementIndex {
    #[tracing::instrument(skip_all, err)]
    async fn get_element(
        &self,
        element_ref: &ElementReference,
    ) -> Result<Option<Arc<Element>>, IndexError> {
        let element_key = hash_element_ref(element_ref);
        let context = self.context.clone();

        let task = task::spawn_blocking(move || {
            let read_txn = context.db.begin_read().map_err(IndexError::other)?;
            let table = read_txn.open_table(ELEMENTS).map_err(IndexError::other)?;

            match table.get(&element_key).map_err(IndexError::other)? {
                Some(value) => {
                    let container = StoredElementContainer::decode(value.value())
                        .map_err(IndexError::other)?;
                    match container.element {
                        Some(stored) => {
                            let element: Element = stored.into();
                            Ok(Some(Arc::new(element)))
                        }
                        None => Err(IndexError::CorruptedData),
                    }
                }
                None => Ok(None),
            }
        });

        match task.await {
            Ok(result) => result,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    #[tracing::instrument(skip_all, err)]
    async fn set_element(
        &self,
        element: &Element,
        slot_affinity: &Vec<usize>,
    ) -> Result<(), IndexError> {
        let stored: StoredElement = element.into();
        let slot_affinity = slot_affinity.clone();
        let context = self.context.clone();

        let task = task::spawn_blocking(move || {
            let write_txn = context.db.begin_write().map_err(IndexError::other)?;
            set_element_internal(&context, &write_txn, stored, &slot_affinity)?;
            write_txn.commit().map_err(IndexError::other)?;
            Ok(())
        });

        match task.await {
            Ok(result) => result,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    #[tracing::instrument(skip_all, err)]
    async fn delete_element(&self, element_ref: &ElementReference) -> Result<(), IndexError> {
        let element_key = hash_element_ref(element_ref);
        let context = self.context.clone();

        let task = task::spawn_blocking(move || {
            let write_txn = context.db.begin_write().map_err(IndexError::other)?;
            delete_element_internal(&context, &write_txn, &element_key)?;
            write_txn.commit().map_err(IndexError::other)?;
            Ok(())
        });

        match task.await {
            Ok(result) => result,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    #[tracing::instrument(skip_all, err)]
    async fn get_slot_element_by_ref(
        &self,
        slot: usize,
        element_ref: &ElementReference,
    ) -> Result<Option<Arc<Element>>, IndexError> {
        let element_key = hash_element_ref(element_ref);
        let context = self.context.clone();

        let task = task::spawn_blocking(move || {
            let read_txn = context.db.begin_read().map_err(IndexError::other)?;
            let slots_table = read_txn.open_table(SLOTS).map_err(IndexError::other)?;

            // Check if element is in the requested slot
            let prev_slots = match slots_table.get(&element_key).map_err(IndexError::other)? {
                Some(v) => BitSet::from_bytes(v.value()),
                None => return Ok(None),
            };

            if !prev_slots.contains(slot) {
                return Ok(None);
            }

            // Get the element
            let elements_table = read_txn.open_table(ELEMENTS).map_err(IndexError::other)?;
            match elements_table
                .get(&element_key)
                .map_err(IndexError::other)?
            {
                Some(value) => {
                    let container = StoredElementContainer::decode(value.value())
                        .map_err(IndexError::other)?;
                    match container.element {
                        Some(stored) => {
                            let element: Element = stored.into();
                            Ok(Some(Arc::new(element)))
                        }
                        None => Err(IndexError::CorruptedData),
                    }
                }
                None => Ok(None),
            }
        });

        match task.await {
            Ok(result) => result,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    #[tracing::instrument(skip_all, err)]
    async fn get_slot_elements_by_inbound(
        &self,
        slot: usize,
        inbound_ref: &ElementReference,
    ) -> Result<ElementStream, IndexError> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let context = self.context.clone();
        let node_hash = hash_element_ref(inbound_ref);

        task::spawn_blocking(move || {
            let read_result = (|| -> Result<(), IndexError> {
                let read_txn = context.db.begin_read().map_err(IndexError::other)?;
                let inbound_table = read_txn.open_table(INBOUND).map_err(IndexError::other)?;
                let elements_table = read_txn.open_table(ELEMENTS).map_err(IndexError::other)?;

                let prefix = crate::keys::encode_inout_prefix(&node_hash, slot);
                let (range_start, range_end) = inout_prefix_range(&prefix);

                let iter = inbound_table
                    .range(range_start..range_end)
                    .map_err(IndexError::other)?;

                for item in iter {
                    match item {
                        Ok((key_guard, _)) => {
                            let key_bytes: [u8; 34] = key_guard.value();
                            let relation_hash = decode_inout_relation_hash(&key_bytes);

                            match elements_table.get(&relation_hash) {
                                Ok(Some(value)) => {
                                    match StoredElementContainer::decode(value.value()) {
                                        Ok(container) => {
                                            if let Some(stored) = container.element {
                                                let element: Element = stored.into();
                                                if tx.blocking_send(Ok(Arc::new(element))).is_err()
                                                {
                                                    break;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let _ = tx.blocking_send(Err(IndexError::other(e)));
                                            break;
                                        }
                                    }
                                }
                                Ok(None) => {
                                    // Stale reference - element was deleted
                                    log::debug!("Stale inbound reference: {relation_hash:?}");
                                }
                                Err(e) => {
                                    let _ = tx.blocking_send(Err(IndexError::other(e)));
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(IndexError::other(e)));
                            break;
                        }
                    }
                }
                Ok(())
            })();

            if let Err(e) = read_result {
                let _ = tx.blocking_send(Err(e));
            }
        });

        Ok(Box::pin(stream! {
            while let Some(item) = rx.recv().await {
                yield item;
            }
        }))
    }

    #[tracing::instrument(skip_all, err)]
    async fn get_slot_elements_by_outbound(
        &self,
        slot: usize,
        outbound_ref: &ElementReference,
    ) -> Result<ElementStream, IndexError> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let context = self.context.clone();
        let node_hash = hash_element_ref(outbound_ref);

        task::spawn_blocking(move || {
            let read_result = (|| -> Result<(), IndexError> {
                let read_txn = context.db.begin_read().map_err(IndexError::other)?;
                let outbound_table = read_txn.open_table(OUTBOUND).map_err(IndexError::other)?;
                let elements_table = read_txn.open_table(ELEMENTS).map_err(IndexError::other)?;

                let prefix = crate::keys::encode_inout_prefix(&node_hash, slot);
                let (range_start, range_end) = inout_prefix_range(&prefix);

                let iter = outbound_table
                    .range(range_start..range_end)
                    .map_err(IndexError::other)?;

                for item in iter {
                    match item {
                        Ok((key_guard, _)) => {
                            let key_bytes: [u8; 34] = key_guard.value();
                            let relation_hash = decode_inout_relation_hash(&key_bytes);

                            match elements_table.get(&relation_hash) {
                                Ok(Some(value)) => {
                                    match StoredElementContainer::decode(value.value()) {
                                        Ok(container) => {
                                            if let Some(stored) = container.element {
                                                let element: Element = stored.into();
                                                if tx.blocking_send(Ok(Arc::new(element))).is_err()
                                                {
                                                    break;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let _ = tx.blocking_send(Err(IndexError::other(e)));
                                            break;
                                        }
                                    }
                                }
                                Ok(None) => {
                                    // Stale reference - element was deleted
                                    log::debug!("Stale outbound reference: {relation_hash:?}");
                                }
                                Err(e) => {
                                    let _ = tx.blocking_send(Err(IndexError::other(e)));
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(IndexError::other(e)));
                            break;
                        }
                    }
                }
                Ok(())
            })();

            if let Err(e) = read_result {
                let _ = tx.blocking_send(Err(e));
            }
        });

        Ok(Box::pin(stream! {
            while let Some(item) = rx.recv().await {
                yield item;
            }
        }))
    }

    async fn clear(&self) -> Result<(), IndexError> {
        let context = self.context.clone();

        let task = task::spawn_blocking(move || {
            let write_txn = context.db.begin_write().map_err(IndexError::other)?;

            // Delete and recreate all tables
            // Note: Redb doesn't have a drop_table, so we clear by deleting all entries
            {
                let mut elements_table =
                    write_txn.open_table(ELEMENTS).map_err(IndexError::other)?;
                let mut slots_table = write_txn.open_table(SLOTS).map_err(IndexError::other)?;
                let mut inbound_table = write_txn.open_table(INBOUND).map_err(IndexError::other)?;
                let mut outbound_table =
                    write_txn.open_table(OUTBOUND).map_err(IndexError::other)?;

                // Collect keys and delete (can't delete while iterating)
                let element_keys: Vec<[u8; 16]> = elements_table
                    .iter()
                    .map_err(IndexError::other)?
                    .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                    .collect();
                for key in element_keys {
                    let _ = elements_table.remove(&key);
                }

                let slot_keys: Vec<[u8; 16]> = slots_table
                    .iter()
                    .map_err(IndexError::other)?
                    .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                    .collect();
                for key in slot_keys {
                    let _ = slots_table.remove(&key);
                }

                let inbound_keys: Vec<[u8; 34]> = inbound_table
                    .iter()
                    .map_err(IndexError::other)?
                    .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                    .collect();
                for key in inbound_keys {
                    let _ = inbound_table.remove(&key);
                }

                let outbound_keys: Vec<[u8; 34]> = outbound_table
                    .iter()
                    .map_err(IndexError::other)?
                    .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                    .collect();
                for key in outbound_keys {
                    let _ = outbound_table.remove(&key);
                }

                // Clear partial join index
                let mut partial_table = write_txn.open_table(PARTIAL).map_err(IndexError::other)?;
                let partial_keys: Vec<Vec<u8>> = partial_table
                    .iter()
                    .map_err(IndexError::other)?
                    .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                    .collect();
                for key in partial_keys {
                    let _ = partial_table.remove(key.as_slice());
                }

                if context.options.archive_enabled {
                    let mut archive_table =
                        write_txn.open_table(ARCHIVE).map_err(IndexError::other)?;
                    let archive_keys: Vec<[u8; 24]> = archive_table
                        .iter()
                        .map_err(IndexError::other)?
                        .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                        .collect();
                    for key in archive_keys {
                        let _ = archive_table.remove(&key);
                    }
                }
            }

            write_txn.commit().map_err(IndexError::other)?;
            Ok(())
        });

        match task.await {
            Ok(v) => v,
            Err(err) => Err(IndexError::other(err)),
        }
    }

    async fn set_joins(&self, match_path: &MatchPath, joins: &Vec<Arc<QueryJoin>>) {
        let joins_by_label = extract_join_spec_by_label(match_path, joins);
        if let Ok(mut join_spec_by_label) = self.context.join_spec_by_label.write() {
            join_spec_by_label.clone_from(&joins_by_label);
        }
    }
}

// Archive index implementation
#[async_trait]
impl ElementArchiveIndex for RedbElementIndex {
    #[tracing::instrument(skip_all, err)]
    async fn get_element_as_at(
        &self,
        element_ref: &ElementReference,
        time: ElementTimestamp,
    ) -> Result<Option<Arc<Element>>, IndexError> {
        if !self.context.options.archive_enabled {
            return Ok(None);
        }

        let element_hash = hash_element_ref(element_ref);
        let context = self.context.clone();

        let task = task::spawn_blocking(move || {
            let read_txn = context.db.begin_read().map_err(IndexError::other)?;
            let archive_table = read_txn.open_table(ARCHIVE).map_err(IndexError::other)?;

            let (range_start, range_end) = archive_range_up_to(&element_hash, time);

            let iter = archive_table
                .range(range_start..range_end)
                .map_err(IndexError::other)?;

            // Get the last (most recent) entry up to the given time
            let mut last_value: Option<Vec<u8>> = None;
            for item in iter {
                match item {
                    Ok((_, value)) => {
                        last_value = Some(value.value().to_vec());
                    }
                    Err(e) => return Err(IndexError::other(e)),
                }
            }

            match last_value {
                Some(bytes) => {
                    let container =
                        StoredElementContainer::decode(bytes.as_slice()).map_err(IndexError::other)?;
                    match container.element {
                        Some(stored) => Ok(Some(Arc::new(stored.into()))),
                        None => Err(IndexError::CorruptedData),
                    }
                }
                None => Ok(None),
            }
        });

        match task.await {
            Ok(result) => result,
            Err(e) => Err(IndexError::other(e)),
        }
    }

    #[tracing::instrument(skip_all, err)]
    async fn get_element_versions(
        &self,
        element_ref: &ElementReference,
        range: TimestampRange<ElementTimestamp>,
    ) -> Result<ElementStream, IndexError> {
        if !self.context.options.archive_enabled {
            return Err(IndexError::NotSupported);
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let context = self.context.clone();
        let element_hash = hash_element_ref(element_ref);

        // Extract from timestamp, handling StartFromPrevious case
        let from_time = match range.from {
            TimestampBound::Included(from) => from,
            TimestampBound::StartFromPrevious(from) => {
                // Need to find the previous archive entry to get its start time
                let ctx = context.clone();
                let hash = element_hash;
                let previous_time = task::spawn_blocking(move || {
                    let read_txn = ctx.db.begin_read().map_err(IndexError::other)?;
                    let archive_table = read_txn.open_table(ARCHIVE).map_err(IndexError::other)?;

                    // Search backwards from the 'from' time
                    let (range_start, range_end) = archive_range_up_to(&hash, from);
                    let iter = archive_table
                        .range(range_start..range_end)
                        .map_err(IndexError::other)?;

                    // Get the last entry's effective_from time
                    let mut last_time = from;
                    for (key_guard, _) in iter.flatten() {
                        let key_bytes: [u8; 24] = key_guard.value();
                        // Extract effective_from from key (bytes 16..24)
                        let mut time_bytes = [0u8; 8];
                        time_bytes.copy_from_slice(&key_bytes[16..24]);
                        last_time = u64::from_be_bytes(time_bytes);
                    }
                    Ok::<_, IndexError>(last_time)
                })
                .await
                .map_err(IndexError::other)??;
                previous_time
            }
        };
        let to_time = range.to;

        task::spawn_blocking(move || {
            let read_result = (|| -> Result<(), IndexError> {
                let read_txn = context.db.begin_read().map_err(IndexError::other)?;
                let archive_table = read_txn.open_table(ARCHIVE).map_err(IndexError::other)?;

                let (range_start, range_end) = archive_range(&element_hash, from_time, to_time);

                let iter = archive_table
                    .range(range_start..range_end)
                    .map_err(IndexError::other)?;

                for item in iter {
                    match item {
                        Ok((_, value)) => {
                            match StoredElementContainer::decode(value.value()) {
                                Ok(container) => {
                                    if let Some(stored) = container.element {
                                        let element: Element = stored.into();
                                        if tx.blocking_send(Ok(Arc::new(element))).is_err() {
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ = tx.blocking_send(Err(IndexError::other(e)));
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(IndexError::other(e)));
                            break;
                        }
                    }
                }
                Ok(())
            })();

            if let Err(e) = read_result {
                let _ = tx.blocking_send(Err(e));
            }
        });

        Ok(Box::pin(stream! {
            while let Some(item) = rx.recv().await {
                yield item;
            }
        }))
    }

    async fn clear(&self) -> Result<(), IndexError> {
        // Clear is handled by ElementIndex::clear which clears archive too
        ElementIndex::clear(self).await
    }
}

// Helper functions

fn slots_to_bitset(slots: &[usize]) -> BitSet {
    let mut bitset = BitSet::new();
    for slot in slots {
        bitset.insert(*slot);
    }
    bitset
}

fn extract_join_spec_by_label(
    match_path: &MatchPath,
    joins: &[Arc<QueryJoin>],
) -> JoinSpecByLabel {
    let mut result: HashMap<String, Vec<(Arc<QueryJoin>, Vec<usize>)>> = HashMap::new();

    for join in joins {
        let mut slots = Vec::new();
        for (slot_num, slot) in match_path.slots.iter().enumerate() {
            if slot.spec.labels.contains(&Arc::from(join.id.clone())) {
                slots.push(slot_num);
            }
        }
        if slots.is_empty() {
            continue;
        }

        for jk in &join.keys {
            result
                .entry(jk.label.clone())
                .or_default()
                .push((join.clone(), slots.clone()));
        }
    }

    result
}

// =============================================================================
// Internal transaction-aware helper functions for partial join support
// =============================================================================

/// Get element from the database using a write transaction.
/// This allows reading within the same transaction that writes are happening.
fn get_element_from_txn(
    txn: &redb::WriteTransaction,
    element_key: &ReferenceHash,
) -> Result<Option<StoredElement>, IndexError> {
    let elements_table = txn.open_table(ELEMENTS).map_err(IndexError::other)?;

    let value_bytes = elements_table
        .get(element_key)
        .map_err(IndexError::other)?
        .map(|value| value.value().to_vec());
    drop(elements_table);

    match value_bytes {
        Some(bytes) => {
            let container =
                StoredElementContainer::decode(bytes.as_slice()).map_err(IndexError::other)?;
            Ok(container.element)
        }
        None => Ok(None),
    }
}

/// Internal set_element that takes a transaction reference.
/// This allows recursive calls within the same transaction (for virtual relations).
fn set_element_internal(
    context: &Context,
    txn: &redb::WriteTransaction,
    element: StoredElement,
    slot_affinity: &[usize],
) -> Result<(), IndexError> {
    let eref = element.get_reference();
    let key_hash = hash_stored_element_ref(eref);

    // Get previous element BEFORE storing the new one (needed for partial join tracking)
    let old_element = get_element_from_txn(txn, &key_hash)?;

    // Get previous slots for relation index cleanup
    let slots_table = txn.open_table(SLOTS).map_err(IndexError::other)?;
    let prev_slots = slots_table
        .get(&key_hash)
        .map_err(IndexError::other)?
        .map(|v| BitSet::from_bytes(v.value()));
    drop(slots_table);

    let new_slots = slots_to_bitset(slot_affinity);

    // Get relation nodes if this is a relation
    let relation_nodes = match &element {
        StoredElement::Relation(r) => Some((r.in_node.clone(), r.out_node.clone())),
        _ => None,
    };

    // Encode element
    let mut buf = BytesMut::new();
    let container = StoredElementContainer {
        element: Some(element.clone()),
    };
    container.encode(&mut buf).map_err(IndexError::other)?;
    let encoded = buf.freeze();

    // Store element
    {
        let mut elements_table = txn.open_table(ELEMENTS).map_err(IndexError::other)?;
        elements_table
            .insert(&key_hash, encoded.as_ref())
            .map_err(IndexError::other)?;
    }

    // Store slots
    {
        let mut slots_table = txn.open_table(SLOTS).map_err(IndexError::other)?;
        slots_table
            .insert(&key_hash, new_slots.clone().into_bit_vec().to_bytes().as_slice())
            .map_err(IndexError::other)?;
    }

    // Handle relation inbound/outbound indexes
    if let Some((in_node, out_node)) = relation_nodes {
        let in_hash = hash_stored_element_ref(&in_node);
        let out_hash = hash_stored_element_ref(&out_node);

        let mut inbound_table = txn.open_table(INBOUND).map_err(IndexError::other)?;
        let mut outbound_table = txn.open_table(OUTBOUND).map_err(IndexError::other)?;

        // Remove old slot indexes if slots changed
        if let Some(prev_slots) = prev_slots {
            if prev_slots != new_slots {
                for slot in prev_slots.into_iter() {
                    let inbound_key = encode_inout_key(&in_hash, slot, &key_hash);
                    let outbound_key = encode_inout_key(&out_hash, slot, &key_hash);
                    let _ = inbound_table.remove(&inbound_key);
                    let _ = outbound_table.remove(&outbound_key);
                }
            }
        }

        // Add new slot indexes
        for slot in slot_affinity.iter() {
            let inbound_key = encode_inout_key(&in_hash, *slot, &key_hash);
            let outbound_key = encode_inout_key(&out_hash, *slot, &key_hash);
            inbound_table
                .insert(&inbound_key, ())
                .map_err(IndexError::other)?;
            outbound_table
                .insert(&outbound_key, ())
                .map_err(IndexError::other)?;
        }
    }

    // Update partial join index (this may recursively call set_element_internal)
    update_source_joins(context, txn, &element, old_element.as_ref())?;

    // Archive if enabled
    if context.options.archive_enabled {
        let mut archive_table = txn.open_table(ARCHIVE).map_err(IndexError::other)?;
        let archive_key = encode_archive_key(&key_hash, element.get_effective_from());
        archive_table
            .insert(&archive_key, encoded.as_ref())
            .map_err(IndexError::other)?;
    }

    Ok(())
}

/// Internal delete_element that takes a transaction reference.
/// This allows recursive calls within the same transaction (for virtual relations).
fn delete_element_internal(
    context: &Context,
    txn: &redb::WriteTransaction,
    element_key: &ReferenceHash,
) -> Result<(), IndexError> {
    // Get previous slots and element for cleanup
    let slots_table = txn.open_table(SLOTS).map_err(IndexError::other)?;
    let prev_slots = slots_table
        .get(element_key)
        .map_err(IndexError::other)?
        .map(|v| BitSet::from_bytes(v.value()));
    drop(slots_table);

    // Get old element to delete source joins
    let prev_element = get_element_from_txn(txn, element_key)?;

    // Delete source joins first (this may recursively call delete_element_internal)
    if let Some(ref old_element) = prev_element {
        delete_source_joins(context, txn, old_element)?;
    }

    // Delete element
    {
        let mut elements_table = txn.open_table(ELEMENTS).map_err(IndexError::other)?;
        let _ = elements_table.remove(element_key);
    }

    // Delete slots
    {
        let mut slots_table = txn.open_table(SLOTS).map_err(IndexError::other)?;
        let _ = slots_table.remove(element_key);
    }

    // Clean up inbound/outbound indexes for relations
    if let (Some(prev_slots), Some(StoredElement::Relation(r))) = (prev_slots, prev_element) {
        let in_hash = hash_stored_element_ref(&r.in_node);
        let out_hash = hash_stored_element_ref(&r.out_node);

        let mut inbound_table = txn.open_table(INBOUND).map_err(IndexError::other)?;
        let mut outbound_table = txn.open_table(OUTBOUND).map_err(IndexError::other)?;

        for slot in prev_slots.into_iter() {
            let inbound_key = encode_inout_key(&in_hash, slot, element_key);
            let outbound_key = encode_inout_key(&out_hash, slot, element_key);
            let _ = inbound_table.remove(&inbound_key);
            let _ = outbound_table.remove(&outbound_key);
        }
    }

    Ok(())
}

/// Update partial join index when a node is inserted/updated.
/// Creates virtual relations between nodes that share join key values.
fn update_source_joins(
    context: &Context,
    txn: &redb::WriteTransaction,
    new_element: &StoredElement,
    old_element: Option<&StoredElement>,
) -> Result<(), IndexError> {
    let node = match new_element {
        StoredElement::Node(n) => n,
        _ => return Ok(()),
    };

    let join_spec_by_label = context
        .join_spec_by_label
        .read()
        .map_err(|_| IndexError::CorruptedData)?;

    for (label, joins) in join_spec_by_label.iter() {
        if !node.metadata.labels.contains(label) {
            continue;
        }

        for (qj, slots) in joins {
            for qjk in &qj.keys {
                if qjk.label != *label {
                    continue;
                }

                let new_value = match node.properties.get(&qjk.property) {
                    Some(v) => v,
                    None => continue,
                };

                // Check if property value actually changed
                if let Some(StoredElement::Node(old)) = old_element {
                    if let Some(old_value) = old.properties.get(&qjk.property) {
                        if old_value == new_value {
                            continue;
                        }
                    }
                }

                // Insert into PARTIAL table
                let pj_prefix =
                    encode_partial_join_prefix(&qj.id, new_value, &qjk.label, &qjk.property);
                let pj_key = encode_partial_join_key(&pj_prefix, &node.metadata.reference);

                let did_insert = {
                    let mut partial_table = txn.open_table(PARTIAL).map_err(IndexError::other)?;
                    let exists = partial_table
                        .get(pj_key.as_slice())
                        .map_err(IndexError::other)?
                        .is_some();
                    if exists {
                        false
                    } else {
                        partial_table
                            .insert(pj_key.as_slice(), ())
                            .map_err(IndexError::other)?;
                        true
                    }
                };

                if did_insert {
                    // Remove old partial joins if value changed
                    if let Some(StoredElement::Node(old)) = old_element {
                        if let Some(old_value) = old.properties.get(&qjk.property) {
                            delete_source_join(
                                context,
                                txn,
                                old_element.expect("checked above").get_reference(),
                                qj,
                                qjk,
                                old_value,
                            )?;
                        }
                    }

                    let element_reference = node.metadata.reference.clone();

                    // Find matching counterparts - collect into Vec to avoid holding borrow
                    for qjk2 in &qj.keys {
                        if qjk == qjk2 {
                            continue;
                        }

                        let other_pj_prefix = encode_partial_join_prefix(
                            &qj.id,
                            new_value,
                            &qjk2.label,
                            &qjk2.property,
                        );
                        let (range_start, range_end) = partial_join_prefix_range(&other_pj_prefix);

                        // Collect matching counterparts into a Vec before recursive calls
                        let counterparts: Vec<StoredElementReference> = {
                            let partial_table =
                                txn.open_table(PARTIAL).map_err(IndexError::other)?;
                            let iter = partial_table
                                .range(range_start.as_slice()..range_end.as_slice())
                                .map_err(IndexError::other)?;

                            let mut result = Vec::new();
                            for item in iter {
                                let (key, _) = item.map_err(IndexError::other)?;
                                let other = decode_partial_join_key(key.value())?;
                                result.push(other);
                            }
                            result
                        };

                        // Now create virtual relations (recursive calls are safe)
                        for other in counterparts {
                            let in_out = StoredElement::Relation(StoredRelation {
                                metadata: StoredElementMetadata {
                                    reference: get_join_virtual_ref(&element_reference, &other),
                                    labels: vec![qj.id.clone()],
                                    effective_from: node.metadata.effective_from,
                                },
                                in_node: element_reference.clone(),
                                out_node: other.clone(),
                                properties: StoredValueMap::new(),
                            });

                            let out_in = StoredElement::Relation(StoredRelation {
                                metadata: StoredElementMetadata {
                                    reference: get_join_virtual_ref(&other, &element_reference),
                                    labels: vec![qj.id.clone()],
                                    effective_from: node.metadata.effective_from,
                                },
                                in_node: other.clone(),
                                out_node: element_reference.clone(),
                                properties: StoredValueMap::new(),
                            });

                            set_element_internal(context, txn, in_out, slots)?;
                            set_element_internal(context, txn, out_in, slots)?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Delete partial join entries when a node is deleted.
fn delete_source_joins(
    context: &Context,
    txn: &redb::WriteTransaction,
    old_element: &StoredElement,
) -> Result<(), IndexError> {
    let node = match old_element {
        StoredElement::Node(n) => n,
        _ => return Ok(()),
    };

    let join_spec_by_label = context
        .join_spec_by_label
        .read()
        .map_err(|_| IndexError::CorruptedData)?;

    for (label, joins) in join_spec_by_label.iter() {
        if !node.metadata.labels.contains(label) {
            continue;
        }

        for (qj, _slots) in joins {
            for qjk in &qj.keys {
                if qjk.label != *label {
                    continue;
                }

                if let Some(value) = node.properties.get(&qjk.property) {
                    delete_source_join(
                        context,
                        txn,
                        old_element.get_reference(),
                        qj,
                        qjk,
                        value,
                    )?;
                }
            }
        }
    }

    Ok(())
}

/// Delete a single partial join entry and its associated virtual relations.
fn delete_source_join(
    context: &Context,
    txn: &redb::WriteTransaction,
    old_element: &StoredElementReference,
    query_join: &QueryJoin,
    join_key: &QueryJoinKey,
    value: &StoredValue,
) -> Result<(), IndexError> {
    let pj_prefix =
        encode_partial_join_prefix(&query_join.id, value, &join_key.label, &join_key.property);
    let pj_key = encode_partial_join_key(&pj_prefix, old_element);

    let did_remove = {
        let mut partial_table = txn.open_table(PARTIAL).map_err(IndexError::other)?;
        let exists = partial_table
            .get(pj_key.as_slice())
            .map_err(IndexError::other)?
            .is_some();
        if exists {
            partial_table
                .remove(pj_key.as_slice())
                .map_err(IndexError::other)?;
            true
        } else {
            false
        }
    };

    if did_remove {
        // Delete virtual relations with counterparts - collect into Vec first
        for qjk2 in &query_join.keys {
            if join_key == qjk2 {
                continue;
            }

            let other_pj_prefix = encode_partial_join_prefix(
                &query_join.id,
                value,
                &qjk2.label,
                &qjk2.property,
            );
            let (range_start, range_end) = partial_join_prefix_range(&other_pj_prefix);

            // Collect counterparts into Vec before recursive calls
            let counterparts: Vec<StoredElementReference> = {
                let partial_table = txn.open_table(PARTIAL).map_err(IndexError::other)?;
                let iter = partial_table
                    .range(range_start.as_slice()..range_end.as_slice())
                    .map_err(IndexError::other)?;

                let mut result = Vec::new();
                for item in iter {
                    let (key, _) = item.map_err(IndexError::other)?;
                    let other = decode_partial_join_key(key.value())?;
                    result.push(other);
                }
                result
            };

            // Now delete virtual relations (recursive calls are safe)
            for other in counterparts {
                let in_out = get_join_virtual_ref(old_element, &other);
                let out_in = get_join_virtual_ref(&other, old_element);

                delete_element_internal(context, txn, &hash_stored_element_ref(&in_out))?;
                delete_element_internal(context, txn, &hash_stored_element_ref(&out_in))?;
            }
        }
    }

    Ok(())
}

/// Create a virtual relation reference from two element references.
fn get_join_virtual_ref(
    ref1: &StoredElementReference,
    ref2: &StoredElementReference,
) -> StoredElementReference {
    let new_id = format!("{}:{}", ref1.element_id, ref2.element_id);
    StoredElementReference {
        source_id: "$join".to_string(),
        element_id: new_id,
    }
}
