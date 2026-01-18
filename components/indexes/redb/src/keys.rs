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

//! Key encoding utilities for Redb tables.
//!
//! All numeric keys use **Big-Endian encoding** to preserve lexicographic sort order
//! in Redb's B-Tree. This is critical for correct ordering of timestamps, sequence IDs,
//! and range queries.
//!
//! # Key Formats
//!
//! | Key Type | Size | Format |
//! |----------|------|--------|
//! | Element Reference | 16 bytes | `fastmurmur3::hash(source_id + element_id).to_be_bytes()` |
//! | Inbound/Outbound | 34 bytes | `hash128(node) + slot(u16 BE) + hash128(relation)` |
//! | Archive | 24 bytes | `hash128(element) + effective_from(u64 BE)` |
//! | Future Queue | 16 bytes | `due_time(u64 BE) + hash64` |
//! | Future Index | 20 bytes | `position(u32 BE) + group_sig(u64 BE) + hash64` |

use std::hash::Hash;

use drasi_core::interface::IndexError;
use drasi_core::models::ElementReference;
use hashers::jenkins::spooky_hash::SpookyHasher;
use redb::TableDefinition;

use crate::storage_models::{StoredElementReference, StoredValue};

/// 128-bit hash of an ElementReference
pub type ReferenceHash = [u8; 16];

// =============================================================================
// Table Definitions
// =============================================================================

/// Element storage: hash128(source_id, element_id) -> StoredElement (protobuf)
pub const ELEMENTS: TableDefinition<[u8; 16], &[u8]> = TableDefinition::new("elements");

/// Slot affinity: hash128(source_id, element_id) -> BitSet bytes
pub const SLOTS: TableDefinition<[u8; 16], &[u8]> = TableDefinition::new("slots");

/// Inbound index: hash128(in_node) + slot(u16) + hash128(relation) -> ()
/// Used for: "find all relations where in_node = X in slot Y"
pub const INBOUND: TableDefinition<[u8; 34], ()> = TableDefinition::new("inbound");

/// Outbound index: hash128(out_node) + slot(u16) + hash128(relation) -> ()
/// Used for: "find all relations where out_node = X in slot Y"
pub const OUTBOUND: TableDefinition<[u8; 34], ()> = TableDefinition::new("outbound");

/// Partial join index: variable-length key -> ()
/// Key format: hash128(join_label + field_value + label + property) + source_id_len + source_id + element_id
pub const PARTIAL: TableDefinition<&[u8], ()> = TableDefinition::new("partial");

/// Archive index: hash128(element) + effective_from(u64 BE) -> StoredElement
/// Used for point-in-time queries (past() function)
pub const ARCHIVE: TableDefinition<[u8; 24], &[u8]> = TableDefinition::new("archive");

/// Result values: hash of (owner, key) -> StoredValueAccumulator
pub const RESULT_VALUES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("result_values");

/// Sorted sets: set_id(u64 BE) + sortable_f64(12 bytes) -> count(i64 BE)
pub const SORTED_SETS: TableDefinition<[u8; 20], [u8; 8]> = TableDefinition::new("sorted_sets");

/// Result metadata: "sequence" -> u64, "source_change_id" -> String
pub const RESULT_METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("result_metadata");

/// Future queue: due_time(u64 BE) + hash64 -> StoredFutureElementRef
/// Ordered by due_time for priority queue semantics
pub const FUTURE_QUEUE: TableDefinition<[u8; 16], &[u8]> = TableDefinition::new("future_queue");

/// Future index: position(u32 BE) + group_sig(u64 BE) + hash64 -> due_time(u64 BE)
/// Used for O(1) lookup by position + group_signature
pub const FUTURE_INDEX: TableDefinition<[u8; 20], [u8; 8]> =
    TableDefinition::new("future_index");

// =============================================================================
// Element Reference Hashing
// =============================================================================

/// Hash an ElementReference to a 128-bit key using fastmurmur3.
/// Result is in big-endian format for correct B-Tree ordering.
pub fn hash_element_ref(element_ref: &ElementReference) -> ReferenceHash {
    let bytes: Vec<u8> = element_ref
        .source_id
        .as_bytes()
        .iter()
        .chain(element_ref.element_id.as_bytes())
        .copied()
        .collect();

    fastmurmur3::hash(bytes.as_slice()).to_be_bytes()
}

/// Hash a StoredElementReference to a 128-bit key.
pub fn hash_stored_element_ref(element_ref: &StoredElementReference) -> ReferenceHash {
    let bytes: Vec<u8> = element_ref
        .source_id
        .as_bytes()
        .iter()
        .chain(element_ref.element_id.as_bytes())
        .copied()
        .collect();

    fastmurmur3::hash(bytes.as_slice()).to_be_bytes()
}

// =============================================================================
// Inbound/Outbound Index Keys
// =============================================================================

/// Encode inbound/outbound index key: hash128(node) + slot(u16 BE) + hash128(relation)
/// Total: 16 + 2 + 16 = 34 bytes
pub fn encode_inout_key(
    node_hash: &ReferenceHash,
    slot: usize,
    relation_hash: &ReferenceHash,
) -> [u8; 34] {
    let mut key = [0u8; 34];
    key[0..16].copy_from_slice(node_hash);
    key[16..18].copy_from_slice(&(slot as u16).to_be_bytes());
    key[18..34].copy_from_slice(relation_hash);
    key
}

/// Encode prefix for inbound/outbound range scans: hash128(node) + slot(u16 BE)
/// Total: 16 + 2 = 18 bytes
pub fn encode_inout_prefix(node_hash: &ReferenceHash, slot: usize) -> [u8; 18] {
    let mut key = [0u8; 18];
    key[0..16].copy_from_slice(node_hash);
    key[16..18].copy_from_slice(&(slot as u16).to_be_bytes());
    key
}

/// Decode relation hash from an inout key (bytes 18..34)
pub fn decode_inout_relation_hash(key: &[u8; 34]) -> ReferenceHash {
    let mut relation_hash = [0u8; 16];
    relation_hash.copy_from_slice(&key[18..34]);
    relation_hash
}

/// Create range bounds for inout prefix scan.
/// Returns (start_inclusive, end_exclusive).
pub fn inout_prefix_range(prefix: &[u8; 18]) -> ([u8; 34], [u8; 34]) {
    let mut start = [0u8; 34];
    start[0..18].copy_from_slice(prefix);
    // suffix is all zeros

    let mut end = [0u8; 34];
    end[0..18].copy_from_slice(prefix);
    increment_bytes(&mut end[0..18]);

    (start, end)
}

// =============================================================================
// Archive Index Keys
// =============================================================================

/// Encode archive key: hash128(element_ref) + effective_from(u64 BE)
/// Total: 16 + 8 = 24 bytes
pub fn encode_archive_key(element_hash: &ReferenceHash, effective_from: u64) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[0..16].copy_from_slice(element_hash);
    key[16..24].copy_from_slice(&effective_from.to_be_bytes());
    key
}

/// Create range bounds for archive queries up to a specific time.
/// Returns (start_inclusive, end_exclusive).
pub fn archive_range_up_to(element_hash: &ReferenceHash, time: u64) -> ([u8; 24], [u8; 24]) {
    let start = encode_archive_key(element_hash, 0);
    let end = encode_archive_key(element_hash, time.saturating_add(1));
    (start, end)
}

/// Create range bounds for archive queries in a time range.
/// Returns (start_inclusive, end_exclusive).
pub fn archive_range(
    element_hash: &ReferenceHash,
    from_time: u64,
    to_time: u64,
) -> ([u8; 24], [u8; 24]) {
    let start = encode_archive_key(element_hash, from_time);
    let end = encode_archive_key(element_hash, to_time.saturating_add(1));
    (start, end)
}

// =============================================================================
// Future Queue Keys
// =============================================================================

/// Encode future queue key: due_time(u64 BE) + hash64(future_ref)
/// Total: 8 + 8 = 16 bytes
pub fn encode_fqueue_key(due_time: u64, hash: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&due_time.to_be_bytes());
    key[8..16].copy_from_slice(&hash.to_be_bytes());
    key
}

/// Decode due_time from a future queue key
pub fn decode_fqueue_due_time(key: &[u8; 16]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&key[0..8]);
    u64::from_be_bytes(buf)
}

/// Encode future index key: position_in_query(u32 BE) + group_signature(u64 BE) + hash64
/// Total: 4 + 8 + 8 = 20 bytes
pub fn encode_findex_key(position_in_query: u32, group_signature: u64, hash: u64) -> [u8; 20] {
    let mut key = [0u8; 20];
    key[0..4].copy_from_slice(&position_in_query.to_be_bytes());
    key[4..12].copy_from_slice(&group_signature.to_be_bytes());
    key[12..20].copy_from_slice(&hash.to_be_bytes());
    key
}

/// Encode future index prefix for range scans: position(u32 BE) + group_sig(u64 BE)
/// Total: 4 + 8 = 12 bytes
pub fn encode_findex_prefix(position_in_query: u32, group_signature: u64) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[0..4].copy_from_slice(&position_in_query.to_be_bytes());
    key[4..12].copy_from_slice(&group_signature.to_be_bytes());
    key
}

/// Create range bounds for future index prefix scan.
/// Returns (start_inclusive, end_exclusive).
pub fn findex_prefix_range(prefix: &[u8; 12]) -> ([u8; 20], [u8; 20]) {
    let mut start = [0u8; 20];
    start[0..12].copy_from_slice(prefix);

    let mut end = [0u8; 20];
    end[0..12].copy_from_slice(prefix);
    increment_bytes(&mut end[0..12]);

    (start, end)
}

// =============================================================================
// Sorted Set Keys (for LazySortedSetStore)
// =============================================================================

/// Encode sorted set key: set_id(u64 BE) + sortable_f64(12 bytes)
/// Total: 8 + 12 = 20 bytes
pub fn encode_sorted_set_key(set_id: u64, value: f64) -> [u8; 20] {
    let mut key = [0u8; 20];
    key[0..8].copy_from_slice(&set_id.to_be_bytes());
    key[8..20].copy_from_slice(&encode_sortable_f64(value));
    key
}

/// Encode sorted set prefix for range scans: set_id(u64 BE)
pub fn encode_sorted_set_prefix(set_id: u64) -> [u8; 8] {
    set_id.to_be_bytes()
}

/// Create range bounds for sorted set queries starting at a value.
/// Returns (start_inclusive, end_exclusive).
pub fn sorted_set_range_from(set_id: u64, start_value: Option<f64>) -> ([u8; 20], [u8; 20]) {
    let start = match start_value {
        Some(v) => {
            // Start just after the given value
            let mut key = encode_sorted_set_key(set_id, v);
            increment_bytes(&mut key[8..20]);
            key
        }
        None => {
            // Start from the beginning of the set
            let mut key = [0u8; 20];
            key[0..8].copy_from_slice(&set_id.to_be_bytes());
            key
        }
    };

    // End at the next set_id
    let mut end = [0u8; 20];
    end[0..8].copy_from_slice(&(set_id.saturating_add(1)).to_be_bytes());

    (start, end)
}

/// Encode f64 to a sortable 12-byte representation that preserves numeric order.
/// This encoding ensures that sorted iteration over keys returns values in ascending order.
fn encode_sortable_f64(f: f64) -> [u8; 12] {
    let mut key = [0u8; 12];

    // Handle the integer part with offset to make negative numbers sort correctly
    let int_part = f.trunc() as i64;
    let offset_int = (int_part as i128 + i64::MAX as i128) as u64;
    key[0..8].copy_from_slice(&offset_int.to_be_bytes());

    // Handle the fractional part (always positive, scaled to u32)
    let frac = (f.abs().fract() * 1_000_000_000.0).trunc() as u32;
    key[8..12].copy_from_slice(&frac.to_be_bytes());

    key
}

/// Decode a sortable f64 from its 12-byte representation.
#[allow(dead_code)]
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

// =============================================================================
// Utility Functions
// =============================================================================

/// Increment a byte slice to create an exclusive upper bound for range queries.
/// Modifies the slice in-place.
fn increment_bytes(bytes: &mut [u8]) {
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 0xFF {
            bytes[i] += 1;
            return;
        }
        bytes[i] = 0;
    }
    // If we overflow, the slice becomes all zeros - this is acceptable
    // as it means we've reached the maximum possible value
}

/// Hash a FutureElementRef for use in queue/index keys.
pub fn hash_future_ref(
    position_in_query: u32,
    group_signature: u64,
    element_ref: &ElementReference,
) -> u64 {
    use std::hash::Hasher;

    let mut hasher = hashers::fnv::FNV1aHasher64::default();
    position_in_query.hash(&mut hasher);
    group_signature.hash(&mut hasher);
    element_ref.source_id.hash(&mut hasher);
    element_ref.element_id.hash(&mut hasher);
    hasher.finish()
}

// =============================================================================
// Partial Join Index Keys
// =============================================================================

/// Encode partial join prefix: hash128(join_label + field_value + label + property)
/// Total: 16 bytes
pub fn encode_partial_join_prefix(
    join_label: &str,
    field_value: &StoredValue,
    label: &str,
    property: &str,
) -> [u8; 16] {
    use std::hash::Hasher;

    let mut hash = SpookyHasher::default();
    join_label.hash(&mut hash);
    field_value.hash(&mut hash);
    label.hash(&mut hash);
    property.hash(&mut hash);

    let result = hash.finish128();
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&result.0.to_be_bytes());
    key[8..16].copy_from_slice(&result.1.to_be_bytes());
    key
}

/// Encode full partial join key: prefix + len(source_id) + source_id + element_id
pub fn encode_partial_join_key(
    prefix: &[u8; 16],
    element_ref: &StoredElementReference,
) -> Vec<u8> {
    let source_id = element_ref.source_id.as_bytes();
    let source_id_size = source_id.len() as u8;
    let mut data = Vec::with_capacity(64);
    data.extend_from_slice(prefix);
    data.push(source_id_size);
    data.extend_from_slice(source_id);
    data.extend_from_slice(element_ref.element_id.as_bytes());
    data
}

/// Decode element reference from a partial join key
pub fn decode_partial_join_key(key: &[u8]) -> Result<StoredElementReference, IndexError> {
    if key.len() < 17 {
        return Err(IndexError::CorruptedData);
    }
    let source_id_size = key[16] as usize;
    if key.len() < 17 + source_id_size {
        return Err(IndexError::CorruptedData);
    }
    let source_id = String::from_utf8(key[17..(17 + source_id_size)].to_vec())
        .map_err(|_| IndexError::CorruptedData)?;
    let element_id = String::from_utf8(key[(17 + source_id_size)..].to_vec())
        .map_err(|_| IndexError::CorruptedData)?;
    Ok(StoredElementReference {
        source_id,
        element_id,
    })
}

/// Create range bounds for partial join prefix scan.
/// Returns (start_inclusive, end_exclusive).
pub fn partial_join_prefix_range(prefix: &[u8; 16]) -> (Vec<u8>, Vec<u8>) {
    let start = prefix.to_vec();
    let mut end = prefix.to_vec();
    increment_bytes(&mut end);
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sortable_f64_encoding_order() {
        let values = [-100.5, -1.0, 0.0, 0.5, 1.0, 100.5];
        let encoded: Vec<[u8; 12]> = values.iter().map(|v| encode_sortable_f64(*v)).collect();

        // Verify that encoded values maintain sort order
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "Encoding should preserve order: {:?} < {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_sortable_f64_roundtrip() {
        let values = [-100.5, -1.0, 0.0, 0.5, 1.0, 100.5, 123.456789];
        for v in values {
            let encoded = encode_sortable_f64(v);
            let decoded = decode_sortable_f64(&encoded);
            assert!(
                (v - decoded).abs() < 0.0000001,
                "Roundtrip failed for {v}: got {decoded}"
            );
        }
    }

    #[test]
    fn test_increment_bytes() {
        let mut bytes = [0x00, 0x00, 0xFF];
        increment_bytes(&mut bytes);
        assert_eq!(bytes, [0x00, 0x01, 0x00]);

        let mut bytes = [0xFF, 0xFF, 0xFF];
        increment_bytes(&mut bytes);
        assert_eq!(bytes, [0x00, 0x00, 0x00]); // Overflow wraps

        let mut bytes = [0x00, 0x00, 0x00];
        increment_bytes(&mut bytes);
        assert_eq!(bytes, [0x00, 0x00, 0x01]);
    }
}
