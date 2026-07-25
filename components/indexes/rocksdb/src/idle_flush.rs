// Copyright 2026 The Drasi Authors.
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

//! Idle-flush sweeper for query index DBs sharing a WriteBufferManager.
//!
//! RocksDB's WriteBufferManager only initiates flushes on a DB that is being
//! written to. A query that goes quiet (bootstrap finished, source idle)
//! leaves its last, below-threshold memtables resident, pinned against the
//! shared write budget indefinitely; enough quiet queries starve the active
//! ones into per-write micro-flush storms (measured: thousands of flushes and
//! write stalls on the starved DB). Until the engine can pressure idle DBs,
//! this sweeper periodically flushes memtables of DBs that have not committed
//! since the previous sweep, returning their budget to the pool. Flushing
//! idle data is safe: it is already durable in the WAL, and an early flush
//! also shortens WAL replay on restart.

use std::sync::{Arc, Mutex, Weak};

use rocksdb::{FlushOptions, OptimisticTransactionDB, WriteBufferManager};

use crate::RocksDbSessionState;

/// Pressure-check cadence. Each tick the sweeper looks at the shared write
/// pool; it only sweeps when the pool is filling (or on the slower baseline
/// below), so frequent ticks cost near nothing. Not configurable by design.
pub(crate) const TICK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Baseline sweep every this many ticks even without pool pressure, so idle
/// residue also drains for durability hygiene (shorter WAL replay).
const BASELINE_SWEEP_TICKS: u32 = 6;

/// Sweep immediately when the shared pool passes this fill fraction (percent).
const PRESSURE_THRESHOLD_PERCENT: usize = 60;

/// Ignore CFs holding less than this. An untouched memtable reports a few
/// KiB of arena, and burst-lull write rhythms would otherwise be swept into
/// many small SSTs between bursts; below 1 MiB the reclaimable budget is not
/// worth the extra compaction input.
const FLUSH_THRESHOLD_BYTES: u64 = 1024 * 1024;

/// Every CF a unified query DB can contain (see `open_unified_db`).
/// `cf_handle` returns None for absent ones (e.g. archive when disabled).
const ALL_CFS: &[&str] = &[
    "default",
    "elements",
    "slots",
    "inbound",
    "outbound",
    "partial",
    "archive",
    "values",
    "sorted-sets",
    "metadata",
    "fqueue",
    "findex",
    "stream_state",
    "outbox",
    "live_results",
];

struct Entry {
    query_id: String,
    db: Weak<OptimisticTransactionDB>,
    session: Weak<RocksDbSessionState>,
    last_seq: u64,
}

/// Registry of DBs subject to idle flushing. Owned by the provider; the
/// background task holds a clone and exits when the provider drops it.
pub(crate) struct IdleFlushRegistry {
    entries: Mutex<Vec<Entry>>,
    write_buffer_manager: WriteBufferManager,
}

/// Aggregates from one sweep pass: where the memtable budget currently lives,
/// and what the sweeper reclaimed. The three "held but untouchable" buckets
/// (active, exempt, in-session) are exactly the states that can starve the
/// shared pool while the sweeper runs — the pressure ledger logs them so a
/// saturated pool is attributable from a single log line.
#[derive(Default)]
pub(crate) struct SweepStats {
    /// DBs whose over-threshold CFs were submitted for flushing.
    pub(crate) flushed: usize,
    /// Pre-flush bytes in the CFs submitted for flushing.
    pub(crate) flushed_bytes: u64,
    /// DBs skipped because writes advanced their sequence since the last pass.
    pub(crate) active: usize,
    /// Memtable bytes held by those active DBs.
    pub(crate) active_bytes: u64,
    /// Idle memtable bytes left in place because each CF was at or under
    /// `FLUSH_THRESHOLD_BYTES`.
    pub(crate) exempt_bytes: u64,
    /// Idle DBs skipped because a session was open.
    pub(crate) in_session: usize,
    /// Memtable bytes held by those in-session DBs.
    pub(crate) in_session_bytes: u64,
}

/// Sweep when the pool is past the pressure threshold, or on the baseline tick.
fn should_sweep(usage: usize, capacity: usize, tick: u32) -> bool {
    usage * 100 > capacity * PRESSURE_THRESHOLD_PERCENT || tick.is_multiple_of(BASELINE_SWEEP_TICKS)
}

impl IdleFlushRegistry {
    pub(crate) fn new(write_buffer_manager: WriteBufferManager) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            write_buffer_manager,
        }
    }

    pub(crate) fn register(
        &self,
        query_id: &str,
        db: &Arc<OptimisticTransactionDB>,
        session: &Arc<RocksDbSessionState>,
    ) {
        let mut entries = self.entries.lock().expect("idle-flush registry poisoned");
        entries.retain(|e| e.query_id != query_id);
        entries.push(Entry {
            query_id: query_id.to_string(),
            db: Arc::downgrade(db),
            session: Arc::downgrade(session),
            last_seq: db.latest_sequence_number(),
        });
    }

    /// One sweep pass. Returns where the memtable budget currently lives and
    /// what the sweeper did about it; consumed by the pressure ledger in
    /// [`IdleFlushRegistry::run`] and by tests.
    pub(crate) fn sweep_once(&self) -> SweepStats {
        let mut stats = SweepStats::default();
        let mut entries = self.entries.lock().expect("idle-flush registry poisoned");
        entries.retain_mut(|entry| {
            let Some(db) = entry.db.upgrade() else {
                return false; // query deleted; drop the entry
            };
            // Per-CF memtable sizes (cheap in-memory property reads), taken up
            // front so the ledger can attribute budget to holders the sweeper
            // is not allowed to touch (active or in-session DBs) as well.
            let sized_cfs: Vec<_> = ALL_CFS
                .iter()
                .filter_map(|name| {
                    let handle = db.cf_handle(name)?;
                    let size = db
                        .property_int_value_cf(&handle, "rocksdb.cur-size-all-mem-tables")
                        .ok()
                        .flatten()
                        .unwrap_or(0);
                    Some((handle, size))
                })
                .collect();
            let total: u64 = sized_cfs.iter().map(|(_, size)| size).sum();
            let seq = db.latest_sequence_number();
            let idle = seq == entry.last_seq;
            entry.last_seq = seq;
            if !idle {
                stats.active += 1;
                stats.active_bytes += total;
                return true;
            }
            // Skip if a session is open (a commit may be imminent; flushing
            // mid-session is safe but pointless churn).
            if let Some(session) = entry.session.upgrade() {
                if session.is_session_active() {
                    stats.in_session += 1;
                    stats.in_session_bytes += total;
                    return true;
                }
            }
            let mut flushable = Vec::new();
            for (handle, size) in sized_cfs {
                if size > FLUSH_THRESHOLD_BYTES {
                    stats.flushed_bytes += size;
                    flushable.push(handle);
                } else {
                    stats.exempt_bytes += size;
                }
            }
            if !flushable.is_empty() {
                let mut opts = FlushOptions::default();
                opts.set_wait(false);
                let refs: Vec<&_> = flushable.iter().collect();
                match db.flush_cfs_opt(&refs, &opts) {
                    Ok(()) => {
                        stats.flushed += 1;
                        log::debug!(
                            "idle-flush: flushed {} CF(s) of quiet query '{}'",
                            refs.len(),
                            entry.query_id
                        );
                    }
                    Err(e) => {
                        log::warn!("idle-flush: flush failed for query '{}': {e}", entry.query_id)
                    }
                }
            }
            true
        });
        stats
    }

    /// Run sweeps until the provider (the only other holder) drops the registry.
    pub(crate) async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(TICK_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut tick: u32 = 0;
        let mut pressured_sweeps: u32 = 0;
        loop {
            interval.tick().await;
            if Arc::strong_count(&self) == 1 {
                return; // provider dropped
            }
            tick = tick.wrapping_add(1);
            let usage = self.write_buffer_manager.get_usage();
            let capacity = self.write_buffer_manager.get_buffer_size();
            if should_sweep(usage, capacity, tick) {
                let stats = self.sweep_once();
                if usage * 100 > capacity * PRESSURE_THRESHOLD_PERCENT {
                    // Ledger: on the first pressured sweep and then once per
                    // BASELINE_SWEEP_TICKS while pressure persists, log where
                    // the pool's memory is being held so sustained saturation
                    // is attributable without per-tick spam.
                    if pressured_sweeps.is_multiple_of(BASELINE_SWEEP_TICKS) {
                        const MIB: u64 = 1024 * 1024;
                        log::info!(
                            "idle-flush ledger: pool {}/{} MiB; flushed {} DB(s) ({} MiB); \
                             {} active DB(s) hold {} MiB; sub-threshold leftovers {} MiB; \
                             {} in-session DB(s) hold {} MiB",
                            usage as u64 / MIB,
                            capacity as u64 / MIB,
                            stats.flushed,
                            stats.flushed_bytes / MIB,
                            stats.active,
                            stats.active_bytes / MIB,
                            stats.exempt_bytes / MIB,
                            stats.in_session,
                            stats.in_session_bytes / MIB,
                        );
                    }
                    pressured_sweeps = pressured_sweeps.wrapping_add(1);
                } else {
                    pressured_sweeps = 0;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::element_index::RocksIndexOptions;
    use crate::tuning::RocksDbTuning;
    use crate::open_unified_db;
    use tempfile::TempDir;

    fn memtable_bytes(db: &OptimisticTransactionDB, cf: &str) -> u64 {
        let h = db.cf_handle(cf).unwrap();
        db.property_int_value_cf(&h, "rocksdb.cur-size-all-mem-tables")
            .ok()
            .flatten()
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn sweeps_idle_db_and_skips_active_ones() {
        let dir = TempDir::new().unwrap();
        let tuning = RocksDbTuning::default();
        let options = RocksIndexOptions {
            archive_enabled: false,
            direct_io: false,
        };
        let db = open_unified_db(dir.path().to_str().unwrap(), "q", &options, &tuning).unwrap();
        let session = Arc::new(RocksDbSessionState::new(db.clone()));

        let registry = IdleFlushRegistry::new(tuning.write_buffer_manager.clone());
        registry.register("q", &db, &session);

        // Fill the elements memtable well past the flush threshold.
        let cf = db.cf_handle("elements").unwrap();
        let payload = vec![7u8; 512];
        for i in 0..4096u32 {
            db.put_cf(&cf, i.to_be_bytes(), &payload).unwrap();
        }
        assert!(memtable_bytes(&db, "elements") > FLUSH_THRESHOLD_BYTES);

        // First sweep: writes happened since registration -> not idle yet.
        assert_eq!(registry.sweep_once().flushed, 0);
        // Second sweep: no writes since -> idle -> flushed.
        assert_eq!(registry.sweep_once().flushed, 1);
        // Flush is async (wait=false); poll until the memtable drains.
        for _ in 0..100 {
            if memtable_bytes(&db, "elements") <= FLUSH_THRESHOLD_BYTES {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(memtable_bytes(&db, "elements") <= FLUSH_THRESHOLD_BYTES);

        // Refill, then hold a session open: the sweeper must skip the DB.
        for i in 0..4096u32 {
            db.put_cf(&cf, i.to_be_bytes(), &payload).unwrap();
        }
        assert_eq!(registry.sweep_once().flushed, 0); // not idle (writes advanced seq)
        session.begin().unwrap();
        assert_eq!(registry.sweep_once().flushed, 0); // idle but session active -> skipped
        session.rollback();
        assert_eq!(registry.sweep_once().flushed, 1); // idle, no session -> flushed
    }

    #[test]
    fn pressure_and_baseline_gating() {
        // Pool under threshold: only the baseline tick sweeps.
        assert!(!should_sweep(50, 100, 1));
        assert!(should_sweep(50, 100, BASELINE_SWEEP_TICKS));
        // Pool over threshold: sweep on any tick.
        assert!(should_sweep(61, 100, 1));
        // Exactly at threshold does not trigger (strict greater-than).
        assert!(!should_sweep(60, 100, 1));
    }

    #[tokio::test]
    async fn dead_dbs_are_pruned() {
        let dir = TempDir::new().unwrap();
        let tuning = RocksDbTuning::default();
        let options = RocksIndexOptions {
            archive_enabled: false,
            direct_io: false,
        };
        let db = open_unified_db(dir.path().to_str().unwrap(), "q", &options, &tuning).unwrap();
        let session = Arc::new(RocksDbSessionState::new(db.clone()));
        let registry = IdleFlushRegistry::new(tuning.write_buffer_manager.clone());
        registry.register("q", &db, &session);
        drop(session);
        drop(db);
        assert_eq!(registry.sweep_once().flushed, 0);
        assert!(registry.entries.lock().unwrap().is_empty());
    }
}
