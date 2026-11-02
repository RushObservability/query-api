//! Object-store backend for the durable ingest buffer.
//!
//! The design is deliberately simple:
//! one object per spilled batch under a prefix, keyed by a sortable
//! `{unix_millis:013}-{seq:08}.batch` so listing yields oldest-first. Drain =
//! list → get oldest → (caller inserts) → delete. No shared manifest, no CAS:
//! at-least-once is the target (ClickHouse async inserts aren't transactional),
//! and a single drain worker removes the only concurrent-duplicate window.
//!
//! Object body = `{table}\n{payload}` (table names have no newline; the payload
//! is the same serde_json batch bytes the disk spool stores).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::spool::SpoolFull;

const INDEX_BATCH_SIZE: usize = 4096;
const INDEX_REFRESH_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Default)]
struct BacklogIndex {
    oldest: BTreeMap<OsPath, u64>,
    complete: bool,
    refreshed_at: Option<Instant>,
}

impl BacklogIndex {
    fn remember(&mut self, key: OsPath, size: u64) {
        if self.complete
            || self
                .oldest
                .last_key_value()
                .is_some_and(|(last, _)| &key < last)
        {
            self.oldest.insert(key, size);
            if self.oldest.len() > INDEX_BATCH_SIZE {
                self.oldest.pop_last();
                self.complete = false;
            }
        }
    }
}

struct AppendReservation<'a> {
    bytes: &'a AtomicU64,
    size: u64,
    committed: bool,
}

impl Drop for AppendReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.bytes.fetch_sub(self.size, Ordering::Relaxed);
        }
    }
}

pub struct ObjectStoreSpool {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    max_bytes: u64,
    bytes: AtomicU64,
    count: AtomicUsize,
    seq: AtomicU64,
    committed: AtomicU64,
    index: Mutex<BacklogIndex>,
    transactions: RwLock<()>,
    reconciliations: AtomicU64,
    #[cfg(test)]
    metadata_visited: AtomicUsize,
}

impl ObjectStoreSpool {
    /// Build an S3/MinIO-backed buffer and seed counters from any objects left
    /// over from a previous run (durable across restarts/pods).
    pub async fn open_s3(
        endpoint: &str,
        bucket: &str,
        prefix: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        max_bytes: u64,
    ) -> anyhow::Result<Self> {
        let mut b = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key);
        if !endpoint.is_empty() {
            b = b
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }
        // Path-style addressing for MinIO/S3-compatibles.
        b = b.with_virtual_hosted_style_request(false);
        let store: Arc<dyn ObjectStore> = Arc::new(b.build()?);
        Self::open(store, prefix, max_bytes).await
    }

    /// Construct over any `ObjectStore` (S3/MinIO in prod, `InMemory` in tests).
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        max_bytes: u64,
    ) -> anyhow::Result<Self> {
        let prefix = if prefix.is_empty() {
            "ingest/".to_string()
        } else if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        };
        let s = ObjectStoreSpool {
            store,
            prefix,
            max_bytes,
            bytes: AtomicU64::new(0),
            count: AtomicUsize::new(0),
            seq: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            index: Mutex::new(BacklogIndex::default()),
            transactions: RwLock::new(()),
            reconciliations: AtomicU64::new(0),
            #[cfg(test)]
            metadata_visited: AtomicUsize::new(0),
        };
        // Seed counters from existing objects so the cap + metrics survive restarts.
        s.refresh_counts().await?;
        Ok(s)
    }

    /// Reconcile local gauges with the shared object-store prefix.
    ///
    /// Appends and commits update local atomics immediately, but another
    /// replica can append or drain objects without touching this process. A
    /// periodic reconciliation keeps capacity/backlog metrics useful on every
    /// replica and corrects counters after cross-pod recovery.
    pub async fn refresh_counts(&self) -> anyhow::Result<()> {
        let _transaction = self.transactions.write().await;
        let mut index = self.index.lock().await;
        self.scan(&mut index, true).await
    }

    /// Count in one streaming pass and optionally retain the oldest bounded
    /// batch. ObjectStore does not guarantee listing order.
    async fn scan(&self, index: &mut BacklogIndex, rebuild_index: bool) -> anyhow::Result<()> {
        let pfx = OsPath::from(self.prefix.trim_end_matches('/'));
        let mut oldest = BTreeMap::new();
        let mut count = 0_usize;
        let mut total = 0_u64;
        let mut stream = self.store.list(Some(&pfx));
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            #[cfg(test)]
            self.metadata_visited.fetch_add(1, Ordering::Relaxed);
            count += 1;
            total = total.saturating_add(meta.size);
            if rebuild_index {
                oldest.insert(meta.location, meta.size);
                if oldest.len() > INDEX_BATCH_SIZE {
                    oldest.pop_last();
                }
            }
        }
        if rebuild_index {
            index.oldest = oldest;
            index.complete = count <= INDEX_BATCH_SIZE;
            index.refreshed_at = Some(Instant::now());
        } else if total != self.total_bytes() || count != self.segment_count() {
            // Another replica changed the backlog; rebuild before selecting
            // the next batch, without retaining the entire listing here.
            index.refreshed_at = None;
        }
        self.bytes.store(total, Ordering::Relaxed);
        self.count.store(count, Ordering::Relaxed);
        self.reconciliations.fetch_add(1, Ordering::Release);
        Ok(())
    }

    async fn ensure_index(&self, index: &mut BacklogIndex) -> anyhow::Result<()> {
        if index
            .refreshed_at
            .is_none_or(|at| at.elapsed() >= INDEX_REFRESH_INTERVAL)
            || index.oldest.is_empty() && self.segment_count() > 0
        {
            self.scan(index, true).await?;
        }
        Ok(())
    }

    /// Spill one batch as a new object. Returns `SpoolFull` if over the cap or
    /// the put fails (so the caller applies 429 backpressure rather than dropping).
    pub async fn append(&self, table: &str, payload: &[u8]) -> Result<(), SpoolFull> {
        // This is already the failure path, so reconcile before enforcing the
        // cap. It accounts for objects written or drained by another replica.
        // Two concurrent producers can still race; exact global reservations
        // require a distributed quota/CAS protocol and are intentionally out
        // of scope for this single-writer drain design.
        let observed = self.reconciliations.load(Ordering::Acquire);
        let transaction = self.transactions.write().await;
        let mut index = self.index.lock().await;
        // Preserve fresh cross-replica capacity accounting. This count-only
        // pass does not collect/sort the backlog or rebuild the drain index.
        // Overlapping callers can share the reconciliation completed while
        // they waited. A subsequent serial append still gets fresh counts.
        if observed == self.reconciliations.load(Ordering::Acquire)
            && self.scan(&mut index, false).await.is_err()
        {
            return Err(SpoolFull);
        }
        drop(index);
        drop(transaction);
        // Concurrent PUTs share a read lock. A reconciliation takes the write
        // lock, so it cannot count a PUT before its local reservation commits.
        let transaction = self.transactions.read().await;
        let rec_len = (table.len() + 1 + payload.len()) as u64;
        if self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |bytes| {
                bytes
                    .checked_add(rec_len)
                    .filter(|total| *total <= self.max_bytes)
            })
            .is_err()
        {
            return Err(SpoolFull);
        }
        let mut reservation = AppendReservation {
            bytes: &self.bytes,
            size: rec_len,
            committed: false,
        };
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        // The UUID prevents two replicas that start with the same local seq and
        // spill in the same millisecond from overwriting one another.
        let key = OsPath::from(format!(
            "{}{:013}-{:08}-{}.batch",
            self.prefix,
            millis,
            seq,
            Uuid::new_v4()
        ));

        let mut body = Vec::with_capacity(rec_len as usize);
        body.extend_from_slice(table.as_bytes());
        body.push(b'\n');
        body.extend_from_slice(payload);

        match self
            .store
            .put(&key, PutPayload::from_bytes(Bytes::from(body)))
            .await
        {
            Ok(_) => {
                self.count.fetch_add(1, Ordering::Relaxed);
                reservation.committed = true;
                drop(reservation);
                drop(transaction);
                let mut index = self.index.lock().await;
                index.remember(key, rec_len);
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "object-store buffer: put failed — applying backpressure");
                Err(SpoolFull)
            }
        }
    }

    /// Oldest spilled batch + its object key (handle), or None if empty.
    /// Corrupt objects are deleted and skipped.
    pub async fn next_batch(&self) -> Option<(OsPath, u64, Vec<(String, Vec<u8>)>)> {
        loop {
            let selected = {
                let _transaction = self.transactions.write().await;
                let mut index = self.index.lock().await;
                match self.ensure_index(&mut index).await {
                    Ok(()) => index
                        .oldest
                        .first_key_value()
                        .map(|(key, size)| (key.clone(), *size)),
                    Err(e) => {
                        tracing::warn!(error = %e, "object-store buffer: list failed");
                        return None;
                    }
                }
            };
            let (key, size) = selected?;
            let data = match self.store.get(&key).await {
                Ok(r) => match r.bytes().await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(error = %e, key = %key, "buffer: get bytes failed");
                        return None;
                    }
                },
                Err(object_store::Error::NotFound { .. }) => {
                    let _transaction = self.transactions.read().await;
                    let mut index = self.index.lock().await;
                    self.forget(&mut index, &key, size);
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, key = %key, "buffer: get failed");
                    return None;
                }
            };
            match split_record(&data) {
                Some((table, payload)) => return Some((key, size, vec![(table, payload)])),
                None => {
                    tracing::error!(key = %key, "object-store buffer: corrupt object — discarding");
                    if !self.delete_batch(&key, size).await {
                        return None;
                    }
                    continue; // try the next object
                }
            }
        }
    }

    /// Delete a successfully-drained object.
    pub async fn commit(&self, key: &OsPath, size: u64) {
        if self.delete_batch(key, size).await {
            self.committed.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn delete_batch(&self, key: &OsPath, size: u64) -> bool {
        let _transaction = self.transactions.read().await;
        let mut index = self.index.lock().await;
        if let Err(e) = self.store.delete(key).await {
            tracing::warn!(error = %e, key = %key, "object-store buffer: delete failed");
            return false;
        }
        self.forget(&mut index, key, size);
        true
    }

    fn forget(&self, index: &mut BacklogIndex, key: &OsPath, size: u64) {
        if index.oldest.remove(key).is_some() {
            self.bytes
                .fetch_sub(size.min(self.total_bytes()), Ordering::Relaxed);
            self.count
                .fetch_sub(self.segment_count().min(1), Ordering::Relaxed);
        } else {
            index.refreshed_at = None;
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
    pub fn segment_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }
    pub fn committed_total(&self) -> u64 {
        self.committed.load(Ordering::Relaxed)
    }

    /// Age (seconds) of the oldest pending object, parsed from its `{millis}-{seq}.batch` key.
    pub async fn oldest_age_secs(&self) -> Option<u64> {
        let _transaction = self.transactions.write().await;
        let mut index = self.index.lock().await;
        self.ensure_index(&mut index).await.ok()?;
        let (key, _) = index.oldest.first_key_value()?;
        let fname = key.as_ref().rsplit('/').next().unwrap_or("");
        let ms = fname
            .split('-')
            .next()
            .and_then(|s| s.parse::<u64>().ok())?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Some(now.saturating_sub(ms) / 1000)
    }
}

/// Split an object body `{table}\n{payload}` into (table, payload).
fn split_record(data: &[u8]) -> Option<(String, Vec<u8>)> {
    let nl = data.iter().position(|&b| b == b'\n')?;
    let table = std::str::from_utf8(&data[..nl]).ok()?.to_string();
    Some((table, data[nl + 1..].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    async fn seed(store: &Arc<dyn ObjectStore>, count: usize) {
        for i in (0..count).rev() {
            store
                .put(
                    &OsPath::from(format!("ingest/{i:013}-00000000.batch")),
                    Bytes::from_static(b"logs\nx").into(),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn drain_batches_keep_a_bounded_oldest_index_and_reuse_age_lookups() {
        let st = store();
        let count = INDEX_BATCH_SIZE * 2 + 17;
        seed(&st, count).await;
        let spool = ObjectStoreSpool::open(st, "ingest", u64::MAX)
            .await
            .unwrap();
        for i in 0..count {
            assert!(spool.index.lock().await.oldest.len() <= INDEX_BATCH_SIZE);
            spool.oldest_age_secs().await.unwrap();
            let (key, size, _) = spool.next_batch().await.unwrap();
            assert_eq!(key, OsPath::from(format!("ingest/{i:013}-00000000.batch")));
            spool.commit(&key, size).await;
        }
        assert!(spool.next_batch().await.is_none());
        assert_eq!(spool.total_bytes(), 0);
        assert_eq!(spool.segment_count(), 0);
        assert_eq!(spool.reconciliations.load(Ordering::Relaxed), 3);
        assert!(spool.metadata_visited.load(Ordering::Relaxed) < count * 3);
    }

    #[tokio::test]
    async fn overlapping_appends_share_counts_but_serial_appends_reconcile_again() {
        let spool = ObjectStoreSpool::open(store(), "ingest", 30).await.unwrap();
        let held = spool.index.lock().await;
        let mut left = Box::pin(spool.append("m", &[0; 10]));
        let mut right = Box::pin(spool.append("m", &[0; 10]));
        assert!(futures_util::poll!(left.as_mut()).is_pending());
        assert!(futures_util::poll!(right.as_mut()).is_pending());
        drop(held);
        let (left, right) = tokio::join!(left, right);
        left.unwrap();
        right.unwrap();
        assert_eq!(spool.reconciliations.load(Ordering::Relaxed), 2);
        assert_eq!(spool.total_bytes(), 24);
        assert!(spool.append("m", &[0; 10]).await.is_err());
        assert_eq!(spool.reconciliations.load(Ordering::Relaxed), 3);
        assert_eq!(spool.total_bytes(), 24);
    }

    #[tokio::test]
    async fn stale_index_skips_external_deletes_and_corrupt_records() {
        let st = store();
        seed(&st, 3).await;
        let spool = ObjectStoreSpool::open(st.clone(), "ingest", 1_000_000)
            .await
            .unwrap();
        st.delete(&OsPath::from("ingest/0000000000000-00000000.batch"))
            .await
            .unwrap();
        st.put(
            &OsPath::from("ingest/0000000000001-00000000.batch"),
            Bytes::from_static(b"corrupt").into(),
        )
        .await
        .unwrap();
        let (key, size, rows) = spool.next_batch().await.unwrap();
        assert_eq!(key, OsPath::from("ingest/0000000000002-00000000.batch"));
        assert_eq!(rows[0].1, b"x");
        spool.commit(&key, size).await;
        spool.refresh_counts().await.unwrap();
        assert_eq!(spool.segment_count(), 0);
        assert_eq!(spool.total_bytes(), 0);
    }

    #[tokio::test]
    #[ignore = "manual object-store recovery benchmark"]
    async fn benchmark_backlog_recovery() {
        for count in [1_000, 10_000, 100_000] {
            let st = store();
            seed(&st, count).await;
            let start = Instant::now();
            let spool = ObjectStoreSpool::open(st, "ingest", u64::MAX)
                .await
                .unwrap();
            while let Some((key, size, _)) = spool.next_batch().await {
                spool.commit(&key, size).await;
            }
            assert_eq!(spool.segment_count(), 0);
            eprintln!(
                "objects={count} prefix_scans={} metadata_visits={} index_limit={INDEX_BATCH_SIZE} elapsed_ms={}",
                spool.reconciliations.load(Ordering::Relaxed),
                spool.metadata_visited.load(Ordering::Relaxed),
                start.elapsed().as_millis()
            );
        }
    }

    #[tokio::test]
    async fn append_drain_commit_roundtrip() {
        let s = ObjectStoreSpool::open(store(), "ingest", 1_000_000)
            .await
            .unwrap();
        s.append("logs", b"alpha").await.unwrap();
        s.append("spans", b"beta").await.unwrap();
        assert_eq!(s.segment_count(), 2);

        // oldest first
        let (k1, size1, recs1) = s.next_batch().await.unwrap();
        assert_eq!(recs1, vec![("logs".to_string(), b"alpha".to_vec())]);
        s.commit(&k1, size1).await;

        let (k2, size2, recs2) = s.next_batch().await.unwrap();
        assert_eq!(recs2, vec![("spans".to_string(), b"beta".to_vec())]);
        s.commit(&k2, size2).await;

        assert!(s.next_batch().await.is_none());
        assert_eq!(s.total_bytes(), 0);
    }

    #[tokio::test]
    async fn cap_triggers_spoolfull() {
        let s = ObjectStoreSpool::open(store(), "ingest", 30).await.unwrap();
        // rec_len = table+1+payload; "m"+1+10 = 12
        s.append("m", &[0u8; 10]).await.unwrap();
        s.append("m", &[0u8; 10]).await.unwrap(); // 24
        let full = s.append("m", &[0u8; 10]).await; // 36 > 30
        assert!(full.is_err());
    }

    #[tokio::test]
    async fn open_seeds_from_existing() {
        let st = store();
        {
            let s = ObjectStoreSpool::open(st.clone(), "ingest", 1_000_000)
                .await
                .unwrap();
            s.append("logs", b"x").await.unwrap();
        }
        // Re-open over the same store → must see the leftover object.
        let s2 = ObjectStoreSpool::open(st, "ingest", 1_000_000)
            .await
            .unwrap();
        assert_eq!(s2.segment_count(), 1);
        assert!(s2.next_batch().await.is_some());
    }

    #[tokio::test]
    async fn uncommitted_batch_remains_available_for_retry() {
        let s = ObjectStoreSpool::open(store(), "ingest", 1_000_000)
            .await
            .unwrap();
        s.append("logs", b"retry-me").await.unwrap();

        let (_, _, first) = s.next_batch().await.unwrap();
        // A failed ClickHouse insert must not delete the object. The replayer
        // can safely read the same batch again on its next pass (at-least-once).
        let (_, _, retry) = s.next_batch().await.unwrap();
        assert_eq!(first, retry);
        assert_eq!(s.segment_count(), 1);
    }

    #[tokio::test]
    async fn shared_reopen_preserves_order_and_reconciles_external_drain() {
        let st = store();
        let writer = ObjectStoreSpool::open(st.clone(), "ingest", 1_000_000)
            .await
            .unwrap();
        writer.append("logs", b"first").await.unwrap();
        writer.append("logs", b"second").await.unwrap();
        let expected_bytes = writer.total_bytes();

        let reader = ObjectStoreSpool::open(st, "ingest", 1_000_000)
            .await
            .unwrap();
        assert_eq!(reader.total_bytes(), expected_bytes);
        // A different process can commit a key; refresh makes this reader's
        // gauges converge instead of reporting a phantom backlog forever.
        let (key, size, records) = reader.next_batch().await.unwrap();
        assert_eq!(records[0].1, b"first");
        writer.commit(&key, size).await;
        reader.refresh_counts().await.unwrap();
        assert_eq!(reader.segment_count(), 1);
        assert_eq!(reader.total_bytes(), expected_bytes - size);

        let (key, size, records) = reader.next_batch().await.unwrap();
        assert_eq!(records[0].1, b"second");
        reader.commit(&key, size).await;
        reader.refresh_counts().await.unwrap();
        assert_eq!(reader.segment_count(), 0);
        assert_eq!(reader.total_bytes(), 0);
    }

    #[tokio::test]
    async fn concurrent_replicas_get_unique_keys_for_same_millisecond() {
        let st = store();
        let left = ObjectStoreSpool::open(st.clone(), "ingest", 1_000_000)
            .await
            .unwrap();
        let right = ObjectStoreSpool::open(st, "ingest", 1_000_000)
            .await
            .unwrap();
        let (left_result, right_result) =
            tokio::join!(left.append("logs", b"left"), right.append("logs", b"right"));
        left_result.unwrap();
        right_result.unwrap();

        left.refresh_counts().await.unwrap();
        let index = left.index.lock().await;
        assert_eq!(index.oldest.len(), 2);
    }
}
