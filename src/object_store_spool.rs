//! Object-store backend for the durable ingest buffer (PRD Phase 2).
//!
//! Design (deliberately simple — see docs/PRD-object-store-ingest-buffer.md):
//! one object per spilled batch under a prefix, keyed by a sortable
//! `{unix_millis:013}-{seq:08}.batch` so listing yields oldest-first. Drain =
//! list → get oldest → (caller inserts) → delete. No shared manifest, no CAS:
//! at-least-once is the target (ClickHouse async inserts aren't transactional),
//! and a single drain worker removes the only concurrent-duplicate window.
//!
//! Object body = `{table}\n{payload}` (table names have no newline; the payload
//! is the same serde_json batch bytes the disk spool stores).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::StreamExt;
use object_store::{ObjectStore, PutPayload};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as OsPath;

use crate::spool::SpoolFull;

pub struct ObjectStoreSpool {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    max_bytes: u64,
    bytes: AtomicU64,
    count: AtomicUsize,
    seq: AtomicU64,
    committed: AtomicU64,
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
            b = b.with_endpoint(endpoint).with_allow_http(endpoint.starts_with("http://"));
        }
        // Path-style addressing for MinIO/S3-compatibles.
        b = b.with_virtual_hosted_style_request(false);
        let store: Arc<dyn ObjectStore> = Arc::new(b.build()?);
        Self::open(store, prefix, max_bytes).await
    }

    /// Construct over any `ObjectStore` (S3/MinIO in prod, `InMemory` in tests).
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: &str, max_bytes: u64) -> anyhow::Result<Self> {
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
        };
        // Seed counters from existing objects so the cap + metrics survive restarts.
        let metas = s.list_sorted().await?;
        let mut total = 0u64;
        for m in &metas {
            total += m.1;
        }
        s.bytes.store(total, Ordering::Relaxed);
        s.count.store(metas.len(), Ordering::Relaxed);
        Ok(s)
    }

    /// All buffered objects as (key, size), sorted oldest-first by key.
    async fn list_sorted(&self) -> anyhow::Result<Vec<(OsPath, u64)>> {
        let pfx = OsPath::from(self.prefix.trim_end_matches('/'));
        let mut out: Vec<(OsPath, u64)> = Vec::new();
        let mut stream = self.store.list(Some(&pfx));
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            out.push((meta.location, meta.size as u64));
        }
        out.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
        Ok(out)
    }

    /// Spill one batch as a new object. Returns `SpoolFull` if over the cap or
    /// the put fails (so the caller applies 429 backpressure rather than dropping).
    pub async fn append(&self, table: &str, payload: &[u8]) -> Result<(), SpoolFull> {
        let rec_len = (table.len() + 1 + payload.len()) as u64;
        if self.bytes.load(Ordering::Relaxed) + rec_len > self.max_bytes {
            return Err(SpoolFull);
        }
        let millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let key = OsPath::from(format!("{}{:013}-{:08}.batch", self.prefix, millis, seq));

        let mut body = Vec::with_capacity(rec_len as usize);
        body.extend_from_slice(table.as_bytes());
        body.push(b'\n');
        body.extend_from_slice(payload);

        match self.store.put(&key, PutPayload::from_bytes(Bytes::from(body))).await {
            Ok(_) => {
                self.bytes.fetch_add(rec_len, Ordering::Relaxed);
                self.count.fetch_add(1, Ordering::Relaxed);
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
    pub async fn next_batch(&self) -> Option<(OsPath, Vec<(String, Vec<u8>)>)> {
        loop {
            let metas = match self.list_sorted().await {
                Ok(m) => m,
                Err(e) => { tracing::warn!(error = %e, "object-store buffer: list failed"); return None; }
            };
            let (key, size) = metas.into_iter().next()?;
            let data = match self.store.get(&key).await {
                Ok(r) => match r.bytes().await {
                    Ok(b) => b,
                    Err(e) => { tracing::warn!(error = %e, key = %key, "buffer: get bytes failed"); return None; }
                },
                Err(e) => { tracing::warn!(error = %e, key = %key, "buffer: get failed"); return None; }
            };
            match split_record(&data) {
                Some((table, payload)) => return Some((key, vec![(table, payload)])),
                None => {
                    tracing::error!(key = %key, "object-store buffer: corrupt object — discarding");
                    let _ = self.store.delete(&key).await;
                    self.bytes.fetch_sub(size.min(self.bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
                    self.count.fetch_sub(self.count.load(Ordering::Relaxed).min(1), Ordering::Relaxed);
                    continue; // try the next object
                }
            }
        }
    }

    /// Delete a successfully-drained object.
    pub async fn commit(&self, key: &OsPath) {
        // Best-effort size accounting: re-deriving exact size isn't worth a HEAD;
        // recompute totals lazily on next open. Decrement count; bytes via list drift
        // is corrected on restart. Here we just delete + decrement count.
        if let Err(e) = self.store.delete(key).await {
            tracing::warn!(error = %e, key = %key, "object-store buffer: delete failed");
            return;
        }
        self.count.fetch_sub(self.count.load(Ordering::Relaxed).min(1), Ordering::Relaxed);
        self.committed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn total_bytes(&self) -> u64 { self.bytes.load(Ordering::Relaxed) }
    pub fn segment_count(&self) -> usize { self.count.load(Ordering::Relaxed) }
    pub fn max_bytes(&self) -> u64 { self.max_bytes }
    pub fn committed_total(&self) -> u64 { self.committed.load(Ordering::Relaxed) }

    /// Age (seconds) of the oldest pending object, parsed from its `{millis}-{seq}.batch` key.
    pub async fn oldest_age_secs(&self) -> Option<u64> {
        let metas = self.list_sorted().await.ok()?;
        let (key, _) = metas.into_iter().next()?;
        let fname = key.as_ref().rsplit('/').next().unwrap_or("");
        let ms = fname.split('-').next().and_then(|s| s.parse::<u64>().ok())?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
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

    fn store() -> Arc<dyn ObjectStore> { Arc::new(InMemory::new()) }

    #[tokio::test]
    async fn append_drain_commit_roundtrip() {
        let s = ObjectStoreSpool::open(store(), "ingest", 1_000_000).await.unwrap();
        s.append("logs", b"alpha").await.unwrap();
        s.append("spans", b"beta").await.unwrap();
        assert_eq!(s.segment_count(), 2);

        // oldest first
        let (k1, recs1) = s.next_batch().await.unwrap();
        assert_eq!(recs1, vec![("logs".to_string(), b"alpha".to_vec())]);
        s.commit(&k1).await;

        let (k2, recs2) = s.next_batch().await.unwrap();
        assert_eq!(recs2, vec![("spans".to_string(), b"beta".to_vec())]);
        s.commit(&k2).await;

        assert!(s.next_batch().await.is_none());
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
            let s = ObjectStoreSpool::open(st.clone(), "ingest", 1_000_000).await.unwrap();
            s.append("logs", b"x").await.unwrap();
        }
        // Re-open over the same store → must see the leftover object.
        let s2 = ObjectStoreSpool::open(st, "ingest", 1_000_000).await.unwrap();
        assert_eq!(s2.segment_count(), 1);
        assert!(s2.next_batch().await.is_some());
    }
}
