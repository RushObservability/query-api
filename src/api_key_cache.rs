//! Process-local cache of resolved API key grants, keyed by key hash. Only
//! found grants are stored, so unknown keys cannot grow it.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::clickhouse_config::ApiKeyGrant;

/// Grants are re-read after this long. A key revoked on another replica stops
/// working here within this window; revokes on this replica evict at once.
pub const FRESH_TTL: Duration = Duration::from_secs(30);
/// While ClickHouse is unreachable, ingest-only keys keep their last good
/// grant this long so telemetry still reaches the ingest spool.
pub const INGEST_STALE_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_ENTRIES: usize = 8_192;

#[derive(Default)]
struct Inner {
    grants: HashMap<String, (ApiKeyGrant, Instant)>,
    /// Bumped whenever a cached grant is revoked or evicted, so a lookup that
    /// started before that cannot re-insert the revoked grant.
    generation: u64,
}

pub struct ApiKeyCache {
    inner: Mutex<Inner>,
    max_entries: usize,
}

impl Default for ApiKeyCache {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            max_entries: MAX_ENTRIES,
        }
    }
}

impl ApiKeyCache {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Take this before the ClickHouse lookup and pass it to [`Self::insert`].
    pub fn generation(&self) -> u64 {
        self.lock().generation
    }

    pub fn fresh(&self, key_hash: &str) -> Option<ApiKeyGrant> {
        self.get(key_hash, Instant::now(), FRESH_TTL, false)
    }

    /// Last good grant for an ingest-only key, for use only when the lookup
    /// failed. Query and legacy keys always fail closed.
    pub fn stale_ingest(&self, key_hash: &str) -> Option<ApiKeyGrant> {
        self.get(key_hash, Instant::now(), INGEST_STALE_TTL, true)
    }

    fn get(
        &self,
        key_hash: &str,
        now: Instant,
        ttl: Duration,
        ingest_only: bool,
    ) -> Option<ApiKeyGrant> {
        self.lock()
            .grants
            .get(key_hash)
            .filter(|(grant, at)| {
                now.saturating_duration_since(*at) < ttl
                    && (!ingest_only || grant.key_type == "ingest")
            })
            .map(|(grant, _)| grant.clone())
    }

    pub fn insert(&self, key_hash: &str, grant: ApiKeyGrant, generation: u64) {
        self.insert_at(key_hash, grant, generation, Instant::now());
    }

    fn insert_at(&self, key_hash: &str, grant: ApiKeyGrant, generation: u64, now: Instant) {
        let mut inner = self.lock();
        if inner.generation != generation {
            return;
        }
        if !inner.grants.contains_key(key_hash) && inner.grants.len() >= self.max_entries {
            inner
                .grants
                .retain(|_, (_, at)| now.saturating_duration_since(*at) < INGEST_STALE_TTL);
            if inner.grants.len() >= self.max_entries {
                let oldest = inner
                    .grants
                    .iter()
                    .min_by_key(|(_, (_, at))| *at)
                    .map(|(hash, _)| hash.clone());
                if let Some(oldest) = oldest {
                    inner.grants.remove(&oldest);
                }
            }
        }
        inner.grants.insert(key_hash.to_string(), (grant, now));
    }

    /// The key no longer resolves (revoked on another replica). Unknown keys
    /// were never cached, so they leave the generation alone; otherwise a flood
    /// of random keys would refuse every real grant's insert.
    pub fn remove(&self, key_hash: &str) {
        let mut inner = self.lock();
        if inner.grants.remove(key_hash).is_some() {
            inner.generation = inner.generation.wrapping_add(1);
        }
    }

    /// Drop a revoked or changed key from this replica's cache.
    pub fn evict_key_id(&self, id: &str) {
        let mut inner = self.lock();
        inner.generation = inner.generation.wrapping_add(1);
        inner.grants.retain(|_, (grant, _)| grant.id != id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(id: &str, key_type: &str) -> ApiKeyGrant {
        ApiKeyGrant {
            id: id.into(),
            name: String::new(),
            tenant_id: "default".into(),
            key_type: key_type.into(),
            role: String::new(),
            agent: false,
            signals: vec!["logs".into()],
            rate_limit_per_minute: 0,
            source_cidrs: Vec::new(),
        }
    }

    fn fresh_at(cache: &ApiKeyCache, key_hash: &str, now: Instant) -> Option<ApiKeyGrant> {
        cache.get(key_hash, now, FRESH_TTL, false)
    }

    fn stale_at(cache: &ApiKeyCache, key_hash: &str, now: Instant) -> Option<ApiKeyGrant> {
        cache.get(key_hash, now, INGEST_STALE_TTL, true)
    }

    #[test]
    fn grants_expire_after_the_fresh_ttl() {
        let cache = ApiKeyCache::default();
        let start = Instant::now();
        cache.insert_at("h", grant("k1", "ingest"), cache.generation(), start);
        assert_eq!(cache.fresh("h").unwrap().id, "k1");
        assert!(fresh_at(&cache, "h", start + FRESH_TTL - Duration::from_secs(1)).is_some());
        assert!(fresh_at(&cache, "h", start + FRESH_TTL).is_none());
    }

    #[test]
    fn only_ingest_keys_fall_back_to_a_stale_grant() {
        let cache = ApiKeyCache::default();
        let start = Instant::now();
        let generation = cache.generation();
        cache.insert_at("ingest", grant("k1", "ingest"), generation, start);
        cache.insert_at("query", grant("k2", "query"), generation, start);
        cache.insert_at("legacy", grant("k3", "legacy"), generation, start);
        let later = start + Duration::from_secs(10 * 60);
        assert_eq!(stale_at(&cache, "ingest", later).unwrap().id, "k1");
        assert!(stale_at(&cache, "query", later).is_none());
        assert!(stale_at(&cache, "legacy", later).is_none());
        assert!(stale_at(&cache, "query", start).is_none());
        assert!(stale_at(&cache, "ingest", start + INGEST_STALE_TTL).is_none());
    }

    #[test]
    fn revoke_evicts_and_blocks_racing_inserts() {
        let cache = ApiKeyCache::default();
        cache.insert("h", grant("k1", "ingest"), cache.generation());
        let before_revoke = cache.generation();
        cache.evict_key_id("k1");
        assert!(cache.fresh("h").is_none());
        assert!(cache.stale_ingest("h").is_none());
        // A lookup that read the key before the revoke must not re-cache it.
        cache.insert("h", grant("k1", "ingest"), before_revoke);
        assert!(cache.fresh("h").is_none());

        cache.insert("h", grant("k1", "ingest"), cache.generation());
        cache.remove("h");
        assert!(cache.stale_ingest("h").is_none());
    }

    #[test]
    fn unknown_key_misses_do_not_block_caching_real_keys() {
        let cache = ApiKeyCache::default();
        let generation = cache.generation(); // a real key's lookup starts
        cache.remove("unknown"); // a random key misses meanwhile
        cache.insert("real", grant("k1", "ingest"), generation);
        assert_eq!(cache.fresh("real").unwrap().id, "k1");

        // A cached key that stops resolving still blocks racing inserts.
        let before_miss = cache.generation();
        cache.remove("real");
        cache.insert("real", grant("k1", "ingest"), before_miss);
        assert!(cache.fresh("real").is_none());
    }

    #[test]
    fn size_is_bounded_by_evicting_the_oldest_grant() {
        let cache = ApiKeyCache {
            inner: Default::default(),
            max_entries: 2,
        };
        let start = Instant::now();
        cache.insert_at("a", grant("a", "ingest"), 0, start);
        cache.insert_at("b", grant("b", "ingest"), 0, start + Duration::from_secs(1));
        cache.insert_at("c", grant("c", "ingest"), 0, start + Duration::from_secs(2));
        assert_eq!(cache.lock().grants.len(), 2);
        let now = start + Duration::from_secs(3);
        assert!(fresh_at(&cache, "a", now).is_none());
        assert!(fresh_at(&cache, "b", now).is_some());
        assert!(fresh_at(&cache, "c", now).is_some());
    }
}
