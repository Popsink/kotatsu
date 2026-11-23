//! The decoded segment-footer cache, bounded in bytes (DEV-664).
//!
//! A footer is immutable once its segment is written, so a cached one never
//! goes stale and evicting it only costs a re-read: one ranged GET. The cache
//! used to be an unbounded map, so a pod's memory grew with every segment it
//! had ever read — a prefix whose compaction stalled holds tens of thousands.
//!
//! The bound is on an estimate of each footer's heap footprint, not on an entry
//! count: a footer lists every sub-stream its segment carries, so one segment of
//! a busy connector weighs many times one of a quiet topic.
//!
//! The bound must hold a whole large prefix. A cold read of a prefix walks its
//! footers in order, so a prefix larger than the cache evicts its own first
//! footers before it ends — and the next read starts cold again, instead of
//! finishing what a request cut at the API timeout left. The default
//! ([`DEFAULT_CAPACITY_BYTES`]) holds a few 40 000-segment prefixes.

use std::{
    mem::size_of,
    sync::{Arc, Mutex},
};

use lru::LruCache;
use serde::Serialize;

use super::segment::{SegmentFooter, SubstreamEntry};

/// Default bound, in bytes: about 100 000 footers of five sub-streams, a
/// quarter of the chart's default 256 MiB memory limit.
pub(super) const DEFAULT_CAPACITY_BYTES: usize = 64 * 1024 * 1024;

/// Overrides [`DEFAULT_CAPACITY_BYTES`], in MiB.
const CAPACITY_ENV: &str = "KOTATSU_FOOTER_CACHE_MIB";

/// What the LRU keeps per entry besides the key and value: its node's links
/// and the hash table slot pointing at it.
const NODE_OVERHEAD: usize = 64;

/// A snapshot of the cache's size, reported by `/api/source/status`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct FooterCacheStats {
    /// Footers held.
    pub entries: usize,
    /// Their estimated footprint.
    pub bytes: usize,
    /// The bound on `bytes`.
    pub capacity_bytes: usize,
    /// Footers dropped to stay under the bound since the process started.
    pub evictions: u64,
}

pub(super) struct FooterCache {
    inner: Mutex<Inner>,
}

struct Inner {
    lru: LruCache<String, Arc<SegmentFooter>>,
    bytes: usize,
    capacity: usize,
    evictions: u64,
}

impl FooterCache {
    pub(super) fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                lru: LruCache::unbounded(),
                bytes: 0,
                capacity: capacity_bytes,
                evictions: 0,
            }),
        }
    }

    /// The bound from `KOTATSU_FOOTER_CACHE_MIB`, or the default.
    pub(super) fn from_env() -> Self {
        let mib = std::env::var(CAPACITY_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok());
        Self::new(mib.map_or(DEFAULT_CAPACITY_BYTES, |mib| {
            mib.saturating_mul(1024 * 1024)
        }))
    }

    /// The footer cached under `key`, marked most recently used.
    pub(super) fn get(&self, key: &str) -> Option<Arc<SegmentFooter>> {
        let mut inner = self.lock();
        inner.lru.get(key).cloned()
    }

    /// Caches a footer, evicting the least recently used ones past the bound.
    /// A footer larger than the whole bound is not cached.
    pub(super) fn insert(&self, key: String, footer: Arc<SegmentFooter>) {
        let size = footprint(&key, &footer);
        let mut inner = self.lock();
        if size > inner.capacity {
            return;
        }
        if let Some(old) = inner.lru.put(key.clone(), footer) {
            inner.bytes -= footprint(&key, &old);
        }
        inner.bytes += size;
        while inner.bytes > inner.capacity {
            let Some((key, old)) = inner.lru.pop_lru() else {
                break;
            };
            inner.bytes -= footprint(&key, &old);
            inner.evictions += 1;
        }
    }

    pub(super) fn stats(&self) -> FooterCacheStats {
        let inner = self.lock();
        FooterCacheStats {
            entries: inner.lru.len(),
            bytes: inner.bytes,
            capacity_bytes: inner.capacity,
            evictions: inner.evictions,
        }
    }

    #[cfg(test)]
    pub(super) fn clear(&self) {
        let mut inner = self.lock();
        inner.lru.clear();
        inner.bytes = 0;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // Every update leaves `Inner` consistent before anything can panic.
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A footer's estimated heap footprint under `key`: the key, the footer, its
/// entries and their topic names, and the LRU's own bookkeeping.
fn footprint(key: &str, footer: &SegmentFooter) -> usize {
    NODE_OVERHEAD
        + size_of::<String>()
        + key.len()
        + size_of::<SegmentFooter>()
        + footer.entries.capacity() * size_of::<SubstreamEntry>()
        + footer
            .entries
            .iter()
            .map(|e| e.topic.capacity())
            .sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn footer(topics: usize) -> Arc<SegmentFooter> {
        Arc::new(SegmentFooter {
            writer_epoch: 1,
            entries: (0..topics)
                .map(|i| SubstreamEntry {
                    topic: format!("acme.prod.db2.table_{i:03}"),
                    topic_id: None,
                    partition: 0,
                    base_offset: 0,
                    record_count: 1,
                    byte_start: 0,
                    byte_len: 1,
                    max_timestamp: 0,
                })
                .collect(),
        })
    }

    fn key(seq: u64) -> String {
        format!("clusters/c/prefixes/acme.prod.db2/segments/{seq:020}.seg")
    }

    /// DEV-664: the cache stays within its bound however many footers go
    /// through it, keeps the most recently used, and drops the oldest.
    #[test]
    fn stays_within_its_bound_across_more_footers_than_it_holds() {
        let one = footprint(&key(0), &footer(5));
        let cache = FooterCache::new(100 * one);

        for seq in 0..10_000 {
            cache.insert(key(seq), footer(5));
            let stats = cache.stats();
            assert!(stats.bytes <= stats.capacity_bytes, "{stats:?}");
        }
        let stats = cache.stats();
        assert_eq!(stats.entries, 100);
        assert_eq!(stats.bytes, 100 * one);
        assert_eq!(stats.evictions, 9_900);
        assert!(cache.get(&key(9_999)).is_some(), "the newest is kept");
        assert!(cache.get(&key(0)).is_none(), "the oldest is evicted");
    }

    /// Through the reader: a prefix of more segments than the cache holds still
    /// reads in full and right, and leaves the cache within its bound.
    #[tokio::test]
    async fn a_prefix_larger_than_the_cache_reads_in_full_within_the_bound() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = Arc::new(InMemory::new());
        let topic = "acme.prod.db2.orders";
        let one = footprint(
            "clusters/c/prefixes/acme.prod.db2/segments/00000000000000000000.seg",
            &footer(1),
        );
        let src = super::super::StorageSource::with_store(store.clone(), "c")
            .with_footer_cache_bytes(50 * one);
        store
            .put(
                &src.keys().topic_routing(topic),
                PutPayload::from(br#"{"prefix":"acme.prod.db2"}"#.to_vec()),
            )
            .await
            .unwrap();
        for seq in 0..500u64 {
            let segment = super::super::segment::build_test_segment(
                3,
                1,
                &[(topic, 0, seq as i64, 1, &[0u8; 10], 1, None)],
            );
            store
                .put(
                    &src.keys().segment("acme.prod.db2", seq),
                    PutPayload::from(segment.to_vec()),
                )
                .await
                .unwrap();
        }

        for _ in 0..2 {
            let segments = src.topic_segments(topic).await.unwrap();
            assert_eq!(segments.bytes.get(&0), Some(&5_000));
            assert_eq!(segments.views[&0].high(), Some(500));
            let stats = src.footer_cache_stats();
            assert!(stats.bytes <= stats.capacity_bytes, "{stats:?}");
            assert!(stats.entries < 500, "{stats:?}");
        }
        assert!(src.footer_cache_stats().evictions >= 900);
    }

    /// A read refreshes a footer, so a hot one outlives colder ones inserted
    /// before and after it.
    #[test]
    fn a_read_footer_outlives_unread_ones() {
        let one = footprint(&key(0), &footer(5));
        let cache = FooterCache::new(3 * one);
        for seq in 0..3 {
            cache.insert(key(seq), footer(5));
        }
        assert!(cache.get(&key(0)).is_some());
        cache.insert(key(3), footer(5));
        assert!(cache.get(&key(0)).is_some(), "read, so kept");
        assert!(cache.get(&key(1)).is_none(), "least recently used");
    }

    /// The bound is in bytes: a footer with many sub-streams weighs more, and
    /// re-inserting a key replaces its weight rather than adding to it.
    #[test]
    fn weighs_footers_by_their_sub_streams() {
        let small = footprint(&key(0), &footer(1));
        let large = footprint(&key(0), &footer(50));
        assert!(large > 10 * small, "{large} vs {small}");

        let cache = FooterCache::new(large);
        cache.insert(key(0), footer(50));
        cache.insert(key(0), footer(50));
        assert_eq!(cache.stats().bytes, large);
        // One more evicts it.
        cache.insert(key(1), footer(1));
        assert_eq!(cache.stats().entries, 1);
        assert!(cache.stats().bytes <= large);

        // Larger than the whole bound: not cached, nothing evicted for it.
        let tiny = FooterCache::new(small);
        tiny.insert(key(0), footer(50));
        assert_eq!(
            tiny.stats(),
            FooterCacheStats {
                capacity_bytes: small,
                ..Default::default()
            }
        );
    }
}
