//! On-demand reading of records from Tansu's S3 storage.
//!
//! Seek model (see issue #9):
//! - Absolute offset = filename base offset + record `offset_delta`.
//! - To read from offset X we use the **predecessor** batch (largest base
//!   offset ≤ X), because X may sit mid-batch.
//! - Time-seek and the high-watermark tail read the tail/probe object in full,
//!   because a coalesced object (Tansu #50) holds several batches and its span
//!   and newest timestamp span all of them, not just the leading header; the
//!   `watermark.json` `timestamps` map is always null in S3 storage and unused.

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use serde::Deserialize;

use super::{
    keys::Keys,
    model::{
        decode_batch, frame_max_timestamp, frame_offset_span, DecodedRecord, OffsetSpec, Watermark,
    },
    StorageError, StorageSource,
};

/// Raw shape of `watermark.json` (`{ low, high, timestamps }`).
#[derive(Deserialize)]
struct WatermarkRaw {
    low: Option<i64>,
    high: Option<i64>,
}

impl StorageSource {
    /// Reads a partition's low/high watermark.
    ///
    /// In Tansu's beta.6 S3 engine `watermark.json` is only a *lazily persisted
    /// hint*: `low`/`high` are written on a cold ListOffsets/fetch, never on the
    /// produce hot path (that would make it a per-write hot object). So both can
    /// be null or stale (frozen at the last cold read) while the real offsets
    /// have moved far past. The record objects are the authority:
    /// - `low`  — stored value if present, else the earliest surviving batch's
    ///   base offset (the log start after retention/compaction), else 0.
    /// - `high` — the last batch's `base + lastOffsetDelta + 1`, via a bounded
    ///   tail scan floored by the stored/cached high (see
    ///   [`Self::high_watermark`]).
    pub async fn watermark(&self, topic: &str, partition: i32) -> Result<Watermark, StorageError> {
        let raw: WatermarkRaw = self
            .get_json(&self.keys().watermark(topic, partition))
            .await?;

        let low = match raw.low {
            Some(low) => low,
            None => self.first_base_offset(topic, partition).await?.unwrap_or(0),
        };
        let high = self.high_watermark(topic, partition, raw.high).await?;

        Ok(Watermark { low, high })
    }

    /// True high watermark, derived from the record objects (the authority).
    ///
    /// `watermark.json` `high` is only a stale floor in beta.6, so trusting it
    /// caps the watermark below the real tail. Instead: take the larger of the
    /// in-memory cached high and the stored high as a floor, list only the
    /// batches at/after it (`list_with_offset` — bounded, not a full-partition
    /// scan), and take the last batch's `base + lastOffsetDelta + 1`. The high
    /// is monotonic, so the cached floor never needs a TTL.
    async fn high_watermark(
        &self,
        topic: &str,
        partition: i32,
        stored: Option<i64>,
    ) -> Result<i64, StorageError> {
        let floor = [self.cached_high(topic, partition), stored]
            .into_iter()
            .flatten()
            .max();

        let high = match self.tail_last_base(topic, partition, floor).await? {
            Some(base) => {
                // The tail object may be coalesced, so its span covers all its
                // sub-batches, not just the first header.
                let bytes = self
                    .get_bytes(&self.keys().batch(topic, partition, base))
                    .await?;
                base + frame_offset_span(&bytes)
            }
            // No batch at/after the floor: the floor is the best we know.
            None => floor.unwrap_or(0),
        }
        .max(floor.unwrap_or(0));

        self.set_cached_high(topic, partition, high);
        Ok(high)
    }

    /// Base offset of the earliest record batch (first key under `records/`),
    /// or `None` when the partition has no batches.
    async fn first_base_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, StorageError> {
        let prefix = self.keys().records_prefix(topic, partition);
        let mut stream = self.store().list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            if let Some(offset) = Keys::base_offset_from_batch(&meta?.location) {
                return Ok(Some(offset));
            }
        }
        Ok(None)
    }

    /// Highest base offset at/after `floor`. With a floor, lists only the tail
    /// (`list_with_offset`); cold (no floor) it falls back to a full scan.
    async fn tail_last_base(
        &self,
        topic: &str,
        partition: i32,
        floor: Option<i64>,
    ) -> Result<Option<i64>, StorageError> {
        let prefix = self.keys().records_prefix(topic, partition);
        let mut stream = match floor {
            Some(f) => self
                .store()
                .list_with_offset(Some(&prefix), &self.keys().batch_floor(topic, partition, f)),
            None => self.store().list(Some(&prefix)),
        };
        let mut last = None;
        while let Some(meta) = stream.next().await {
            if let Some(offset) = Keys::base_offset_from_batch(&meta?.location) {
                last = Some(offset.max(last.unwrap_or(i64::MIN)));
            }
        }
        Ok(last)
    }

    /// Cached high watermark for a partition, if computed earlier this process.
    fn cached_high(&self, topic: &str, partition: i32) -> Option<i64> {
        self.high_cache
            .lock()
            .ok()?
            .get(&(topic.to_string(), partition))
            .copied()
    }

    /// Records a high watermark, only ever raising the cached value (the high is
    /// monotonic, so a cached entry stays a valid floor).
    fn set_cached_high(&self, topic: &str, partition: i32, high: i64) {
        if let Ok(mut cache) = self.high_cache.lock() {
            let entry = cache.entry((topic.to_string(), partition)).or_insert(high);
            *entry = (*entry).max(high);
        }
    }

    /// Lists the base offsets of every record batch in a partition, sorted
    /// ascending. Filenames are zero-padded, so listing order is numeric order.
    pub async fn list_base_offsets(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Vec<i64>, StorageError> {
        let prefix = self.keys().records_prefix(topic, partition);
        let mut stream = self.store().list(Some(&prefix));
        let mut offsets = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            if let Some(offset) = Keys::base_offset_from_batch(&meta.location) {
                offsets.push(offset);
            }
        }
        offsets.sort_unstable();
        Ok(offsets)
    }

    /// Fetches up to `limit` records from a partition starting at `spec`.
    ///
    /// Control batches are skipped and records outside `[start, high)` are
    /// excluded. Reads only the batches it needs (from the predecessor batch
    /// onward), stopping once `limit` records are collected.
    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        spec: OffsetSpec,
        limit: usize,
    ) -> Result<Vec<DecodedRecord>, StorageError> {
        let wm = self.watermark(topic, partition).await?;

        let start = match spec {
            OffsetSpec::Earliest => wm.low,
            OffsetSpec::Latest => (wm.high - limit as i64).max(wm.low),
            OffsetSpec::At(offset) => offset.clamp(wm.low, wm.high),
            OffsetSpec::Timestamp(ts) => self.seek_time(topic, partition, ts).await?,
        };

        if start >= wm.high || limit == 0 {
            return Ok(Vec::new());
        }

        let bases = self.list_base_offsets(topic, partition).await?;
        let from = predecessor_index(&bases, start);

        let mut out = Vec::with_capacity(limit.min(wm.count() as usize));
        for &base in &bases[from..] {
            if out.len() >= limit {
                break;
            }
            let bytes = self
                .get_bytes(&self.keys().batch(topic, partition, base))
                .await?;
            for record in decode_batch(bytes, base, partition)? {
                if record.offset >= start && record.offset < wm.high {
                    out.push(record);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    /// Returns the base offset of the first object that could contain a record
    /// at or after `target_ts`, found by binary-searching objects on their
    /// newest timestamp.
    ///
    /// An object may be coalesced (Tansu #50), so its newest record sits in its
    /// last sub-batch; the search compares against the object's max timestamp
    /// across all sub-batches, which needs the full object. Slightly
    /// over-inclusive: the returned object may start with a few records older
    /// than `target_ts`. Returns `high` when no object reaches the target.
    pub async fn seek_time(
        &self,
        topic: &str,
        partition: i32,
        target_ts: i64,
    ) -> Result<i64, StorageError> {
        let bases = self.list_base_offsets(topic, partition).await?;
        if bases.is_empty() {
            return Ok(0);
        }

        // Leftmost object whose max_timestamp >= target_ts.
        let (mut lo, mut hi) = (0usize, bases.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            let max_ts = self
                .object_max_timestamp(topic, partition, bases[mid])
                .await?;
            // A frame with no parseable sub-batch can't reach the target.
            if max_ts.is_some_and(|ts| ts >= target_ts) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        match bases.get(lo) {
            Some(&base) => Ok(base),
            None => self.watermark(topic, partition).await.map(|wm| wm.high),
        }
    }

    /// The newest record timestamp of an object, across all its sub-batches.
    async fn object_max_timestamp(
        &self,
        topic: &str,
        partition: i32,
        base: i64,
    ) -> Result<Option<i64>, StorageError> {
        let bytes = self
            .get_bytes(&self.keys().batch(topic, partition, base))
            .await?;
        Ok(frame_max_timestamp(&bytes))
    }

    /// Reads an object's full bytes.
    async fn get_bytes(&self, path: &Path) -> Result<Bytes, StorageError> {
        let result = self
            .store()
            .get(path)
            .await
            .map_err(|e| StorageError::from_object(e, path))?;
        result
            .bytes()
            .await
            .map_err(|e| StorageError::from_object(e, path))
    }
}

/// Index of the predecessor batch: the largest base offset `<= target`, or 0.
fn predecessor_index(bases: &[i64], target: i64) -> usize {
    match bases.binary_search(&target) {
        Ok(i) => i,
        Err(0) => 0,
        Err(i) => i - 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predecessor_picks_largest_base_at_or_below() {
        let bases = [0, 2, 4, 6];
        assert_eq!(predecessor_index(&bases, 0), 0); // exact
        assert_eq!(predecessor_index(&bases, 3), 1); // mid-batch → base 2
        assert_eq!(predecessor_index(&bases, 4), 2); // exact
        assert_eq!(predecessor_index(&bases, 100), 3); // past end → last batch
    }

    // Real single-record batches (lastOffsetDelta = 0) → each spans one offset.
    const BATCH: &[u8] = include_bytes!("../../tests/fixtures/offset-0.batch");
    const BATCH2: &[u8] = include_bytes!("../../tests/fixtures/offset-2.batch");

    async fn seed(store: &object_store::memory::InMemory, src: &StorageSource, base: i64) {
        use object_store::{ObjectStore, PutPayload};
        store
            .put(
                &src.keys().batch("t", 0, base),
                PutPayload::from(BATCH.to_vec()),
            )
            .await
            .unwrap();
    }

    /// Seeds a coalesced object (several batches concatenated, Tansu #50) at
    /// `base`, named by the first sub-batch's offset.
    async fn seed_coalesced(
        store: &object_store::memory::InMemory,
        src: &StorageSource,
        base: i64,
        parts: &[&[u8]],
    ) {
        use object_store::{ObjectStore, PutPayload};
        let mut buf = Vec::new();
        for part in parts {
            buf.extend_from_slice(part);
        }
        store
            .put(&src.keys().batch("t", 0, base), PutPayload::from(buf))
            .await
            .unwrap();
    }

    /// #72/#71: a stale `watermark.json` (high far below the real tail, null low)
    /// must NOT cap the watermark — high is derived from the last batch, low from
    /// the earliest batch.
    #[tokio::test]
    async fn high_derived_from_records_ignores_stale_watermark_floor() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        // stale hint: high=5 (real tail is higher), low absent
        store
            .put(
                &src.keys().watermark("t", 0),
                PutPayload::from(br#"{"low":null,"high":5,"timestamps":null}"#.to_vec()),
            )
            .await
            .unwrap();
        for base in [0_i64, 10, 20] {
            seed(&store, &src, base).await;
        }

        let wm = src.watermark("t", 0).await.unwrap();
        assert_eq!(
            wm.high, 21,
            "high = last base (20) + 1, not the stale floor 5"
        );
        assert_eq!(
            wm.low, 0,
            "low derived from earliest batch when null in file"
        );
    }

    /// #73: the cached high floors the next bounded scan, and that scan still
    /// catches batches appended above the floor.
    #[tokio::test]
    async fn cached_high_floors_scan_and_catches_new_tail() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        store
            .put(
                &src.keys().watermark("t", 0),
                PutPayload::from(br#"{"low":null,"high":null,"timestamps":null}"#.to_vec()),
            )
            .await
            .unwrap();
        for base in [0_i64, 10, 20] {
            seed(&store, &src, base).await;
        }
        assert_eq!(src.watermark("t", 0).await.unwrap().high, 21); // caches 21

        seed(&store, &src, 30).await; // new batch above the cached floor
        assert_eq!(
            src.watermark("t", 0).await.unwrap().high,
            31,
            "bounded scan from the cached floor still finds the new tail"
        );
    }

    /// #80: when the tail object is coalesced, the high watermark must span
    /// every sub-batch, not just the first — else it under-reports the tail.
    #[tokio::test]
    async fn high_watermark_spans_all_sub_batches_of_coalesced_tail() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        store
            .put(
                &src.keys().watermark("t", 0),
                PutPayload::from(br#"{"low":null,"high":null,"timestamps":null}"#.to_vec()),
            )
            .await
            .unwrap();
        seed(&store, &src, 0).await; // legacy single batch at 0
                                     // Tail object at base 10 holds two single-record batches ⇒ span 2.
        seed_coalesced(&store, &src, 10, &[BATCH, BATCH2]).await;

        assert_eq!(
            src.watermark("t", 0).await.unwrap().high,
            12,
            "high = tail base (10) + span over both sub-batches (2)"
        );
    }

    /// #80: reading a coalesced object returns every record with contiguous
    /// absolute offsets, end-to-end through `fetch`.
    #[tokio::test]
    async fn fetch_returns_all_records_of_a_coalesced_object() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        store
            .put(
                &src.keys().watermark("t", 0),
                PutPayload::from(br#"{"low":null,"high":null,"timestamps":null}"#.to_vec()),
            )
            .await
            .unwrap();
        seed_coalesced(&store, &src, 0, &[BATCH, BATCH2]).await;

        let records = src.fetch("t", 0, OffsetSpec::Earliest, 100).await.unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [0, 1],
            "both sub-batch records surface, with contiguous offsets"
        );
    }
}
