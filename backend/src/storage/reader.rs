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
        let view = self.build_segment_view(topic, partition).await?;
        if !view.is_empty() {
            return self.segment_watermark(topic, partition, &view).await;
        }

        self.legacy_watermark(topic, partition).await
    }

    /// Watermark for a pure-legacy topition (records under `records/`), derived
    /// from the record objects with the `watermark.json` hint as a floor.
    async fn legacy_watermark(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Watermark, StorageError> {
        let raw = self.watermark_hint(topic, partition).await?;
        let low = match raw.low {
            Some(low) => low,
            None => self.first_base_offset(topic, partition).await?.unwrap_or(0),
        };
        let high = self.high_watermark(topic, partition, raw.high).await?;
        Ok(Watermark { low, high })
    }

    /// Watermark for a segment-backed (possibly hybrid) topition. The footers are
    /// the authority: `high` is the tail of the segment region; `low` is the
    /// earliest legacy record batch when the topic is hybrid (legacy `[0, C)` +
    /// segments `[C, ∞)`), otherwise the earliest segment offset `C`.
    async fn segment_watermark(
        &self,
        topic: &str,
        partition: i32,
        view: &super::segview::SegView,
    ) -> Result<Watermark, StorageError> {
        let seg_low = view.low().unwrap_or(0);
        let high = view.high().unwrap_or(seg_low);
        // A legacy region below the seam means a hybrid topic; its earliest batch
        // is the log start. A pure-segment topic has no `records/` objects.
        let low = self
            .first_base_offset(topic, partition)
            .await?
            .filter(|&b| b < seg_low)
            .unwrap_or(seg_low);
        self.set_cached_high(topic, partition, high);
        Ok(Watermark { low, high })
    }

    /// Reads the `watermark.json` hint, tolerating its absence. In the leaseless
    /// engine it is only lazily persisted (never on the produce hot path), so a
    /// live partition — segment-backed ones especially — may have none.
    async fn watermark_hint(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<WatermarkRaw, StorageError> {
        match self
            .get_json::<WatermarkRaw>(&self.keys().watermark(topic, partition))
            .await
        {
            Ok(raw) => Ok(raw),
            Err(StorageError::NotFound(_)) => Ok(WatermarkRaw {
                low: None,
                high: None,
            }),
            Err(e) => Err(e),
        }
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
        // A segment deleted by compaction between listing and read is retried
        // once against a freshly-listed view (mirrors Tansu's fetch retry-on-404).
        for attempt in 0..2 {
            let view = self.build_segment_view(topic, partition).await?;
            let wm = if view.is_empty() {
                self.legacy_watermark(topic, partition).await?
            } else {
                self.segment_watermark(topic, partition, &view).await?
            };

            let start = match spec {
                OffsetSpec::Earliest => wm.low,
                OffsetSpec::Latest => (wm.high - limit as i64).max(wm.low),
                OffsetSpec::At(offset) => offset.clamp(wm.low, wm.high),
                OffsetSpec::Timestamp(ts) => self.seek_time(topic, partition, ts).await?,
            };

            if start >= wm.high || limit == 0 {
                return Ok(Vec::new());
            }

            let mut out = Vec::with_capacity(limit.min(wm.count() as usize));

            if view.is_empty() {
                self.fetch_legacy_into(topic, partition, start, wm.high, limit, &mut out)
                    .await?;
                return Ok(out);
            }

            // Hybrid: serve the legacy region below the seam `C`, then segments.
            let seam = view.low().unwrap_or(wm.high);
            if start < seam {
                let legacy_hi = seam.min(wm.high);
                self.fetch_legacy_into(topic, partition, start, legacy_hi, limit, &mut out)
                    .await?;
            }

            match self
                .fetch_segments_into(&view, partition, start, wm.high, limit, &mut out)
                .await?
            {
                true => return Ok(out),
                // A region vanished (compaction); retry with a fresh view.
                false if attempt == 0 => continue,
                false => return Ok(out),
            }
        }
        Ok(Vec::new())
    }

    /// Reads legacy `records/` batches for `[start, high)`, appending decoded
    /// records to `out` until `limit` is reached.
    async fn fetch_legacy_into(
        &self,
        topic: &str,
        partition: i32,
        start: i64,
        high: i64,
        limit: usize,
        out: &mut Vec<DecodedRecord>,
    ) -> Result<(), StorageError> {
        let bases = self.list_base_offsets(topic, partition).await?;
        if bases.is_empty() {
            return Ok(());
        }
        let from = predecessor_index(&bases, start);
        for &base in &bases[from..] {
            if out.len() >= limit {
                break;
            }
            let bytes = self
                .get_bytes(&self.keys().batch(topic, partition, base))
                .await?;
            for record in decode_batch(bytes, base, partition)? {
                if record.offset >= start && record.offset < high {
                    out.push(record);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Reads segment-backed records for `[start, high)` from the view's owner
    /// pieces, appending to `out` until `limit`. Returns `false` if a segment
    /// region vanished mid-read (the caller retries with a fresh view).
    async fn fetch_segments_into(
        &self,
        view: &super::segview::SegView,
        partition: i32,
        start: i64,
        high: i64,
        limit: usize,
        out: &mut Vec<DecodedRecord>,
    ) -> Result<bool, StorageError> {
        for piece in &view.pieces {
            if out.len() >= limit {
                break;
            }
            // Records this piece owns and that fall in the requested window.
            let lo = start.max(piece.lo);
            let hi = high.min(piece.hi);
            if lo >= hi {
                continue;
            }
            let Some(bytes) = self.read_segment_region(piece).await? else {
                return Ok(false); // compaction race
            };
            for record in decode_batch(bytes, piece.base_offset, partition)? {
                if record.offset >= lo && record.offset < hi {
                    out.push(record);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(true)
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
        let view = self.build_segment_view(topic, partition).await?;
        if view.is_empty() {
            return self.seek_time_legacy(topic, partition, target_ts).await;
        }

        // Hybrid: the legacy region `[0, C)` is older, so search it first; a hit
        // there is the answer. Otherwise the target lies in the segment region.
        let seam = view.low().unwrap_or(0);
        let legacy_bases: Vec<i64> = self
            .list_base_offsets(topic, partition)
            .await?
            .into_iter()
            .filter(|&b| b < seam)
            .collect();
        if let Some(base) = self
            .seek_time_in_bases(topic, partition, &legacy_bases, target_ts)
            .await?
        {
            return Ok(base);
        }

        // First owner piece whose region reaches the target (footer timestamps,
        // no object read). Over-inclusive like the legacy path.
        for piece in &view.pieces {
            if piece.max_timestamp >= target_ts {
                return Ok(piece.lo);
            }
        }
        Ok(view.high().unwrap_or(seam))
    }

    /// Legacy time-seek over `records/` objects (pure-legacy topic).
    async fn seek_time_legacy(
        &self,
        topic: &str,
        partition: i32,
        target_ts: i64,
    ) -> Result<i64, StorageError> {
        let bases = self.list_base_offsets(topic, partition).await?;
        if bases.is_empty() {
            return Ok(0);
        }
        match self
            .seek_time_in_bases(topic, partition, &bases, target_ts)
            .await?
        {
            Some(base) => Ok(base),
            None => self.watermark(topic, partition).await.map(|wm| wm.high),
        }
    }

    /// Leftmost base offset in `bases` whose object's newest record reaches
    /// `target_ts`, by binary search on per-object max timestamp. `None` when no
    /// object in the slice reaches the target (the caller looks further on).
    async fn seek_time_in_bases(
        &self,
        topic: &str,
        partition: i32,
        bases: &[i64],
        target_ts: i64,
    ) -> Result<Option<i64>, StorageError> {
        if bases.is_empty() {
            return Ok(None);
        }
        let (mut lo, mut hi) = (0usize, bases.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            let max_ts = self
                .object_max_timestamp(topic, partition, bases[mid])
                .await?;
            if max_ts.is_some_and(|ts| ts >= target_ts) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        Ok(bases.get(lo).copied())
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

    // --- Prefix-coalesced virtual-topic segments (#82) ---------------------

    const BATCH3: &[u8] = include_bytes!("../../tests/fixtures/offset-4.batch");

    // A three-component topic coalesces under prefix `org.env.conn`.
    const SEG_TOPIC: &str = "org.env.conn.orders";

    async fn put_segment(
        store: &object_store::memory::InMemory,
        src: &StorageSource,
        seq: u64,
        version: u16,
        epoch: i64,
        regions: &[super::super::segment::TestRegion<'_>],
    ) {
        use object_store::{ObjectStore, PutPayload};
        let bytes = super::super::segment::build_test_segment(version, epoch, regions);
        let path = src.keys().segment(&Keys::prefix_of(SEG_TOPIC), seq);
        store
            .put(&path, PutPayload::from(bytes.to_vec()))
            .await
            .unwrap();
    }

    /// #82: a segment-backed topic (no `records/` objects) — watermark comes from
    /// the footer and `fetch` reads records via the segment region.
    #[tokio::test]
    async fn segment_backed_topic_reads_from_footer_and_region() {
        use object_store::memory::InMemory;
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        // One segment, one sub-stream for orders/p0: 3 records at offsets 0..3.
        let region: Vec<u8> = [BATCH, BATCH2, BATCH3].concat();
        put_segment(
            &store,
            &src,
            0,
            2, // v2 footer, as production emits
            7,
            &[(SEG_TOPIC, 0, 0, 3, &region, 555)],
        )
        .await;

        let wm = src.watermark(SEG_TOPIC, 0).await.unwrap();
        assert_eq!((wm.low, wm.high), (0, 3), "watermark from the footer");

        let records = src
            .fetch(SEG_TOPIC, 0, OffsetSpec::Earliest, 100)
            .await
            .unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [0, 1, 2],
            "all three records read from the segment region"
        );

        // A mid-stream `At` read starts where asked.
        let from1 = src
            .fetch(SEG_TOPIC, 0, OffsetSpec::At(1), 100)
            .await
            .unwrap();
        assert_eq!(from1.iter().map(|r| r.offset).collect::<Vec<_>>(), [1, 2]);
    }

    /// #92: a compacted topic with ≥ 3 dotted components is routed under its own
    /// **full name**, and the pin is what says so. Derived, Kotatsu would list
    /// `prefixes/org.env.conn/segments/`, find nothing, and render the topic as
    /// empty — no messages, no watermark, no error.
    #[tokio::test]
    async fn compacted_topic_reads_from_its_pinned_prefix() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        // The broker pinned this topic to its own name (compacted routing).
        store
            .put(
                &src.keys().topic_routing(SEG_TOPIC),
                PutPayload::from(format!(r#"{{"prefix":"{SEG_TOPIC}"}}"#).into_bytes()),
            )
            .await
            .unwrap();

        // Its segments therefore live under `prefixes/{topic}/segments/`, not
        // under the connector prefix the name derives to.
        let region: Vec<u8> = [BATCH, BATCH2, BATCH3].concat();
        let bytes = super::super::segment::build_test_segment(
            3, // v3, as production emits
            7,
            &[(SEG_TOPIC, 0, 0, 3, &region, 555)],
        );
        store
            .put(
                &src.keys().segment(SEG_TOPIC, 0),
                PutPayload::from(bytes.to_vec()),
            )
            .await
            .unwrap();
        assert_ne!(
            Keys::prefix_of(SEG_TOPIC),
            SEG_TOPIC,
            "the derivation this test exists to disprove"
        );

        let wm = src.watermark(SEG_TOPIC, 0).await.unwrap();
        assert_eq!((wm.low, wm.high), (0, 3), "watermark, not an empty topic");
        let records = src
            .fetch(SEG_TOPIC, 0, OffsetSpec::Earliest, 100)
            .await
            .unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [0, 1, 2],
            "records read through the pinned prefix"
        );
    }

    /// #82: a hybrid topic — legacy `records/` below the seam `C`, segments above.
    /// `fetch` stitches the two regions continuously across `C`.
    #[tokio::test]
    async fn hybrid_topic_stitches_legacy_then_segments() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        // Legacy region [0, 1): a single batch at offset 0.
        store
            .put(
                &src.keys().batch(SEG_TOPIC, 0, 0),
                PutPayload::from(BATCH.to_vec()),
            )
            .await
            .unwrap();
        // Segment region [1, 3): two records, seam C = 1.
        let region: Vec<u8> = [BATCH2, BATCH3].concat();
        put_segment(&store, &src, 0, 2, 7, &[(SEG_TOPIC, 0, 1, 2, &region, 999)]).await;

        let wm = src.watermark(SEG_TOPIC, 0).await.unwrap();
        assert_eq!(
            (wm.low, wm.high),
            (0, 3),
            "low from the legacy batch, high from the segment tail"
        );

        let records = src
            .fetch(SEG_TOPIC, 0, OffsetSpec::Earliest, 100)
            .await
            .unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [0, 1, 2],
            "offset 0 from legacy, 1 & 2 from the segment, contiguous across the seam"
        );
    }

    /// #82: overlap resolution end-to-end — a compaction-merged segment at a
    /// higher sequence supersedes the originals it covers, so records are read
    /// once, from the merged segment.
    #[tokio::test]
    async fn overlapping_segments_read_once_via_winner() {
        use object_store::memory::InMemory;
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        // Two original single-record segments [0,1) and [1,2)…
        put_segment(&store, &src, 0, 2, 3, &[(SEG_TOPIC, 0, 0, 1, BATCH, 1)]).await;
        put_segment(&store, &src, 1, 2, 3, &[(SEG_TOPIC, 0, 1, 1, BATCH2, 2)]).await;
        // …superseded by a merged segment [0,2) at a higher seq (same epoch).
        let merged: Vec<u8> = [BATCH, BATCH2].concat();
        put_segment(&store, &src, 9, 2, 3, &[(SEG_TOPIC, 0, 0, 2, &merged, 2)]).await;

        let records = src
            .fetch(SEG_TOPIC, 0, OffsetSpec::Earliest, 100)
            .await
            .unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [0, 1],
            "each offset surfaces exactly once, from the merged winner"
        );
    }
}
