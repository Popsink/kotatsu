//! On-demand reading of records from Tansu's S3 storage.
//!
//! **Segments are the only layout** (Popsink/tansu#199 / #226 / #252, and #179
//! deleted the read paths for anything else). A topition's records live in shared
//! per-prefix segment objects, located through the footer index the segment view
//! builds; there is no `records/{offset}.batch` region below them and no seam to
//! stitch across. A `records/` object still sitting in a bucket is abandoned data
//! the broker will never serve — not the start of the log — so nothing here looks
//! for one (#93).
//!
//! Seek model (see issue #9):
//! - Absolute offset = the owning segment region's base offset + the record's
//!   `offset_delta`; a region is a concatenation of batches, so the running base
//!   advances by each sub-batch's offset span.
//! - Time-seek compares against each region's `max_timestamp`, straight from the
//!   footer — no object read. Slightly over-inclusive: the region it lands on may
//!   open with a few records older than the target.
//! - The `watermark.json` `timestamps` map is always null in S3 storage and unused.

use futures::StreamExt;
use serde::Deserialize;

use super::{
    model::{decode_batch, DecodedRecord, OffsetSpec, Watermark},
    StorageError, StorageSource,
};

/// Raw shape of `watermark.json`.
///
/// The broker's own struct is `{high, truncate, served}` plus an unknown-field
/// catch-all. `low` was deleted from it by Popsink/tansu#180 and is deliberately
/// **not** modelled here: it survives in objects written before that (the
/// catch-all round-trips it rather than erasing it), but a historic `low` predates
/// every segment written since and can only be stale, so it is ignored rather
/// than preferred over the footers (#94). Being absent from the struct, it also
/// cannot make an old object fail to parse.
#[derive(Deserialize)]
struct WatermarkRaw {
    high: Option<i64>,
}

impl StorageSource {
    /// Reads a partition's low/high watermark.
    ///
    /// The segment footers are the authority:
    /// - `low` — the base offset of the oldest live segment slice. An empty log
    ///   starts where it ends (`low == high`, Popsink/tansu#299): reporting 0
    ///   there advertises records no fetch can return.
    /// - `high` — the tail of the segment region, floored by the persisted
    ///   `watermark.json` high and by what this process has already seen. The
    ///   floor matters because the persisted value can sit *above* the segment
    ///   tail: a segment expiry raises it while deleting the segments that held
    ///   those offsets. The high is monotonic, so a cached floor never needs a TTL.
    ///
    /// `watermark.json` is lazily persisted — never written on the produce hot
    /// path, since that would make it a per-write hot object — so it can be absent
    /// or carry `{"high":null}` on a perfectly live partition. Both mean "no
    /// floor", not an error.
    pub async fn watermark(&self, topic: &str, partition: i32) -> Result<Watermark, StorageError> {
        let view = self.build_segment_view(topic, partition).await?;
        let raw = self.watermark_hint(topic, partition).await?;
        Ok(self.resolve_watermark(topic, partition, &view, &raw))
    }

    /// Folds the segment view and the persisted hint into a watermark. Split out
    /// because [`Self::fetch`] resolves both from the view it already built,
    /// instead of building a second one.
    fn resolve_watermark(
        &self,
        topic: &str,
        partition: i32,
        view: &super::segview::SegView,
        raw: &WatermarkRaw,
    ) -> Watermark {
        let high = [view.high(), raw.high, self.cached_high(topic, partition)]
            .into_iter()
            .flatten()
            .max()
            .unwrap_or(0);
        // No live segment ⇒ an empty log, which starts at its end.
        let low = view.low().unwrap_or(high);

        self.set_cached_high(topic, partition, high);
        Watermark { low, high }
    }

    /// Reads the `watermark.json` hint, tolerating its absence: it is only lazily
    /// persisted, so a live partition may have none at all.
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
            Err(StorageError::NotFound(_)) => Ok(WatermarkRaw { high: None }),
            Err(e) => Err(e),
        }
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

    /// Fetches up to `limit` records from a partition starting at `spec`.
    ///
    /// Control batches are skipped and records outside `[start, high)` are
    /// excluded. Reads only the segment regions that overlap the window, each with
    /// a single ranged GET of exactly this topition's byte span — never a whole
    /// segment object, and never another topic's bytes.
    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        spec: OffsetSpec,
        limit: usize,
    ) -> Result<Vec<DecodedRecord>, StorageError> {
        // A segment deleted by compaction or retention between listing and read is
        // retried once against a freshly-listed view (mirrors Tansu's fetch
        // retry-on-404).
        for attempt in 0..2 {
            let view = self.build_segment_view(topic, partition).await?;
            let raw = self.watermark_hint(topic, partition).await?;
            let wm = self.resolve_watermark(topic, partition, &view, &raw);

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

    /// The first offset that could carry a record at or after `target_ts`, from the
    /// footer's per-region `max_timestamp` — no object read.
    ///
    /// Slightly over-inclusive: the region returned may open with a few records
    /// older than `target_ts`. Answers the log end when no region reaches the
    /// target, so a fetch from here reads nothing.
    pub async fn seek_time(
        &self,
        topic: &str,
        partition: i32,
        target_ts: i64,
    ) -> Result<i64, StorageError> {
        let view = self.build_segment_view(topic, partition).await?;

        for piece in &view.pieces {
            if piece.max_timestamp >= target_ts {
                return Ok(piece.lo);
            }
        }

        let raw = self.watermark_hint(topic, partition).await?;
        Ok(self.resolve_watermark(topic, partition, &view, &raw).high)
    }

    /// Per-partition on-disk size for a topic: the bytes its sub-stream slices
    /// occupy inside the shared segment objects of its routed prefix.
    ///
    /// One listing of the prefix plus the footers (cached, immutable) — the same
    /// reads the segment view already makes, and no object content. Byte spans are
    /// per sub-stream, so a topic is charged its own share of a shared segment and
    /// never a sibling's. Partitions with no slice are absent from the map (callers
    /// default them to `0`).
    pub(super) async fn topic_storage_bytes(
        &self,
        topic: &str,
    ) -> Result<std::collections::BTreeMap<i32, i64>, StorageError> {
        let prefix = self.routed_prefix_of(topic).await?;
        let list_prefix = self.keys().segment_prefix(&prefix);

        let mut sizes = std::collections::BTreeMap::new();
        let mut stream = self.store().list(Some(&list_prefix));
        while let Some(meta) = stream.next().await {
            let location = meta?.location;
            let Some(seq) = super::Keys::seq_from_segment(&location) else {
                continue;
            };
            let Some(footer) = self.segment_footer(&prefix, seq).await? else {
                continue;
            };
            for entry in footer.entries.iter().filter(|e| e.topic == topic) {
                *sizes.entry(entry.partition).or_insert(0) += entry.byte_len as i64;
            }
        }
        Ok(sizes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Keys;

    // Real single-record batches (lastOffsetDelta = 0) → each spans one offset.
    const BATCH: &[u8] = include_bytes!("../../tests/fixtures/offset-0.batch");
    const BATCH2: &[u8] = include_bytes!("../../tests/fixtures/offset-2.batch");
    const BATCH3: &[u8] = include_bytes!("../../tests/fixtures/offset-4.batch");

    // A three-component topic coalesces under prefix `org.env.conn`.
    const SEG_TOPIC: &str = "org.env.conn.orders";

    /// Writes a segment under `prefix` holding `regions`, as Tansu's writer does.
    async fn put_segment_under(
        store: &object_store::memory::InMemory,
        src: &StorageSource,
        prefix: &str,
        seq: u64,
        version: u16,
        epoch: i64,
        regions: &[super::super::segment::TestRegion<'_>],
    ) {
        use object_store::{ObjectStore, PutPayload};
        let bytes = super::super::segment::build_test_segment(version, epoch, regions);
        store
            .put(
                &src.keys().segment(prefix, seq),
                PutPayload::from(bytes.to_vec()),
            )
            .await
            .unwrap();
    }

    /// The common case: `SEG_TOPIC` under its derived connector prefix.
    async fn put_segment(
        store: &object_store::memory::InMemory,
        src: &StorageSource,
        seq: u64,
        version: u16,
        epoch: i64,
        regions: &[super::super::segment::TestRegion<'_>],
    ) {
        put_segment_under(
            store,
            src,
            &Keys::prefix_of(SEG_TOPIC),
            seq,
            version,
            epoch,
            regions,
        )
        .await;
    }

    async fn put_watermark(
        store: &object_store::memory::InMemory,
        src: &StorageSource,
        topic: &str,
        partition: i32,
        body: &str,
    ) {
        use object_store::{ObjectStore, PutPayload};
        store
            .put(
                &src.keys().watermark(topic, partition),
                PutPayload::from(body.as_bytes().to_vec()),
            )
            .await
            .unwrap();
    }

    /// #82: a segment-backed topic — watermark comes from the footer and `fetch`
    /// reads records via the segment region.
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
            2, // v2 footer — read-only history, still in buckets
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

        // Its segments therefore live under `prefixes/{topic}/segments/`, not under
        // the connector prefix the name derives to.
        let region: Vec<u8> = [BATCH, BATCH2, BATCH3].concat();
        put_segment_under(
            &store,
            &src,
            SEG_TOPIC,
            0,
            3, // v3, as production emits
            7,
            &[(SEG_TOPIC, 0, 0, 3, &region, 555)],
        )
        .await;
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

    /// #93: a bucket that still holds abandoned `records/` objects reports exactly
    /// what the broker does — the segments — and never serves the abandoned data.
    /// Those objects held offsets 0..2 before the layout changed; the live log
    /// starts at 10.
    #[tokio::test]
    async fn abandoned_records_objects_are_neither_read_nor_counted() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        // Written by a pre-#199 broker, at a path this reader no longer builds.
        for (base, body) in [(0_i64, BATCH), (1, BATCH2)] {
            let path = object_store::path::Path::from(format!(
                "clusters/c/topics/{SEG_TOPIC}/partitions/0000000000/records/{base:0>20}.batch"
            ));
            store
                .put(&path, PutPayload::from(body.to_vec()))
                .await
                .unwrap();
        }

        // The live log: one segment holding two records at 10..12.
        let region: Vec<u8> = [BATCH, BATCH2].concat();
        put_segment(&store, &src, 0, 3, 1, &[(SEG_TOPIC, 0, 10, 2, &region, 42)]).await;

        let wm = src.watermark(SEG_TOPIC, 0).await.unwrap();
        assert_eq!(
            (wm.low, wm.high, wm.count()),
            (10, 12, 2),
            "log starts at the segment base, not at the abandoned object's 0"
        );

        let records = src
            .fetch(SEG_TOPIC, 0, OffsetSpec::Earliest, 100)
            .await
            .unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [10, 11],
            "nothing below the segment region surfaces"
        );

        // Storage size is the sub-stream's byte span in the segment — the
        // abandoned objects are not the topic's bytes either.
        let sizes = src.topic_storage_bytes(SEG_TOPIC).await.unwrap();
        assert_eq!(sizes.get(&0), Some(&(region.len() as i64)));
    }

    /// #93/#94: `high` comes from the footer even when `watermark.json` is stale or
    /// carries nulls — the shape a live partition actually has, since the object is
    /// never written on the produce path.
    #[tokio::test]
    async fn footer_high_wins_over_a_stale_or_null_watermark_object() {
        use object_store::memory::InMemory;
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        put_watermark(&store, &src, SEG_TOPIC, 0, r#"{"high":null}"#).await;
        let region: Vec<u8> = [BATCH, BATCH2, BATCH3].concat();
        put_segment(&store, &src, 0, 3, 1, &[(SEG_TOPIC, 0, 0, 3, &region, 1)]).await;

        assert_eq!(src.watermark(SEG_TOPIC, 0).await.unwrap().high, 3);

        // A stale stored high (below the tail) must not cap the watermark.
        let src = StorageSource::with_store(store.clone(), "c");
        put_watermark(&store, &src, SEG_TOPIC, 0, r#"{"high":1}"#).await;
        assert_eq!(src.watermark(SEG_TOPIC, 0).await.unwrap().high, 3);
    }

    /// #94: a watermark object still carrying a historic `low` (written before
    /// Popsink/tansu#180 deleted the field) parses, and its value is ignored — the
    /// footers are the log start. Here the oldest segment was expired, so the
    /// stale `low` of 0 would advertise records that no longer exist.
    #[tokio::test]
    async fn historic_low_parses_and_is_ignored() {
        use object_store::memory::InMemory;
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        put_watermark(&store, &src, SEG_TOPIC, 0, r#"{"low":0,"high":null}"#).await;
        // Only the later segment survives retention.
        put_segment(&store, &src, 1, 3, 1, &[(SEG_TOPIC, 0, 50, 2, BATCH, 1)]).await;

        let wm = src.watermark(SEG_TOPIC, 0).await.unwrap();
        assert_eq!(
            (wm.low, wm.high, wm.count()),
            (50, 52, 2),
            "log starts at the surviving segment's base, not the historic 0"
        );
    }

    /// #94: with no live segment the log is empty and starts where it ends — the
    /// persisted high. Reporting `low = 0` there advertises a full log's worth of
    /// messages that no fetch can return.
    #[tokio::test]
    async fn drained_partition_starts_where_it_ends() {
        use object_store::memory::InMemory;
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        // Every segment expired; the assignment floor survives in the watermark.
        put_watermark(&store, &src, SEG_TOPIC, 0, r#"{"high":500}"#).await;

        let wm = src.watermark(SEG_TOPIC, 0).await.unwrap();
        assert_eq!((wm.low, wm.high, wm.count()), (500, 500, 0));
        assert!(src
            .fetch(SEG_TOPIC, 0, OffsetSpec::Earliest, 10)
            .await
            .unwrap()
            .is_empty());
    }

    /// A partition with nothing at all reads as empty, not as an error.
    #[tokio::test]
    async fn partition_with_no_objects_is_empty() {
        use object_store::memory::InMemory;
        let src = StorageSource::with_store(std::sync::Arc::new(InMemory::new()), "c");
        let wm = src.watermark(SEG_TOPIC, 0).await.unwrap();
        assert_eq!((wm.low, wm.high, wm.count()), (0, 0, 0));
    }

    /// #73: the cached high is a monotonic floor — it survives a segment
    /// disappearing, and a segment appended above it still raises the watermark.
    #[tokio::test]
    async fn cached_high_is_a_floor_and_still_catches_a_new_tail() {
        use object_store::{memory::InMemory, ObjectStore};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        put_segment(&store, &src, 0, 3, 1, &[(SEG_TOPIC, 0, 0, 2, BATCH, 1)]).await;
        assert_eq!(src.watermark(SEG_TOPIC, 0).await.unwrap().high, 2); // caches 2

        put_segment(&store, &src, 1, 3, 1, &[(SEG_TOPIC, 0, 2, 3, BATCH2, 2)]).await;
        assert_eq!(
            src.watermark(SEG_TOPIC, 0).await.unwrap().high,
            5,
            "a freshly-listed segment raises the high"
        );

        // Retention takes the tail segment; the high does not go backwards.
        store
            .delete(&src.keys().segment(&Keys::prefix_of(SEG_TOPIC), 1))
            .await
            .unwrap();
        assert_eq!(src.watermark(SEG_TOPIC, 0).await.unwrap().high, 5);
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

    /// Time-seek lands on the first region whose newest record reaches the target,
    /// and answers the log end when none does.
    #[tokio::test]
    async fn seek_time_uses_footer_timestamps() {
        use object_store::memory::InMemory;
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        put_segment(&store, &src, 0, 3, 1, &[(SEG_TOPIC, 0, 0, 2, BATCH, 100)]).await;
        put_segment(&store, &src, 1, 3, 1, &[(SEG_TOPIC, 0, 2, 2, BATCH2, 200)]).await;

        assert_eq!(src.seek_time(SEG_TOPIC, 0, 50).await.unwrap(), 0);
        assert_eq!(src.seek_time(SEG_TOPIC, 0, 150).await.unwrap(), 2);
        assert_eq!(
            src.seek_time(SEG_TOPIC, 0, 999).await.unwrap(),
            4,
            "nothing reaches the target ⇒ the log end"
        );
    }
}
