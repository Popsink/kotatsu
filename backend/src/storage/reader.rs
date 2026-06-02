//! On-demand reading of records from Tansu's S3 storage.
//!
//! Seek model (see issue #9):
//! - Absolute offset = filename base offset + record `offset_delta`.
//! - To read from offset X we use the **predecessor** batch (largest base
//!   offset ≤ X), because X may sit mid-batch.
//! - Time-seek reads batch headers via range GETs; the `watermark.json`
//!   `timestamps` map is always null in S3 storage and is not used.

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use serde::Deserialize;

use super::{
    keys::Keys,
    model::{decode_batch, BatchHeader, DecodedRecord, OffsetSpec, Watermark},
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
    pub async fn watermark(&self, topic: &str, partition: i32) -> Result<Watermark, StorageError> {
        let raw: WatermarkRaw = self
            .get_json(&self.keys().watermark(topic, partition))
            .await?;
        Ok(Watermark {
            low: raw.low.unwrap_or(0),
            high: raw.high.unwrap_or(0),
        })
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

    /// Returns the base offset of the first batch that could contain a record
    /// at or after `target_ts`, found by binary-searching batch headers (each
    /// read with a small range GET — no full-batch downloads).
    ///
    /// Slightly over-inclusive: the returned batch may start with a few records
    /// older than `target_ts`. Returns `high` when no batch reaches the target.
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

        // Leftmost batch whose max_timestamp >= target_ts.
        let (mut lo, mut hi) = (0usize, bases.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            let header = self.batch_header(topic, partition, bases[mid]).await?;
            if header.max_timestamp >= target_ts {
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

    /// Reads just the fixed header of a batch via a range GET.
    async fn batch_header(
        &self,
        topic: &str,
        partition: i32,
        base: i64,
    ) -> Result<BatchHeader, StorageError> {
        let path = self.keys().batch(topic, partition, base);
        let bytes = self
            .store()
            .get_range(&path, 0..BatchHeader::PREFIX_LEN)
            .await
            .map_err(|e| StorageError::from_object(e, &path))?;
        BatchHeader::parse(&bytes)
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
}
