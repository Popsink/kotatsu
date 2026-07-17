//! Types returned by the storage reader, and batch decoding.

use bytes::Bytes;
use serde::Serialize;
use tansu_sans_io::record::{deflated, inflated};

use super::StorageError;

/// Low/high offsets for a partition, from `watermark.json`.
///
/// Tansu stores `{ low, high, timestamps }`; `timestamps` is always `null` in
/// the S3 storage engine, so we don't model it (time-seek uses batch headers).
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Watermark {
    /// Earliest available offset (log start). `null` in the file ⇒ 0.
    pub low: i64,
    /// Next offset to be written (high watermark). `null` ⇒ 0.
    pub high: i64,
}

impl Watermark {
    /// Approximate message count.
    pub fn count(&self) -> i64 {
        (self.high - self.low).max(0)
    }
}

/// Where to start reading from in a partition.
#[derive(Clone, Copy, Debug)]
pub enum OffsetSpec {
    /// The low watermark.
    Earliest,
    /// The tail — the last `limit` records.
    Latest,
    /// A specific offset (clamped to `[low, high]`).
    At(i64),
    /// The first batch whose records reach this Unix-millis timestamp.
    Timestamp(i64),
}

/// A single decoded record, with its absolute offset.
#[derive(Clone, Debug, Serialize)]
pub struct DecodedRecord {
    pub offset: i64,
    pub partition: i32,
    /// Unix-millis (batch `base_timestamp` + record `timestamp_delta`).
    pub timestamp: i64,
    #[serde(serialize_with = "ser_opt_bytes")]
    pub key: Option<Bytes>,
    #[serde(serialize_with = "ser_opt_bytes")]
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecordHeader {
    #[serde(serialize_with = "ser_opt_bytes")]
    pub key: Option<Bytes>,
    #[serde(serialize_with = "ser_opt_bytes")]
    pub value: Option<Bytes>,
}

/// The fixed-size prefix of a Kafka record batch, parsed without decoding the
/// whole batch — used by time-seek via a range GET.
#[derive(Clone, Copy, Debug)]
pub struct BatchHeader {
    /// Offset of the last record in the batch, relative to `baseOffset`. The
    /// batch's exclusive end offset is `base + last_offset_delta + 1`.
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
}

impl BatchHeader {
    /// Bytes needed to cover up to `maxTimestamp` (ends at offset 43) in the
    /// Kafka RecordBatch on-disk format.
    pub const PREFIX_LEN: u64 = 43;

    /// Parses the header from at least [`BatchHeader::PREFIX_LEN`] leading bytes.
    /// Field offsets per the Kafka record-batch format:
    /// `lastOffsetDelta` @ 23 (i32), `baseTimestamp` @ 27, `maxTimestamp` @ 35
    /// (both i64), all big-endian.
    pub fn parse(prefix: &[u8]) -> Result<Self, StorageError> {
        if prefix.len() < Self::PREFIX_LEN as usize {
            return Err(StorageError::Decode("batch header too short".into()));
        }
        let be64 = |o: usize| i64::from_be_bytes(prefix[o..o + 8].try_into().unwrap());
        let be32 = |o: usize| i32::from_be_bytes(prefix[o..o + 4].try_into().unwrap());
        Ok(Self {
            last_offset_delta: be32(23),
            base_timestamp: be64(27),
            max_timestamp: be64(35),
        })
    }
}

/// Splits a `.batch` object into its constituent sub-batch byte slices.
///
/// With Tansu's produce coalescing (#50) one object may hold several record
/// batches concatenated (a `deflated::Frame`), each on the wire as
/// `base_offset (i64) + batch_length (i32) + batch_length bytes`
/// (`12 + batch_length` total). A legacy or non-coalesced object is a
/// one-element frame. A trailing run that does not form a whole batch is
/// ignored, mirroring `deflated::Batch::try_from`.
fn split_frame(bytes: &Bytes) -> Vec<Bytes> {
    // base_offset (i64) + batch_length (i32) precede the `batch_length` body.
    const PREFIX: usize = 12;

    let mut slices = Vec::new();
    let mut offset = 0usize;
    while offset + PREFIX <= bytes.len() {
        let len = i32::from_be_bytes(bytes[offset + 8..offset + PREFIX].try_into().unwrap());
        let total = match usize::try_from(len) {
            Ok(len) => PREFIX + len,
            Err(_) => break,
        };
        if offset + total > bytes.len() {
            break;
        }
        slices.push(bytes.slice(offset..offset + total));
        offset += total;
    }
    slices
}

/// Total offset span of a frame: `Σ (last_offset_delta + 1)` over its
/// sub-batches. This is the number of offsets the object occupies (which after
/// compaction can exceed the record count), used to derive the high watermark
/// of a coalesced tail object. Parses only fixed batch headers — no inflation.
pub fn frame_offset_span(bytes: &Bytes) -> i64 {
    split_frame(bytes)
        .iter()
        .filter_map(|slice| BatchHeader::parse(slice).ok())
        .map(|h| h.last_offset_delta as i64 + 1)
        .sum()
}

/// Largest `maxTimestamp` over a frame's sub-batches — the object's newest
/// record time, used by frame-aware time-seek. A coalesced object's newest
/// record sits in its last sub-batch, so the first header alone under-reports
/// it. Returns `None` for a frame with no parseable sub-batch.
pub fn frame_max_timestamp(bytes: &Bytes) -> Option<i64> {
    split_frame(bytes)
        .iter()
        .filter_map(|slice| BatchHeader::parse(slice).ok())
        .map(|h| h.max_timestamp)
        .max()
}

/// Decodes a `.batch` object into records with **absolute** offsets.
///
/// An object may hold a single batch (legacy / coalescing off) or several
/// batches concatenated (Tansu coalescing, #50); both are handled — a
/// single-batch object yields byte-for-byte the same records as before.
///
/// Critical: the absolute offset comes from the filename's base offset, not the
/// batch's own `base_offset` field (Tansu overwrites the latter). Within a
/// coalesced object the running base advances by each sub-batch's offset span
/// (`last_offset_delta + 1`), so the second sub-batch starts where the first
/// ended. Control batches (transaction markers) are skipped individually but
/// still advance the running base — they occupy offsets.
pub fn decode_batch(
    bytes: Bytes,
    base_offset: i64,
    partition: i32,
) -> Result<Vec<DecodedRecord>, StorageError> {
    let mut out = Vec::new();
    let mut sub_base = base_offset;

    for slice in split_frame(&bytes) {
        let deflated =
            deflated::Batch::try_from(slice).map_err(|e| StorageError::Decode(e.to_string()))?;
        // The offset span of this sub-batch, captured before `deflated` is
        // consumed by inflation. Advances the running base even for control
        // batches, which occupy offsets but carry no user records.
        let span = deflated.last_offset_delta as i64 + 1;

        if deflated.is_control() {
            sub_base += span;
            continue;
        }

        let inflated =
            inflated::Batch::try_from(deflated).map_err(|e| StorageError::Decode(e.to_string()))?;
        let base_timestamp = inflated.base_timestamp;

        out.extend(inflated.records.into_iter().map(|r| {
            DecodedRecord {
                offset: sub_base + r.offset_delta as i64,
                partition,
                timestamp: base_timestamp + r.timestamp_delta,
                key: r.key,
                value: r.value,
                headers: r
                    .headers
                    .into_iter()
                    .map(|h| RecordHeader {
                        key: h.key,
                        value: h.value,
                    })
                    .collect(),
            }
        }));

        sub_base += span;
    }

    Ok(out)
}

/// Serializes optional bytes as a UTF-8 string when valid, else hex — matching
/// how the UI shows keys/values (#7).
fn ser_opt_bytes<S>(bytes: &Option<Bytes>, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    match bytes {
        None => s.serialize_none(),
        Some(b) => {
            let mut map = s.serialize_map(Some(2))?;
            match std::str::from_utf8(b) {
                Ok(text) => {
                    map.serialize_entry("kind", "utf8")?;
                    map.serialize_entry("data", text)?;
                }
                Err(_) => {
                    map.serialize_entry("kind", "hex")?;
                    map.serialize_entry("data", &hex(b))?;
                }
            }
            map.end()
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real `.batch` objects produced by Tansu (one message per batch).
    const OFFSET_0: &[u8] = include_bytes!("../../tests/fixtures/offset-0.batch");
    const OFFSET_2: &[u8] = include_bytes!("../../tests/fixtures/offset-2.batch");
    const OFFSET_4: &[u8] = include_bytes!("../../tests/fixtures/offset-4.batch");

    /// Concatenates single-batch objects into one coalesced-frame object, the
    /// exact wire layout Tansu's coalescing produce path writes (#50).
    fn coalesced(parts: &[&[u8]]) -> Bytes {
        let mut buf = Vec::new();
        for part in parts {
            buf.extend_from_slice(part);
        }
        Bytes::from(buf)
    }

    fn key_str(r: &DecodedRecord) -> String {
        String::from_utf8(r.key.as_ref().unwrap().to_vec()).unwrap()
    }
    fn value_str(r: &DecodedRecord) -> String {
        String::from_utf8(r.value.as_ref().unwrap().to_vec()).unwrap()
    }

    #[test]
    fn decodes_real_batch() {
        let records = decode_batch(Bytes::from_static(OFFSET_0), 0, 0).unwrap();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.offset, 0);
        assert_eq!(r.partition, 0);
        assert_eq!(key_str(r), "key-1");
        assert_eq!(value_str(r), r#"{"id":1,"item":"widget-1"}"#);
        assert!(r.timestamp > 0);
    }

    #[test]
    fn second_batch_decodes_with_its_message() {
        let records = decode_batch(Bytes::from_static(OFFSET_2), 2, 0).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 2);
        assert_eq!(key_str(&records[0]), "key-3");
    }

    #[test]
    fn absolute_offset_comes_from_filename_base_not_the_batch() {
        // The pitfall: even with a base offset that doesn't match the batch's
        // own `base_offset` field, the absolute offset must follow the argument.
        let records = decode_batch(Bytes::from_static(OFFSET_0), 99, 0).unwrap();
        assert_eq!(records[0].offset, 99);
    }

    #[test]
    fn decodes_coalesced_frame_with_running_absolute_offsets() {
        // Two single-record batches concatenated = one coalesced object, named
        // by the first batch's base offset. Each sub-batch spans one offset, so
        // the second record sits at object_base + 1 — driven by the running
        // base, not either fixture's own `base_offset` field.
        let records = decode_batch(coalesced(&[OFFSET_0, OFFSET_2]), 0, 0).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, 1);
        assert_eq!(key_str(&records[0]), "key-1");
        assert_eq!(key_str(&records[1]), "key-3");
        assert!(records[1].timestamp > 0);
    }

    #[test]
    fn coalesced_frame_offsets_run_from_the_object_base() {
        let records = decode_batch(coalesced(&[OFFSET_0, OFFSET_2, OFFSET_4]), 100, 0).unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [100, 101, 102]
        );
    }

    #[test]
    fn frame_offset_span_sums_sub_batches() {
        // Three single-record sub-batches ⇒ span 3.
        assert_eq!(
            frame_offset_span(&coalesced(&[OFFSET_0, OFFSET_2, OFFSET_4])),
            3
        );
        // A legacy single-batch object is a one-element frame ⇒ span 1.
        assert_eq!(frame_offset_span(&Bytes::from_static(OFFSET_0)), 1);
    }

    #[test]
    fn frame_max_timestamp_covers_the_latest_sub_batch() {
        let single = frame_max_timestamp(&Bytes::from_static(OFFSET_0)).unwrap();
        let frame = frame_max_timestamp(&coalesced(&[OFFSET_0, OFFSET_2])).unwrap();
        // The frame's newest record is at least as new as its first sub-batch's.
        assert!(frame >= single);
    }

    #[test]
    fn batch_header_parses_timestamps() {
        let header = BatchHeader::parse(OFFSET_0).unwrap();
        assert!(header.base_timestamp > 0);
        assert!(header.max_timestamp >= header.base_timestamp);
        // Single-record batch ⇒ last record sits at base + 0.
        assert_eq!(header.last_offset_delta, 0);
    }
}
