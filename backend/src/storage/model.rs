//! Types returned by the storage reader, and batch decoding.

use bytes::Bytes;
use serde::Serialize;
use tansu_sans_io::record::{deflated, inflated};

use super::StorageError;

/// Low/high offsets for a partition — what the broker would serve.
///
/// Neither is read from `watermark.json` as a value: `low` was deleted from that
/// object by Popsink/tansu#180 and comes from the segment footers, `high` from the
/// footers floored by the object's persisted `high` (#93, #94).
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Watermark {
    /// Earliest readable offset — the base of the oldest live segment slice,
    /// raised to the truncation floor when `DeleteRecords` has set one (#95). An
    /// empty log starts where it ends, so `low == high` there rather than 0
    /// (Popsink/tansu#299): a 0 would advertise records no fetch can return.
    pub low: i64,
    /// Next offset to be written (log end).
    pub high: i64,
    /// End of what is actually servable, when the last segment expiry certified it
    /// (`served.end`, Popsink/tansu#290) and that certification still describes the
    /// current `high`. `[served_end, high)` is then a gap the expiry destroyed: no
    /// fetch will ever return an offset in it, so it is excluded from
    /// [`Self::count`]. `None` — the normal case — means the whole
    /// `[low, high)` range is servable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub served_end: Option<i64>,
}

impl Watermark {
    /// Approximate message count, excluding a certified-dead gap.
    pub fn count(&self) -> i64 {
        (self.served_end.unwrap_or(self.high) - self.low).max(0)
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
/// whole batch.
///
/// Nothing on the read path needs it any more — the segment footer carries every
/// sub-stream's offset span and newest timestamp, so neither the high watermark
/// nor time-seek reads a batch header (#93). Kept as the wire-format reference
/// this module documents itself against.
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

/// Splits a segment region into its constituent sub-batch byte slices.
///
/// A region holds several record batches concatenated (a `deflated::Frame`), each
/// on the wire as `base_offset (i64) + batch_length (i32) + batch_length bytes`
/// (`12 + batch_length` total); a single-batch region is a one-element frame. A
/// trailing run that does not form a whole batch is ignored, mirroring
/// `deflated::Batch::try_from`.
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

/// Decodes a segment region into records with **absolute** offsets.
///
/// A region is one `(topic, partition)` sub-stream's record batches
/// concatenated, so it may hold one batch or many; both are handled.
///
/// Critical: the absolute offset comes from the footer's base offset for the
/// region, not the batch's own `base_offset` field (Tansu overwrites the latter).
/// Within a region the running base advances by each sub-batch's offset span
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
    fn split_frame_walks_every_sub_batch() {
        // Three single-record sub-batches ⇒ three slices, and a single-batch
        // region is a one-element frame.
        assert_eq!(
            split_frame(&coalesced(&[OFFSET_0, OFFSET_2, OFFSET_4])).len(),
            3
        );
        assert_eq!(split_frame(&Bytes::from_static(OFFSET_0)).len(), 1);
        // A trailing run that cannot form a whole batch is ignored.
        assert_eq!(
            split_frame(&coalesced(&[OFFSET_0, &OFFSET_2[..8]])).len(),
            1
        );
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
