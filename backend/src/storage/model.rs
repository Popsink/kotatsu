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
    pub base_timestamp: i64,
    pub max_timestamp: i64,
}

impl BatchHeader {
    /// Bytes needed to cover up to `maxTimestamp` (ends at offset 43) in the
    /// Kafka RecordBatch on-disk format.
    pub const PREFIX_LEN: u64 = 43;

    /// Parses the header from at least [`BatchHeader::PREFIX_LEN`] leading bytes.
    /// Field offsets per the Kafka record-batch format:
    /// `baseTimestamp` @ 27, `maxTimestamp` @ 35 (both i64 big-endian).
    pub fn parse(prefix: &[u8]) -> Result<Self, StorageError> {
        if prefix.len() < Self::PREFIX_LEN as usize {
            return Err(StorageError::Decode("batch header too short".into()));
        }
        let be = |o: usize| i64::from_be_bytes(prefix[o..o + 8].try_into().unwrap());
        Ok(Self {
            base_timestamp: be(27),
            max_timestamp: be(35),
        })
    }
}

/// Decodes a `.batch` object into records with **absolute** offsets.
///
/// Critical: the absolute offset comes from the filename's base offset, not the
/// batch's own `base_offset` field (Tansu overwrites the latter). Control
/// batches (transaction markers) are skipped — they carry no user records.
pub fn decode_batch(
    bytes: Bytes,
    base_offset: i64,
    partition: i32,
) -> Result<Vec<DecodedRecord>, StorageError> {
    let deflated =
        deflated::Batch::try_from(bytes).map_err(|e| StorageError::Decode(e.to_string()))?;

    if deflated.is_control() {
        return Ok(Vec::new());
    }

    let inflated =
        inflated::Batch::try_from(deflated).map_err(|e| StorageError::Decode(e.to_string()))?;
    let base_timestamp = inflated.base_timestamp;

    let records = inflated
        .records
        .into_iter()
        .map(|r| DecodedRecord {
            offset: base_offset + r.offset_delta as i64,
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
        })
        .collect();

    Ok(records)
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
    fn batch_header_parses_timestamps() {
        let header = BatchHeader::parse(OFFSET_0).unwrap();
        assert!(header.base_timestamp > 0);
        assert!(header.max_timestamp >= header.base_timestamp);
    }
}
