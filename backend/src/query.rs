//! Reusable read-query logic shared by the HTTP API (`api`) and the Python
//! bindings. Keeps message decoding, filtering and the bounded scan in one
//! place so both front-ends behave identically.

use serde_json::{json, Value};

use crate::{
    schema::{decode_field, raw_field, FieldFormat, SchemaRegistry},
    storage::{OffsetSpec, StorageError, StorageSource},
};

/// Maximum records returned in one `messages` call.
pub const MAX_LIMIT: usize = 500;
/// Hard cap on records scanned per filtered `messages` call — keeps the
/// on-demand model honest (no unbounded S3 reads).
pub const MAX_SCAN_CAP: usize = 50_000;
/// Default scan budget when filtering.
pub const DEFAULT_MAX_SCAN: usize = 5000;

/// A read-query error: either a caller mistake (`BadRequest`) or a storage error.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("{0}")]
    BadRequest(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Parses an offset spec string: `earliest`, `latest`, `timestamp:<ms>`, or a
/// concrete offset.
pub fn parse_offset(raw: &str) -> Result<OffsetSpec, QueryError> {
    let bad = || QueryError::BadRequest(format!("invalid offset: {raw}"));
    match raw {
        "earliest" => Ok(OffsetSpec::Earliest),
        "latest" => Ok(OffsetSpec::Latest),
        _ => {
            if let Some(ts) = raw.strip_prefix("timestamp:") {
                ts.parse().map(OffsetSpec::Timestamp).map_err(|_| bad())
            } else {
                raw.parse().map(OffsetSpec::At).map_err(|_| bad())
            }
        }
    }
}

/// User-facing message for an out-of-range partition — names the requested
/// partition and the topic's real count, without exposing any storage layout.
fn partition_out_of_range(partition: i32, partitions: i32) -> String {
    format!(
        "partition {} out of range (topic has {} partition{})",
        partition,
        partitions,
        if partitions == 1 { "" } else { "s" },
    )
}

/// A compiled needle for matching a decoded field's text.
enum Needle {
    Sub(String),
    Re(regex::Regex),
}

impl Needle {
    fn build(raw: &str, regex: bool) -> Result<Self, QueryError> {
        if regex {
            regex::Regex::new(raw)
                .map(Needle::Re)
                .map_err(|e| QueryError::BadRequest(format!("invalid regex: {e}")))
        } else {
            Ok(Needle::Sub(raw.to_lowercase()))
        }
    }
    fn matches(&self, hay: &str) -> bool {
        match self {
            Needle::Sub(s) => hay.to_lowercase().contains(s),
            Needle::Re(r) => r.is_match(hay),
        }
    }
}

/// Extracts the searchable text of a decoded field (`{kind, data, …}` → its data).
fn searchable(field: &Value) -> String {
    match field {
        Value::Null => String::new(),
        Value::Object(o) => match o.get("data") {
            Some(Value::String(s)) => s.clone(),
            Some(v) => v.to_string(),
            None => field.to_string(),
        },
        other => other.to_string(),
    }
}

/// Parameters for a `messages` read (raw, front-end-agnostic).
#[derive(Clone, Debug)]
pub struct MessageParams {
    pub partition: i32,
    /// Offset spec string: `earliest` | `latest` | `timestamp:<ms>` | `<n>`.
    pub offset: String,
    pub limit: usize,
    /// `auto` | `avro` | `json` | `raw`.
    pub key_format: Option<String>,
    pub value_format: Option<String>,
    pub key_contains: Option<String>,
    pub value_contains: Option<String>,
    pub header_key: Option<String>,
    pub header_value: Option<String>,
    pub regex: bool,
    pub max_scan: usize,
}

impl Default for MessageParams {
    fn default() -> Self {
        Self {
            partition: 0,
            offset: "latest".to_string(),
            limit: 50,
            key_format: None,
            value_format: None,
            key_contains: None,
            value_contains: None,
            header_key: None,
            header_value: None,
            regex: false,
            max_scan: DEFAULT_MAX_SCAN,
        }
    }
}

/// Fetches, decodes and filters messages from a topic partition, returning the
/// full response object (`{partition, watermark, count, scanned, filtered,
/// exhausted, records}`). Shared by the HTTP handler and the Python binding.
pub async fn messages(
    source: &StorageSource,
    registry: Option<&SchemaRegistry>,
    topic: &str,
    p: &MessageParams,
) -> Result<Value, QueryError> {
    let spec = parse_offset(&p.offset)?;

    // Validate topic + partition up front so a missing topic or an out-of-range
    // partition returns a clean, distinct error rather than a storage NotFound
    // whose message leaks the internal S3 object key (#63). A missing topic
    // surfaces as `StorageError::TopicNotFound` from `topic_partitions`.
    let partitions = source.topic_partitions(topic).await?;
    if p.partition < 0 || p.partition >= partitions {
        return Err(QueryError::BadRequest(partition_out_of_range(
            p.partition,
            partitions,
        )));
    }

    let limit = p.limit.clamp(1, MAX_LIMIT);
    let key_format = FieldFormat::parse(p.key_format.as_deref());
    let value_format = FieldFormat::parse(p.value_format.as_deref());

    let key_needle = p
        .key_contains
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| Needle::build(s, p.regex))
        .transpose()?;
    let value_needle = p
        .value_contains
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| Needle::build(s, p.regex))
        .transpose()?;
    let header_key = p.header_key.as_deref().filter(|s| !s.is_empty());
    let header_value = p.header_value.as_deref().filter(|s| !s.is_empty());
    let filtering = key_needle.is_some() || value_needle.is_some() || header_key.is_some();

    let scan_limit = if filtering {
        p.max_scan.clamp(1, MAX_SCAN_CAP)
    } else {
        limit
    };

    let watermark = source.watermark(topic, p.partition).await?;
    let records = source.fetch(topic, p.partition, spec, scan_limit).await?;
    let exhausted = records.len() < scan_limit; // fetched fewer than asked ⇒ end of partition

    let mut rendered = Vec::new();
    let mut scanned = 0usize;
    for record in &records {
        scanned += 1;
        let key = decode_field(registry, &record.key, key_format).await;
        let value = decode_field(registry, &record.value, value_format).await;

        if filtering {
            if let Some(n) = &key_needle {
                if !n.matches(&searchable(&key)) {
                    continue;
                }
            }
            if let Some(n) = &value_needle {
                if !n.matches(&searchable(&value)) {
                    continue;
                }
            }
            if let Some(hk) = header_key {
                let hit = record.headers.iter().any(|h| {
                    let k = h.key.as_deref().and_then(|b| std::str::from_utf8(b).ok());
                    k == Some(hk)
                        && match header_value {
                            Some(hv) => h
                                .value
                                .as_deref()
                                .and_then(|b| std::str::from_utf8(b).ok())
                                .is_some_and(|v| v.contains(hv)),
                            None => true,
                        }
                });
                if !hit {
                    continue;
                }
            }
        }

        rendered.push(json!({
            "offset": record.offset,
            "partition": record.partition,
            "timestamp": record.timestamp,
            "key": key,
            "value": value,
            "headers": record.headers.iter().map(|h| json!({
                "key": h.key.as_ref().map(raw_field),
                "value": h.value.as_ref().map(raw_field),
            })).collect::<Vec<_>>(),
        }));
        if rendered.len() >= limit {
            break;
        }
    }

    Ok(json!({
        "partition": p.partition,
        "watermark": watermark,
        "count": rendered.len(),
        "scanned": scanned,
        "filtered": filtering,
        "exhausted": exhausted && scanned == records.len(),
        "records": rendered,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substring_needle_is_case_insensitive() {
        let n = Needle::build("ORDER", false).unwrap();
        assert!(n.matches("my-orders-topic"));
        assert!(!n.matches("events"));
    }

    #[test]
    fn regex_needle_matches_and_rejects_bad() {
        let n = Needle::build("widget-[12]$", true).unwrap();
        assert!(n.matches("widget-1"));
        assert!(!n.matches("widget-3"));
        assert!(Needle::build("[", true).is_err());
    }

    #[test]
    fn searchable_extracts_field_data() {
        assert_eq!(searchable(&Value::Null), "");
        assert_eq!(
            searchable(&json!({"kind": "utf8", "data": "hello"})),
            "hello"
        );
        assert!(searchable(&json!({"kind": "avro", "data": {"id": 3}})).contains("\"id\":3"));
    }

    #[test]
    fn partition_out_of_range_message_is_sanitized_and_pluralized() {
        // Names the partition + real count, never a storage object key (#63).
        let single = partition_out_of_range(999, 1);
        assert_eq!(single, "partition 999 out of range (topic has 1 partition)");
        let many = partition_out_of_range(5, 3);
        assert_eq!(many, "partition 5 out of range (topic has 3 partitions)");
        assert!(!single.contains("watermark") && !single.contains("clusters/"));
    }

    #[test]
    fn parse_offset_variants() {
        assert!(matches!(
            parse_offset("earliest").unwrap(),
            OffsetSpec::Earliest
        ));
        assert!(matches!(
            parse_offset("latest").unwrap(),
            OffsetSpec::Latest
        ));
        assert!(matches!(parse_offset("42").unwrap(), OffsetSpec::At(42)));
        assert!(matches!(
            parse_offset("timestamp:1700").unwrap(),
            OffsetSpec::Timestamp(1700)
        ));
        assert!(parse_offset("nope").is_err());
    }
}
