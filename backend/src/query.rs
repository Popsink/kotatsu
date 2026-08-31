//! Reusable read-query logic shared by the HTTP API (`api`) and the Python
//! bindings. Keeps message decoding, filtering and the bounded scan in one
//! place so both front-ends behave identically.

use futures::stream::{StreamExt, TryStreamExt};
use serde_json::{json, Value};

use crate::{
    schema::{decode_field, raw_field, FieldFormat, SchemaRegistry},
    storage::{DecodedRecord, OffsetSpec, StorageError, StorageSource, Watermark},
};

/// Maximum records returned in one `messages` call.
pub const MAX_LIMIT: usize = 500;
/// Hard cap on records scanned per filtered `messages` call — keeps the
/// on-demand model honest (no unbounded S3 reads).
pub const MAX_SCAN_CAP: usize = 50_000;
/// Default scan budget when filtering.
pub const DEFAULT_MAX_SCAN: usize = 5000;
/// How many partitions a `partition=all` read may hold ranged GETs open for at
/// once. A 200-partition topic must not open 200 simultaneous reads.
const FANOUT: usize = 8;

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

/// The decode formats and match predicates of one read, built once and shared
/// by every partition of a fan-out.
struct Filters<'a> {
    key_format: FieldFormat,
    value_format: FieldFormat,
    key_needle: Option<Needle>,
    value_needle: Option<Needle>,
    header_key: Option<&'a str>,
    header_value: Option<&'a str>,
    /// Whether any predicate is set — decides scan budget and the `filtered` flag.
    filtering: bool,
}

impl<'a> Filters<'a> {
    fn build(p: &'a MessageParams) -> Result<Self, QueryError> {
        let needle = |raw: &Option<String>| -> Result<Option<Needle>, QueryError> {
            raw.as_deref()
                .filter(|s| !s.is_empty())
                .map(|s| Needle::build(s, p.regex))
                .transpose()
        };
        let key_needle = needle(&p.key_contains)?;
        let value_needle = needle(&p.value_contains)?;
        let header_key = p.header_key.as_deref().filter(|s| !s.is_empty());
        let header_value = p.header_value.as_deref().filter(|s| !s.is_empty());

        Ok(Self {
            key_format: FieldFormat::parse(p.key_format.as_deref()),
            value_format: FieldFormat::parse(p.value_format.as_deref()),
            filtering: key_needle.is_some() || value_needle.is_some() || header_key.is_some(),
            key_needle,
            value_needle,
            header_key,
            header_value,
        })
    }

    fn matches(&self, key: &Value, value: &Value, record: &DecodedRecord) -> bool {
        if let Some(n) = &self.key_needle {
            if !n.matches(&searchable(key)) {
                return false;
            }
        }
        if let Some(n) = &self.value_needle {
            if !n.matches(&searchable(value)) {
                return false;
            }
        }
        if let Some(hk) = self.header_key {
            let hit = record.headers.iter().any(|h| {
                let k = h.key.as_deref().and_then(|b| std::str::from_utf8(b).ok());
                k == Some(hk)
                    && match self.header_value {
                        Some(hv) => h
                            .value
                            .as_deref()
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .is_some_and(|v| v.contains(hv)),
                        None => true,
                    }
            });
            if !hit {
                return false;
            }
        }
        true
    }
}

/// Parameters for a `messages` read (raw, front-end-agnostic).
#[derive(Clone, Debug)]
pub struct MessageParams {
    /// Partition spec: `all` (every partition, merged) or a concrete number.
    pub partition: String,
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
            partition: "all".to_string(),
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

/// The set of partitions a message read covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionSelector {
    /// Every partition of the topic, merged newest-first (#102).
    All,
    /// One partition, in storage order — the historical behaviour.
    One(i32),
}

/// Parses a partition spec: `all`, or a concrete partition number.
pub fn parse_partition(raw: &str) -> Result<PartitionSelector, QueryError> {
    match raw {
        "all" => Ok(PartitionSelector::All),
        _ => raw.parse().map(PartitionSelector::One).map_err(|_| {
            QueryError::BadRequest(format!(
                "invalid partition: {raw} (expected a partition number or 'all')"
            ))
        }),
    }
}

/// Whether a scan that filled its budget nonetheless covered the whole partition.
///
/// Which edge proves it depends on the direction of the read. `latest` returns the
/// *last* records, so it touches the log end on its very first one — checking that
/// end would call every `latest` read complete, however little of the partition it
/// actually looked at. Reading backwards, only reaching the log start proves
/// coverage; reading forward, only reaching the served end does.
fn covers_partition(spec: OffsetSpec, first: i64, last: i64, low: i64, served_end: i64) -> bool {
    match spec {
        OffsetSpec::Latest => first <= low,
        _ => last + 1 >= served_end,
    }
}

/// What one partition contributed to a read.
struct PartitionScan {
    partition: i32,
    watermark: Watermark,
    rendered: Vec<Value>,
    scanned: usize,
    exhausted: bool,
}

/// The decoded, filtered records of a single partition, bounded by `budget`.
#[allow(clippy::too_many_arguments)]
async fn scan_partition(
    source: &StorageSource,
    registry: Option<&SchemaRegistry>,
    topic: &str,
    partition: i32,
    spec: OffsetSpec,
    budget: usize,
    limit: usize,
    f: &Filters<'_>,
) -> Result<PartitionScan, QueryError> {
    let watermark = source.watermark(topic, partition).await?;
    let records = source.fetch(topic, partition, spec, budget).await?;
    // Fetched fewer than asked ⇒ we reached the end. Reading *exactly* the budget
    // is no longer rare once it is split across partitions (#102), so also check
    // whether the records we got happen to span to the partition's edge.
    let served_end = watermark.served_end.unwrap_or(watermark.high);
    let reached_end = records.len() < budget
        || match (records.first(), records.last()) {
            (Some(f), Some(l)) => {
                covers_partition(spec, f.offset, l.offset, watermark.low, served_end)
            }
            _ => true,
        };

    let mut rendered = Vec::new();
    let mut scanned = 0usize;
    for record in &records {
        scanned += 1;
        let key = decode_field(registry, &record.key, f.key_format).await;
        let value = decode_field(registry, &record.value, f.value_format).await;

        if f.filtering && !f.matches(&key, &value, record) {
            continue;
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

    Ok(PartitionScan {
        partition,
        watermark,
        scanned,
        // A short `scanned` means the `limit` break fired, so records went unlooked-at.
        exhausted: reached_end && scanned == records.len(),
        rendered,
    })
}

/// How many records each partition of a fan-out may read.
///
/// The budget belongs to the topic, not to each partition: splitting it is what
/// keeps a 12-partition search from costing twelve times a single-partition one.
/// Every partition still gets at least one record so a wide topic with a small
/// limit still reports from all of them — the overshoot is then bounded by the
/// partition count, never by a multiple of the budget.
fn partition_budget(scan_limit: usize, partitions: usize) -> usize {
    (scan_limit / partitions.max(1)).max(1)
}

/// Sort key for the cross-partition merge. Timestamps are not globally ordered
/// across partitions in Kafka, so this is a best-effort ordering, not a total
/// one — the response says so, and the UI must not present it as authoritative.
fn merge_key(record: &Value) -> (i64, i64, i64) {
    let n = |k: &str| record.get(k).and_then(Value::as_i64).unwrap_or(0);
    (n("timestamp"), n("partition"), n("offset"))
}

/// Fetches, decodes and filters messages from a topic, returning the full
/// response object. Shared by the HTTP handler and the Python binding.
///
/// `params.partition` is either one partition — storage order, today's response
/// shape — or `all`, which fans out over every partition concurrently and merges
/// newest-first (#102). The scan budget is topic-wide in both cases: a 12-partition
/// search spends one budget, not twelve, give or take a record per partition.
///
/// That budget is what makes `all` approximate, and deliberately so. Each partition
/// is read up to its share, so with a `limit` small next to the partition count the
/// answer is "the newest few of every partition", not provably "the newest `limit`
/// of the topic" — proving that would mean reading `limit` from every partition,
/// which is the N× cost the budget exists to prevent. `order_best_effort` marks it.
pub async fn messages(
    source: &StorageSource,
    registry: Option<&SchemaRegistry>,
    topic: &str,
    p: &MessageParams,
) -> Result<Value, QueryError> {
    let spec = parse_offset(&p.offset)?;
    let selector = parse_partition(&p.partition)?;

    // Validate topic + partition up front so a missing topic or an out-of-range
    // partition returns a clean, distinct error rather than a storage NotFound
    // whose message leaks the internal S3 object key (#63). A missing topic
    // surfaces as `StorageError::TopicNotFound` from `topic_partitions`.
    let partitions = source.topic_partitions(topic).await?;
    let targets: Vec<i32> = match selector {
        PartitionSelector::One(n) => {
            if n < 0 || n >= partitions {
                return Err(QueryError::BadRequest(partition_out_of_range(
                    n, partitions,
                )));
            }
            vec![n]
        }
        PartitionSelector::All => {
            // Offset 42 names a different record in every partition, so a concrete
            // offset only means something against one of them.
            if matches!(spec, OffsetSpec::At(_)) {
                return Err(QueryError::BadRequest(
                    "offset=<n> needs a single partition; with partition=all use \
                     earliest, latest or timestamp:<ms>"
                        .to_string(),
                ));
            }
            (0..partitions).collect()
        }
    };

    let limit = p.limit.clamp(1, MAX_LIMIT);
    let filters = Filters::build(p)?;
    let scan_limit = if filters.filtering {
        p.max_scan.clamp(1, MAX_SCAN_CAP)
    } else {
        limit
    };
    let budget = partition_budget(scan_limit, targets.len());

    let (source, registry, topic, filters) = (source, registry, topic, &filters);
    let mut scans: Vec<PartitionScan> = futures::stream::iter(targets.iter().copied())
        .map(|partition| async move {
            scan_partition(
                source, registry, topic, partition, spec, budget, limit, filters,
            )
            .await
        })
        .buffer_unordered(FANOUT)
        .try_collect()
        .await?;
    scans.sort_by_key(|s| s.partition);

    let scanned: usize = scans.iter().map(|s| s.scanned).sum();
    let all_exhausted = scans.iter().all(|s| s.exhausted);

    if let PartitionSelector::One(n) = selector {
        // One partition: the response shape and record order are unchanged.
        let scan = scans.remove(0);
        return Ok(json!({
            "partition": n,
            "watermark": scan.watermark,
            "count": scan.rendered.len(),
            "scanned": scanned,
            "filtered": filters.filtering,
            "exhausted": all_exhausted,
            "records": scan.rendered,
        }));
    }

    let mut records: Vec<Value> = scans
        .iter()
        .flat_map(|s| s.rendered.iter().cloned())
        .collect();
    records.sort_by_key(|r| std::cmp::Reverse(merge_key(r)));
    let truncated = records.len() > limit;
    records.truncate(limit);

    Ok(json!({
        "partitions": scans.iter().map(|s| json!({
            "partition": s.partition,
            "watermark": s.watermark,
            "scanned": s.scanned,
            "exhausted": s.exhausted,
        })).collect::<Vec<_>>(),
        "count": records.len(),
        "scanned": scanned,
        "filtered": filters.filtering,
        "exhausted": all_exhausted && !truncated,
        "order": "timestamp_desc",
        "order_best_effort": true,
        "records": records,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `latest` read starts at the log end, so the end proves nothing about how
    /// much of the partition it saw. Getting this backwards makes every capped
    /// `latest` scan claim it was complete.
    #[test]
    fn a_latest_scan_is_only_covered_once_it_reaches_the_log_start() {
        // Read the last record of a 6-record partition: 5 more remain below.
        assert!(!covers_partition(OffsetSpec::Latest, 5, 5, 0, 6));
        // Read all six.
        assert!(covers_partition(OffsetSpec::Latest, 0, 5, 0, 6));
        // A truncated partition starts at its floor, not at 0.
        assert!(covers_partition(OffsetSpec::Latest, 2, 5, 2, 6));
    }

    #[test]
    fn a_forward_scan_is_covered_once_it_reaches_the_served_end() {
        assert!(covers_partition(OffsetSpec::Earliest, 0, 5, 0, 6));
        assert!(!covers_partition(OffsetSpec::Earliest, 0, 3, 0, 6));
        // `served_end` below `high` is retention having destroyed the tail (#95):
        // reaching it is as far as any read can go.
        assert!(covers_partition(OffsetSpec::Earliest, 0, 3, 0, 4));
    }

    #[test]
    fn partition_spec_accepts_all_and_numbers() {
        assert_eq!(parse_partition("all").unwrap(), PartitionSelector::All);
        assert_eq!(parse_partition("0").unwrap(), PartitionSelector::One(0));
        assert_eq!(parse_partition("11").unwrap(), PartitionSelector::One(11));
    }

    #[test]
    fn partition_spec_rejects_nonsense_by_name() {
        let err = parse_partition("some").unwrap_err().to_string();
        assert!(err.contains("some"), "{err}");
        assert!(err.contains("'all'"), "{err}");
    }

    /// #102's central constraint: a topic-wide search must not cost N times a
    /// single-partition one.
    #[test]
    fn the_scan_budget_is_topic_wide_not_per_partition() {
        for partitions in [1usize, 2, 3, 12, 200] {
            let total = partition_budget(5000, partitions) * partitions;
            assert!(
                total <= 5000 + partitions,
                "{partitions} partitions read {total} records against a 5000 budget"
            );
        }
        assert_eq!(partition_budget(5000, 1), 5000);
        assert_eq!(partition_budget(5000, 10), 500);
    }

    #[test]
    fn every_partition_reads_at_least_one_record() {
        // A 60-partition topic with limit 50 would otherwise leave most of them
        // unread, and their records could never appear in the merge.
        assert_eq!(partition_budget(50, 60), 1);
        assert_eq!(partition_budget(0, 4), 1);
    }

    #[test]
    fn the_merge_puts_the_newest_record_first() {
        let rec = |ts: i64, part: i64, off: i64| json!({ "timestamp": ts, "partition": part, "offset": off });
        let mut records = [rec(100, 3, 12), rec(205, 3, 13), rec(140, 0, 88)];
        records.sort_by_key(|r| std::cmp::Reverse(merge_key(r)));
        let order: Vec<i64> = records
            .iter()
            .map(|r| r["timestamp"].as_i64().unwrap())
            .collect();
        assert_eq!(order, vec![205, 140, 100]);
    }

    #[test]
    fn the_merge_breaks_timestamp_ties_deterministically() {
        let rec = |part: i64, off: i64| json!({ "timestamp": 7, "partition": part, "offset": off });
        let mut records = [rec(0, 5), rec(2, 1), rec(0, 9)];
        records.sort_by_key(|r| std::cmp::Reverse(merge_key(r)));
        let order: Vec<(i64, i64)> = records
            .iter()
            .map(|r| {
                (
                    r["partition"].as_i64().unwrap(),
                    r["offset"].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(order, vec![(2, 1), (0, 9), (0, 5)]);
    }

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
