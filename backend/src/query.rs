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
    ///
    /// With `cursor` set this names only the *direction* of travel; the cursor
    /// supplies each partition's starting point.
    pub offset: String,
    /// Resume points from a previous page's `resume`, as `0:412,3:998` (#104).
    pub cursor: Option<String>,
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
            cursor: None,
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

/// Which way a read walks a partition.
///
/// `latest` starts at the log end and every further page is *older*; every other
/// spec starts somewhere and walks *newer*. Four things turn on the sense — which
/// edge proves coverage, which end of the fetched window is scanned first, where
/// the next page resumes, and which end of the merge survives truncation — so it
/// is named once rather than re-derived from the spec at each site.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    /// Towards older offsets: `latest`.
    Backward,
    /// Towards newer offsets: `earliest`, `timestamp:<ms>`, `<n>`.
    Forward,
}

impl Direction {
    fn of(spec: OffsetSpec) -> Self {
        match spec {
            OffsetSpec::Latest => Direction::Backward,
            _ => Direction::Forward,
        }
    }

    /// The offset a page resumes from, given the last one it accounted for.
    fn past(self, edge: i64) -> i64 {
        match self {
            Direction::Forward => edge + 1,
            Direction::Backward => edge - 1,
        }
    }
}

/// Parses a resume cursor: `0:412,3:998` — one offset per partition.
///
/// A fan-out cannot be resumed by one `offset`: partition 0 and partition 3 stop
/// in different places (#104). So a continuation carries a point per partition,
/// and `offset` is left naming only the *direction* of travel. A partition the
/// previous page exhausted is simply absent, and is not read again.
pub fn parse_cursor(raw: &str) -> Result<Vec<(i32, i64)>, QueryError> {
    let bad = |pair: &str| {
        QueryError::BadRequest(format!(
            "invalid cursor: {pair} (expected comma-separated `partition:offset` pairs)"
        ))
    };
    let points: Vec<(i32, i64)> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let (p, o) = pair.split_once(':').ok_or_else(|| bad(pair))?;
            Ok((
                p.trim().parse().map_err(|_| bad(pair))?,
                o.trim().parse().map_err(|_| bad(pair))?,
            ))
        })
        .collect::<Result<_, QueryError>>()?;

    // Two starting points for one partition would scan it twice and return its
    // records twice.
    for (i, (part, _)) in points.iter().enumerate() {
        if points[..i].iter().any(|(seen, _)| seen == part) {
            return Err(QueryError::BadRequest(format!(
                "invalid cursor: partition {part} named twice"
            )));
        }
    }
    Ok(points)
}

/// What one partition contributed to a read.
struct PartitionScan {
    partition: i32,
    watermark: Watermark,
    rendered: Vec<Value>,
    scanned: usize,
    /// The furthest offset the scan actually looked at, in the read direction —
    /// the frontier a partition that matched nothing resumes from (#104).
    frontier: Option<i64>,
}

/// Where one partition's scan begins, and where a backward page must stop.
#[derive(Clone, Copy, Debug)]
struct Window {
    spec: OffsetSpec,
    /// Reading backwards, the newest offset this page may include. `fetch` only
    /// ever walks *forward* from a computed start, so a continuation window that
    /// bumps into the log floor would otherwise run back over the page already
    /// shown; the ceiling trims it. `None` for a forward read.
    ceiling: Option<i64>,
}

/// The decoded, filtered records of a single partition, bounded by `budget`.
#[allow(clippy::too_many_arguments)]
async fn scan_partition(
    source: &StorageSource,
    registry: Option<&SchemaRegistry>,
    topic: &str,
    partition: i32,
    win: Window,
    dir: Direction,
    budget: usize,
    limit: usize,
    f: &Filters<'_>,
) -> Result<PartitionScan, QueryError> {
    let watermark = source.watermark(topic, partition).await?;
    let mut records = source.fetch(topic, partition, win.spec, budget).await?;
    if let Some(ceiling) = win.ceiling {
        records.retain(|r| r.offset <= ceiling);
    }
    // Scan in the direction of travel. `fetch` always hands back ascending offsets,
    // so a backward read walks them in reverse — otherwise the `limit` break below
    // would keep the *oldest* matches of a window the caller asked the newest of,
    // and the records past the break could never be reached by a further page.
    let ordered: Vec<&DecodedRecord> = match dir {
        Direction::Forward => records.iter().collect(),
        Direction::Backward => records.iter().rev().collect(),
    };

    let mut rendered = Vec::new();
    let mut scanned = 0usize;
    let mut frontier = None;
    for record in ordered {
        scanned += 1;
        frontier = Some(record.offset);
        let key = decode_field(registry, &record.key, f.key_format).await;
        let value = decode_field(registry, &record.value, f.value_format).await;

        if f.filtering && !f.matches(&key, &value, record) {
            continue;
        }

        rendered.push(json!({
            "offset": record.offset,
            "partition": record.partition,
            "timestamp": record.timestamp,
            "size": record_size(record),
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
        frontier,
        rendered,
    })
}

/// The record's **serialized** size: the bytes of its key, value and headers as
/// they sit in the batch (#108).
///
/// Not its share of the topic's `storage_bytes`, which is compressed segment
/// space and a property of the segment, not of one record — there is no honest
/// way to attribute it per record. The two numbers differ by the compression
/// ratio, so the UI labels this one for what it is.
///
/// A field absent from the record contributes nothing, which is what `null` on
/// the wire costs: Kafka encodes it as a length of -1, not as an empty payload.
fn record_size(record: &DecodedRecord) -> usize {
    let len = |b: &Option<bytes::Bytes>| b.as_ref().map_or(0, |v| v.len());
    len(&record.key)
        + len(&record.value)
        + record
            .headers
            .iter()
            .map(|h| len(&h.key) + len(&h.value))
            .sum::<usize>()
}

/// How many records each partition of a fan-out may read.
///
/// The *scan* budget belongs to the topic, not to each partition: splitting it is
/// what keeps a 12-partition filtered search from costing twelve times a
/// single-partition one. Every partition still gets at least one record so a wide
/// topic with a small budget still reports from all of them — the overshoot is
/// then bounded by the partition count, never by a multiple of the budget.
fn partition_budget(scan_limit: usize, partitions: usize) -> usize {
    (scan_limit / partitions.max(1)).max(1)
}

/// How many records each partition may read when nothing is being filtered.
///
/// `limit`, not a share of it. Dividing the page across partitions made a page
/// stop being a page: on a topic whose records sit in one partition — the usual
/// shape, since the sticky partitioner concentrates keyless writes — asking for
/// 50 on 12 partitions returned 4. #102's budget rule is about `max_scan` and the
/// S3 amplification of a *filtered* scan; an unfiltered read is capped at
/// `MAX_LIMIT` records per partition, which is a different order of cost. The
/// topic-wide cap still applies, so a 200-partition topic does not read 200 pages.
fn page_budget(limit: usize, partitions: usize) -> usize {
    limit.min(partition_budget(MAX_SCAN_CAP, partitions))
}

/// Sort key for the cross-partition merge. Timestamps are not globally ordered
/// across partitions in Kafka, so this is a best-effort ordering, not a total
/// one — the response says so, and the UI must not present it as authoritative.
fn merge_key(record: &Value) -> (i64, i64, i64) {
    let n = |k: &str| record.get(k).and_then(Value::as_i64).unwrap_or(0);
    (n("timestamp"), n("partition"), n("offset"))
}

/// Orders the merged records the way the read travels, then keeps one page.
///
/// The sense matters because of the truncation: a descending sort keeps the newest
/// `limit`, which is right for `latest` and wrong for `earliest` — there it would
/// throw away exactly the oldest records the caller asked for, and page two could
/// then hold records *newer* than page one (#104).
fn merge_page(records: &mut Vec<Value>, dir: Direction, limit: usize) {
    match dir {
        Direction::Backward => records.sort_by_key(|r| std::cmp::Reverse(merge_key(r))),
        Direction::Forward => records.sort_by_key(merge_key),
    }
    records.truncate(limit);
}

/// Where each partition resumes, keyed by partition, given the page just built.
///
/// Three cases, and the middle one is the whole difficulty:
///
/// - the partition put records on the page — resume *past* the last of them;
/// - it matched records but every one lost the merge to another partition's —
///   resume *at* the first of them, because none of them reached anyone;
/// - it matched nothing — resume past what it scanned, so an unproductive filtered
///   region is not walked a second time. That is the case #104 names.
///
/// Collapsing the middle case into the third is the tempting mistake: on a small
/// `limit` a whole partition can lose the merge, and marking it done there loses
/// every record it holds.
///
/// `None` for a partition with nothing left in the read direction.
///
/// Paging is exact as long as a partition's own timestamps do not go backwards.
/// They can — a producer sets them — and a record whose timestamp regressed far
/// enough to be truncated out of a page below one that was kept is then stepped
/// over. Resuming from the *lowest* gap instead would return records already shown,
/// which reads worse; this shares the response's `order_best_effort` caveat rather
/// than pretending a total order the log does not have.
fn resume_points(scans: &[PartitionScan], page: &[Value], dir: Direction) -> Vec<Option<i64>> {
    scans
        .iter()
        .map(|s| {
            let offsets = |records: &[Value]| -> Vec<i64> {
                records
                    .iter()
                    .filter(|r| {
                        r.get("partition").and_then(Value::as_i64) == Some(s.partition as i64)
                    })
                    .filter_map(|r| r.get("offset").and_then(Value::as_i64))
                    .collect()
            };
            let leading = |v: Vec<i64>| match dir {
                Direction::Forward => v.into_iter().max(),
                Direction::Backward => v.into_iter().min(),
            };
            let trailing = |v: Vec<i64>| match dir {
                Direction::Forward => v.into_iter().min(),
                Direction::Backward => v.into_iter().max(),
            };

            let next = match leading(offsets(page)) {
                // Shown: resume past the last of them.
                Some(edge) => dir.past(edge),
                // Matched, but the whole contribution lost the merge — every record
                // of this partition was outranked by another's. None of them reached
                // anyone, so the next page starts *at* the first, not past it.
                None => match trailing(offsets(&s.rendered)) {
                    Some(first) => first,
                    // Matched nothing at all: skip the region rather than re-walk it.
                    None => dir.past(s.frontier?),
                },
            };

            let served_end = s.watermark.served_end.unwrap_or(s.watermark.high);
            match dir {
                Direction::Forward => (next < served_end).then_some(next),
                Direction::Backward => (next >= s.watermark.low).then_some(next),
            }
        })
        .collect()
}

/// Fetches, decodes and filters messages from a topic, returning the full
/// response object. Shared by the HTTP handler and the Python binding.
///
/// `params.partition` is either one partition — storage order, today's response
/// shape — or `all`, which fans out over every partition concurrently and merges
/// them (#102). `params.offset` sets both where the read starts and which way it
/// travels: `latest` walks towards older records, everything else towards newer,
/// and the merge is ordered to match so that page two never holds records the
/// wrong side of page one.
///
/// Every response carries a `resume` point per partition — the offset the next
/// page starts from, or `null` where there is nothing left. Handing those back as
/// `params.cursor` continues the read instead of restarting it (#104).
///
/// A *filtered* read's scan budget is topic-wide: a 12-partition search spends one
/// `max_scan`, not twelve, give or take a record per partition. That is what makes
/// a filtered `all` approximate — each partition is read up to its share, so with
/// a small budget the answer is "what every partition had near its frontier", not
/// provably "the best `limit` of the topic". `order_best_effort` marks it. An
/// unfiltered read is not divided: a page is a page (see `page_budget`).
pub async fn messages(
    source: &StorageSource,
    registry: Option<&SchemaRegistry>,
    topic: &str,
    p: &MessageParams,
) -> Result<Value, QueryError> {
    let spec = parse_offset(&p.offset)?;
    let selector = parse_partition(&p.partition)?;
    let dir = Direction::of(spec);
    let cursor = p.cursor.as_deref().map(parse_cursor).transpose()?;

    // Validate topic + partition up front so a missing topic or an out-of-range
    // partition returns a clean, distinct error rather than a storage NotFound
    // whose message leaks the internal S3 object key (#63). A missing topic
    // surfaces as `StorageError::TopicNotFound` from `topic_partitions`.
    let partitions = source.topic_partitions(topic).await?;
    let selected: Vec<i32> = match selector {
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

    // A cursor narrows the read to the partitions that still have something left,
    // and replaces `offset` as each one's starting point. Its partitions must be
    // ones this request selected, or the page would silently answer about a
    // different topic slice than the caller asked for.
    let targets: Vec<i32> = match &cursor {
        None => selected.clone(),
        Some(points) => {
            for (part, _) in points {
                if !selected.contains(part) {
                    return Err(QueryError::BadRequest(format!(
                        "cursor names partition {part}, which this query does not read"
                    )));
                }
            }
            points.iter().map(|(part, _)| *part).collect()
        }
    };

    let limit = p.limit.clamp(1, MAX_LIMIT);
    let filters = Filters::build(p)?;
    let budget = if filters.filtering {
        partition_budget(p.max_scan.clamp(1, MAX_SCAN_CAP), targets.len())
    } else {
        page_budget(limit, targets.len())
    };

    let window = |partition: i32| -> Window {
        let Some(from) = cursor
            .as_ref()
            .and_then(|c| c.iter().find(|(part, _)| *part == partition))
            .map(|(_, off)| *off)
        else {
            return Window {
                spec,
                ceiling: None,
            };
        };
        match dir {
            // Forward, the resume point *is* the start.
            Direction::Forward => Window {
                spec: OffsetSpec::At(from),
                ceiling: None,
            },
            // Backward, it is the newest offset still unread, so the window is the
            // `budget` records ending there — `fetch` reads forward from its start.
            Direction::Backward => Window {
                spec: OffsetSpec::At((from + 1).saturating_sub(budget as i64)),
                ceiling: Some(from),
            },
        }
    };

    let (source, registry, topic, filters) = (source, registry, topic, &filters);
    let mut scans: Vec<PartitionScan> = futures::stream::iter(targets.iter().copied())
        .map(|partition| {
            let win = window(partition);
            async move {
                scan_partition(
                    source, registry, topic, partition, win, dir, budget, limit, filters,
                )
                .await
            }
        })
        .buffer_unordered(FANOUT)
        .try_collect()
        .await?;
    scans.sort_by_key(|s| s.partition);

    let scanned: usize = scans.iter().map(|s| s.scanned).sum();

    let order = match dir {
        Direction::Backward => "timestamp_desc",
        Direction::Forward => "timestamp_asc",
    };

    if let PartitionSelector::One(n) = selector {
        // A cursor listing no partition is a caller paging past the end. There is
        // nothing to read and nothing to say about it, so answer an empty page
        // rather than index a scan that was never run.
        let Some(mut scan) = scans.pop() else {
            return Ok(json!({
                "partition": n, "count": 0, "scanned": 0,
                "filtered": filters.filtering, "exhausted": true,
                "resume": Value::Null, "order": order, "records": [],
            }));
        };
        // One partition: the response shape #102 kept, plus the `resume` and
        // `order` every mode now carries. Records come back in the direction of
        // travel, so a `latest` read is newest-first rather than in storage order —
        // otherwise a page that stopped at `limit` would hand back the oldest
        // records of a window the caller asked the newest of.
        let resume = resume_points(std::slice::from_ref(&scan), &scan.rendered, dir)[0];
        let page = std::mem::take(&mut scan.rendered);
        return Ok(json!({
            "partition": n,
            "watermark": scan.watermark,
            "count": page.len(),
            "scanned": scanned,
            "filtered": filters.filtering,
            "exhausted": resume.is_none(),
            "resume": resume,
            "order": order,
            "records": page,
        }));
    }

    let mut records: Vec<Value> = scans
        .iter()
        .flat_map(|s| s.rendered.iter().cloned())
        .collect();
    merge_page(&mut records, dir, limit);
    let resume = resume_points(&scans, &records, dir);

    Ok(json!({
        "partitions": scans.iter().zip(&resume).map(|(s, r)| json!({
            "partition": s.partition,
            "watermark": s.watermark,
            "scanned": s.scanned,
            // Nothing left to read here, in this direction.
            "exhausted": r.is_none(),
            "resume": r,
        })).collect::<Vec<_>>(),
        "count": records.len(),
        "scanned": scanned,
        "filtered": filters.filtering,
        "exhausted": resume.iter().all(Option::is_none),
        "order": order,
        "order_best_effort": true,
        "records": records,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_latest_reads_backwards() {
        assert_eq!(Direction::of(OffsetSpec::Latest), Direction::Backward);
        assert_eq!(Direction::of(OffsetSpec::Earliest), Direction::Forward);
        assert_eq!(Direction::of(OffsetSpec::At(42)), Direction::Forward);
        assert_eq!(
            Direction::of(OffsetSpec::Timestamp(1700)),
            Direction::Forward
        );
    }

    /// #108: the number that explains a partition's size, per record.
    #[test]
    fn a_record_size_is_its_key_value_and_header_bytes() {
        use crate::storage::RecordHeader;

        let record =
            |key: Option<&str>, value: Option<&str>, headers: Vec<(&str, &str)>| DecodedRecord {
                offset: 0,
                partition: 0,
                timestamp: 0,
                key: key.map(|s| bytes::Bytes::from(s.to_string())),
                value: value.map(|s| bytes::Bytes::from(s.to_string())),
                headers: headers
                    .into_iter()
                    .map(|(k, v)| RecordHeader {
                        key: Some(bytes::Bytes::from(k.to_string())),
                        value: Some(bytes::Bytes::from(v.to_string())),
                    })
                    .collect(),
            };

        assert_eq!(record_size(&record(Some("k"), Some("val"), vec![])), 4);
        // Headers count: they are bytes on the wire like any other field, and a
        // topic whose records carry tracing headers pays for them.
        assert_eq!(
            record_size(&record(Some("k"), Some("val"), vec![("trace", "abc")])),
            12
        );
        // A missing field costs nothing — Kafka writes a length of -1, not an
        // empty payload — and must not read as a zero-length one either.
        assert_eq!(record_size(&record(None, Some("val"), vec![])), 3);
        assert_eq!(record_size(&record(None, None, vec![])), 0);
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

    /// Building a `PartitionScan` as a fan-out would leave it, to exercise the
    /// resume arithmetic without S3.
    fn scan(
        partition: i32,
        low: i64,
        high: i64,
        frontier: Option<i64>,
        shown: &[i64],
    ) -> PartitionScan {
        PartitionScan {
            partition,
            watermark: Watermark {
                low,
                high,
                served_end: None,
            },
            rendered: shown
                .iter()
                .map(|o| json!({ "partition": partition, "offset": o }))
                .collect(),
            scanned: shown.len(),
            frontier,
        }
    }

    #[test]
    fn a_cursor_carries_one_offset_per_partition() {
        assert_eq!(
            parse_cursor("0:412,3:998").unwrap(),
            vec![(0, 412), (3, 998)]
        );
        assert_eq!(parse_cursor("2:7").unwrap(), vec![(2, 7)]);
        // An exhausted read hands back nothing, and that must not be an error.
        assert_eq!(parse_cursor("").unwrap(), vec![]);
    }

    #[test]
    fn a_cursor_naming_a_partition_twice_is_refused() {
        // Two starting points for one partition would scan it twice and return its
        // records twice — a page that repeats itself, not a page.
        let err = parse_cursor("0:1,2:4,0:9").unwrap_err().to_string();
        assert!(err.contains("twice"), "{err}");
    }

    #[test]
    fn a_malformed_cursor_names_the_pair_it_choked_on() {
        for raw in ["0", "0:", "a:1", "0:x", "0:1,nope"] {
            let err = parse_cursor(raw).unwrap_err().to_string();
            assert!(err.contains("partition:offset"), "{raw}: {err}");
        }
    }

    /// #104: dividing the page across partitions made a page stop being a page —
    /// 50 records asked for on a 12-partition topic came back as 4.
    #[test]
    fn an_unfiltered_page_is_not_divided_across_partitions() {
        assert_eq!(page_budget(50, 12), 50);
        assert_eq!(page_budget(500, 1), 500);
        // The topic-wide cap still bites on a very wide topic.
        assert_eq!(page_budget(500, 200), MAX_SCAN_CAP / 200);
    }

    /// A forward read asked for the *oldest* records; keeping the newest of the
    /// merge would hand back the opposite, and let page two precede page one.
    #[test]
    fn the_merge_keeps_the_end_of_the_page_the_read_travels_towards() {
        let rec = |ts: i64| json!({ "timestamp": ts, "partition": 0, "offset": ts });
        let src = || vec![rec(10), rec(30), rec(20)];

        let mut back = src();
        merge_page(&mut back, Direction::Backward, 2);
        assert_eq!(
            back.iter()
                .map(|r| r["timestamp"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![30, 20]
        );

        let mut fwd = src();
        merge_page(&mut fwd, Direction::Forward, 2);
        assert_eq!(
            fwd.iter()
                .map(|r| r["timestamp"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![10, 20]
        );
    }

    #[test]
    fn a_page_resumes_past_the_last_record_it_showed() {
        let scans = [scan(0, 0, 100, Some(40), &[10, 40])];
        let page: Vec<Value> = scans[0].rendered.clone();
        assert_eq!(
            resume_points(&scans, &page, Direction::Forward),
            vec![Some(41)]
        );
        assert_eq!(
            resume_points(&scans, &page, Direction::Backward),
            vec![Some(9)]
        );
    }

    /// The distinction #104 turns on: a record scanned, matched, then dropped by
    /// the merge's truncation was never shown, so the next page must return it.
    #[test]
    fn a_record_truncated_out_of_the_page_is_not_skipped() {
        let scans = [scan(0, 0, 100, Some(40), &[10, 40])];
        // The merge kept only offset 10; 40 never reached the caller.
        let page = vec![json!({ "partition": 0, "offset": 10 })];
        assert_eq!(
            resume_points(&scans, &page, Direction::Forward),
            vec![Some(11)]
        );
    }

    /// The middle case, and the one that loses records if it is collapsed into the
    /// third: on a small `limit` a whole partition's contribution can lose the merge
    /// to another's. Nothing of it was shown, so nothing of it may be skipped.
    #[test]
    fn a_partition_whose_whole_contribution_lost_the_merge_starts_again_at_its_first() {
        let scans = [scan(1, 0, 100, Some(30), &[10, 20, 30])];
        // The page is all partition 0's: partition 1 matched, and showed nothing.
        let page = vec![json!({ "partition": 0, "offset": 7 })];
        assert_eq!(
            resume_points(&scans, &page, Direction::Forward),
            vec![Some(10)]
        );
        assert_eq!(
            resume_points(&scans, &page, Direction::Backward),
            vec![Some(30)]
        );
    }

    /// ...whereas a partition that matched nothing resumes at its scan frontier,
    /// so an unproductive filtered region is not walked a second time.
    #[test]
    fn a_partition_that_matched_nothing_resumes_past_what_it_scanned() {
        let scans = [scan(3, 0, 100, Some(64), &[])];
        assert_eq!(
            resume_points(&scans, &[], Direction::Forward),
            vec![Some(65)]
        );
        assert_eq!(
            resume_points(&scans, &[], Direction::Backward),
            vec![Some(63)]
        );
    }

    #[test]
    fn a_partition_with_nothing_left_reports_no_resume() {
        // Forward, past the served end.
        assert_eq!(
            resume_points(
                &[scan(0, 0, 6, Some(5), &[5])],
                &[json!({ "partition": 0, "offset": 5 })],
                Direction::Forward
            ),
            vec![None]
        );
        // Backward, past the truncation floor.
        assert_eq!(
            resume_points(
                &[scan(0, 2, 6, Some(2), &[2])],
                &[json!({ "partition": 0, "offset": 2 })],
                Direction::Backward
            ),
            vec![None]
        );
        // Nothing scanned at all: an empty partition.
        assert_eq!(
            resume_points(&[scan(0, 0, 0, None, &[])], &[], Direction::Forward),
            vec![None]
        );
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
