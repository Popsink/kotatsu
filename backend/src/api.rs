//! JSON API handlers.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    pagination::Page,
    schema::{decode_field, FieldFormat, SchemaError, SchemaRegistry},
    state::AppState,
    storage::{OffsetSpec, StorageError, StorageSource},
};

/// Query params for paginated list endpoints (`?search=&limit=&offset=`).
#[derive(Deserialize)]
pub struct ListQuery {
    search: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

impl From<ListQuery> for Page {
    fn from(q: ListQuery) -> Self {
        Page::new(q.search, q.limit, q.offset)
    }
}

/// An API error with an HTTP status and a message.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

impl From<StorageError> for ApiError {
    fn from(err: StorageError) -> Self {
        let status = match err {
            StorageError::NotConfigured => StatusCode::SERVICE_UNAVAILABLE,
            StorageError::NotFound(_) => StatusCode::NOT_FOUND,
            StorageError::ClusterNotFound(_) => StatusCode::NOT_FOUND,
            StorageError::TopicNotFound(_) => StatusCode::NOT_FOUND,
            StorageError::GroupNotFound(_) => StatusCode::NOT_FOUND,
            StorageError::Unreachable(_) => StatusCode::BAD_GATEWAY,
            StorageError::Decode(_) | StorageError::Parse { .. } | StorageError::ObjectStore(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        ApiError::new(status, err.to_string())
    }
}

impl From<SchemaError> for ApiError {
    fn from(err: SchemaError) -> Self {
        let status = match err {
            SchemaError::NotConfigured => StatusCode::SERVICE_UNAVAILABLE,
            SchemaError::SubjectNotFound(_) => StatusCode::NOT_FOUND,
            SchemaError::Request(_) => StatusCode::BAD_GATEWAY,
        };
        ApiError::new(status, err.to_string())
    }
}

/// Resolves the schema registry, or 503 if none configured.
fn registry(state: &AppState) -> Result<&SchemaRegistry, ApiError> {
    state.registry.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "no schema registry configured",
        )
    })
}

/// Resolves the configured source, or 503 if none.
fn source(state: &AppState) -> Result<&StorageSource, ApiError> {
    state
        .source
        .as_ref()
        .ok_or_else(|| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "no S3 source configured"))
}

/// Resolves the source and verifies the path cluster matches the configured one.
fn cluster_source<'a>(state: &'a AppState, cluster: &str) -> Result<&'a StorageSource, ApiError> {
    let source = source(state)?;
    if cluster != source.keys().cluster() {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("unknown cluster '{cluster}'"),
        ));
    }
    Ok(source)
}

/// `GET /api/clusters`
pub async fn clusters(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let source = source(&state)?;
    let clusters = source.list_clusters().await?;
    Ok(Json(json!({ "clusters": clusters })))
}

/// `GET /api/clusters/{cluster}` — meta.json summary.
pub async fn cluster(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let source = cluster_source(&state, &cluster)?;
    let summary = source.cluster_summary().await?;
    Ok(Json(json!(summary)))
}

/// `GET /api/clusters/{cluster}/topics?search=&limit=&offset=`
pub async fn topics(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let source = cluster_source(&state, &cluster)?;
    let paged = source.list_topics(&query.into()).await?;
    Ok(Json(json!({
        "cluster": cluster,
        "items": paged.items,
        "total": paged.total,
        "limit": paged.limit,
        "offset": paged.offset,
    })))
}

/// `GET /api/clusters/{cluster}/topics/{topic}`
pub async fn topic_detail(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let source = cluster_source(&state, &cluster)?;
    let detail = source.topic_detail(&topic).await?;
    Ok(Json(json!(detail)))
}

#[derive(Deserialize)]
pub struct MessagesQuery {
    #[serde(default)]
    partition: i32,
    #[serde(default = "default_offset")]
    offset: String,
    #[serde(default = "default_limit")]
    limit: usize,
    /// `auto` | `avro` | `json` | `raw` (see [`FieldFormat`]).
    value_format: Option<String>,
    key_format: Option<String>,
    // Filters (applied to the decoded fields, scanning forward up to `max_scan`).
    key_contains: Option<String>,
    value_contains: Option<String>,
    header_key: Option<String>,
    header_value: Option<String>,
    #[serde(default)]
    regex: bool,
    #[serde(default = "default_max_scan")]
    max_scan: usize,
}

fn default_offset() -> String {
    "latest".to_string()
}
fn default_max_scan() -> usize {
    5000
}

/// Hard cap on records scanned per filtered request — keeps the on-demand model
/// honest (no unbounded S3 reads).
const MAX_SCAN_CAP: usize = 50_000;

/// A compiled needle for matching a decoded field's text.
enum Needle {
    Sub(String),
    Re(regex::Regex),
}

impl Needle {
    fn build(raw: &str, regex: bool) -> Result<Self, ApiError> {
        if regex {
            regex::Regex::new(raw)
                .map(Needle::Re)
                .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, format!("invalid regex: {e}")))
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
fn default_limit() -> usize {
    50
}

/// Maximum records returned in one request.
const MAX_LIMIT: usize = 500;

fn parse_offset(raw: &str) -> Result<OffsetSpec, ApiError> {
    let bad =
        |what: &str| ApiError::new(StatusCode::BAD_REQUEST, format!("invalid offset: {what}"));
    match raw {
        "earliest" => Ok(OffsetSpec::Earliest),
        "latest" => Ok(OffsetSpec::Latest),
        _ => {
            if let Some(ts) = raw.strip_prefix("timestamp:") {
                ts.parse().map(OffsetSpec::Timestamp).map_err(|_| bad(raw))
            } else {
                raw.parse().map(OffsetSpec::At).map_err(|_| bad(raw))
            }
        }
    }
}

/// `GET /api/clusters/{cluster}/groups?search=&limit=&offset=`
pub async fn groups(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let source = cluster_source(&state, &cluster)?;
    let paged = source.list_groups(&query.into()).await?;
    Ok(Json(json!({
        "cluster": cluster,
        "items": paged.items,
        "total": paged.total,
        "limit": paged.limit,
        "offset": paged.offset,
    })))
}

/// `GET /api/clusters/{cluster}/groups/{group}`
pub async fn group_detail(
    State(state): State<AppState>,
    Path((cluster, group)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let source = cluster_source(&state, &cluster)?;
    let detail = source.group_detail(&group).await?;
    Ok(Json(json!(detail)))
}

/// `GET /api/schemas?search=&limit=&offset=` — list subjects in the registry.
pub async fn schemas(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let registry = registry(&state)?;
    let page: Page = query.into();
    let (items, total) = page.select(registry.subjects().await?);
    Ok(Json(json!({
        "registry": registry.base_url(),
        "items": items,
        "total": total,
        "limit": page.limit,
        "offset": page.offset,
    })))
}

/// `GET /api/schemas/{subject}` — versions + the latest schema for a subject.
pub async fn schema_subject(
    State(state): State<AppState>,
    Path(subject): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let registry = registry(&state)?;
    let versions = registry.versions(&subject).await?;
    let latest = registry.version(&subject, "latest").await?;
    Ok(Json(json!({
        "subject": subject,
        "versions": versions,
        "latest": latest,
    })))
}

/// `GET /api/clusters/{cluster}/topics/{topic}/messages`
///
/// Reads records directly from S3 on user action. `offset` accepts
/// `earliest`, `latest`, a specific offset, or `timestamp:<ms>`. Confluent-
/// framed Avro keys/values are decoded against the schema registry (#8).
pub async fn messages(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
    Query(query): Query<MessagesQuery>,
) -> Result<Json<Value>, ApiError> {
    let source = cluster_source(&state, &cluster)?;

    let spec = parse_offset(&query.offset)?;
    let limit = query.limit.clamp(1, MAX_LIMIT);

    let registry = state.registry.as_ref();
    let value_format = FieldFormat::parse(query.value_format.as_deref());
    let key_format = FieldFormat::parse(query.key_format.as_deref());

    // Build filters; when any is set we scan forward up to `max_scan` records.
    let key_needle = query
        .key_contains
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| Needle::build(s, query.regex))
        .transpose()?;
    let value_needle = query
        .value_contains
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| Needle::build(s, query.regex))
        .transpose()?;
    let header_key = query.header_key.as_deref().filter(|s| !s.is_empty());
    let header_value = query.header_value.as_deref().filter(|s| !s.is_empty());
    let filtering = key_needle.is_some() || value_needle.is_some() || header_key.is_some();

    let scan_limit = if filtering {
        query.max_scan.clamp(1, MAX_SCAN_CAP)
    } else {
        limit
    };

    let watermark = source.watermark(&topic, query.partition).await?;
    let records = source
        .fetch(&topic, query.partition, spec, scan_limit)
        .await?;
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
                "key": h.key.as_ref().map(crate::schema::raw_field),
                "value": h.value.as_ref().map(crate::schema::raw_field),
            })).collect::<Vec<_>>(),
        }));
        if rendered.len() >= limit {
            break;
        }
    }

    Ok(Json(json!({
        "partition": query.partition,
        "watermark": watermark,
        "count": rendered.len(),
        "scanned": scanned,
        "filtered": filtering,
        "exhausted": exhausted && scanned == records.len(),
        "records": rendered,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        // Object data is stringified so substring search still works.
        assert!(searchable(&json!({"kind": "avro", "data": {"id": 3}})).contains("\"id\":3"));
    }
}
