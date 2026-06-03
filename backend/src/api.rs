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
    schema::{decode_field, SchemaError, SchemaRegistry},
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
}

fn default_offset() -> String {
    "latest".to_string()
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

    let watermark = source.watermark(&topic, query.partition).await?;
    let records = source.fetch(&topic, query.partition, spec, limit).await?;

    let registry = state.registry.as_ref();
    let mut rendered = Vec::with_capacity(records.len());
    for record in &records {
        rendered.push(json!({
            "offset": record.offset,
            "partition": record.partition,
            "timestamp": record.timestamp,
            "key": decode_field(registry, &record.key).await,
            "value": decode_field(registry, &record.value).await,
            "headers": record.headers.iter().map(|h| json!({
                "key": h.key.as_ref().map(crate::schema::raw_field),
                "value": h.value.as_ref().map(crate::schema::raw_field),
            })).collect::<Vec<_>>(),
        }));
    }

    Ok(Json(json!({
        "partition": query.partition,
        "watermark": watermark,
        "count": rendered.len(),
        "records": rendered,
    })))
}
