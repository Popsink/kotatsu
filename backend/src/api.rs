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
    query::{self, MessageParams, QueryError},
    schema::{SchemaError, SchemaRegistry},
    state::AppState,
    storage::{LagMode, StorageError, StorageSource},
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
        match err {
            // User-facing, already sanitized (no internal layout in the message).
            StorageError::NotConfigured => {
                ApiError::new(StatusCode::SERVICE_UNAVAILABLE, err.to_string())
            }
            StorageError::ClusterNotFound(_)
            | StorageError::TopicNotFound(_)
            | StorageError::GroupNotFound(_) => {
                ApiError::new(StatusCode::NOT_FOUND, err.to_string())
            }
            StorageError::Unreachable(_) => ApiError::new(StatusCode::BAD_GATEWAY, err.to_string()),

            // Internal-detail-bearing errors: the raw object key / store error
            // stays in the server logs only; the client gets a generic message
            // so we don't leak the S3 object layout (#63, same class as #56).
            StorageError::NotFound(_) => {
                tracing::warn!(error = %err, "storage object not found");
                ApiError::new(StatusCode::NOT_FOUND, "not found")
            }
            StorageError::Decode(_) | StorageError::Parse { .. } | StorageError::ObjectStore(_) => {
                tracing::error!(error = %err, "storage read failed");
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal storage error")
            }
        }
    }
}

impl From<SchemaError> for ApiError {
    fn from(err: SchemaError) -> Self {
        let status = match err {
            SchemaError::NotConfigured => StatusCode::SERVICE_UNAVAILABLE,
            SchemaError::NotFound => StatusCode::NOT_FOUND,
            SchemaError::Unreachable => StatusCode::BAD_GATEWAY,
        };
        ApiError::new(status, err.to_string())
    }
}

impl From<QueryError> for ApiError {
    fn from(err: QueryError) -> Self {
        match err {
            QueryError::BadRequest(msg) => ApiError::new(StatusCode::BAD_REQUEST, msg),
            QueryError::Storage(e) => e.into(),
        }
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

/// Query params for the topic-tree endpoint: a `prefix` (the chosen dotted path,
/// empty at the root) plus the usual search/paging.
#[derive(Deserialize)]
pub struct TreeQuery {
    #[serde(default)]
    prefix: String,
    search: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

/// `GET /api/clusters/{cluster}/topic-tree?prefix=&search=&limit=&offset=`
///
/// One level of the prefix tree. Below `org.env.conn` (depth < 3) it returns the
/// distinct next components as group nodes; at the connector level it returns the
/// topics under that prefix as summary rows (`level` tells the client which).
pub async fn topic_tree(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<Value>, ApiError> {
    let source = cluster_source(&state, &cluster)?;
    let prefix = query.prefix.trim_matches('.').to_string();
    let depth = if prefix.is_empty() {
        0
    } else {
        prefix.split('.').count()
    };
    let page: Page = Page::new(query.search, query.limit, query.offset);

    if depth >= crate::storage::CONNECTOR_DEPTH {
        let paged = source.list_topics_under(&prefix, &page).await?;
        Ok(Json(json!({
            "cluster": cluster,
            "prefix": prefix,
            "depth": depth,
            "level": "topic",
            "items": paged.items,
            "total": paged.total,
            "limit": paged.limit,
            "offset": paged.offset,
        })))
    } else {
        let paged = source.topic_groups_at(&prefix, &page).await?;
        Ok(Json(json!({
            "cluster": cluster,
            "prefix": prefix,
            "depth": depth,
            "level": "group",
            "items": paged.items,
            "total": paged.total,
            "limit": paged.limit,
            "offset": paged.offset,
        })))
    }
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

/// `GET /api/clusters/{cluster}/topics/{topic}/groups` — consumer groups with a
/// committed offset on this topic (scans groups; called lazily by the UI).
pub async fn topic_groups(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let source = cluster_source(&state, &cluster)?;
    let groups = source.groups_consuming(&topic).await?;
    Ok(Json(json!({ "topic": topic, "groups": groups })))
}

#[derive(Deserialize)]
pub struct MessagesQuery {
    /// `all` (default) or a concrete partition number.
    #[serde(default = "default_partition")]
    partition: String,
    #[serde(default = "default_offset")]
    offset: String,
    /// Resume points from a previous response's `resume`: `0:412,3:998` (#104).
    cursor: Option<String>,
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

fn default_partition() -> String {
    "all".to_string()
}
fn default_offset() -> String {
    "latest".to_string()
}
fn default_max_scan() -> usize {
    query::DEFAULT_MAX_SCAN
}
fn default_limit() -> usize {
    50
}

/// Query params for the consumer-group listing: the usual search/paging, plus
/// the opt-in lag figures and what to order by (#107).
///
/// The paging fields are repeated rather than `#[serde(flatten)]`-ed from
/// [`ListQuery`]: flattening needs a self-describing format, which a query
/// string is not, and the extractor would reject every request.
#[derive(Deserialize)]
pub struct GroupsQuery {
    search: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
    /// Compute lag. Off by default: it reads the high watermark behind every
    /// committed offset, so the plain listing has to stay cheap for callers that
    /// only want names and states.
    #[serde(default)]
    lag: bool,
    /// `lag` (default when lag is on) or `name`.
    sort: Option<String>,
}

impl From<&GroupsQuery> for Page {
    fn from(q: &GroupsQuery) -> Self {
        Page::new(q.search.clone(), q.limit, q.offset)
    }
}

/// `GET /api/clusters/{cluster}/groups?search=&limit=&offset=&lag=&sort=`
pub async fn groups(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
    Query(query): Query<GroupsQuery>,
) -> Result<Json<Value>, ApiError> {
    let source = cluster_source(&state, &cluster)?;
    let mode = LagMode::from_request(query.lag, query.sort.as_deref());
    let page = Page::from(&query);
    let paged = source.list_groups(&page, mode).await?;
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

/// Maps a registry error to a user-level message, naming the subject on 404
/// (internal route/URL details stay in the server logs).
fn subject_err(subject: &str) -> impl Fn(SchemaError) -> ApiError + '_ {
    move |e| match e {
        SchemaError::NotFound => ApiError::new(
            StatusCode::NOT_FOUND,
            format!("subject '{subject}' not found"),
        ),
        other => other.into(),
    }
}

/// `GET /api/schemas/{subject}` — versions, latest schema, compatibility.
pub async fn schema_subject(
    State(state): State<AppState>,
    Path(subject): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let registry = registry(&state)?;
    let versions = registry
        .versions(&subject)
        .await
        .map_err(subject_err(&subject))?;
    let latest = registry
        .version(&subject, "latest")
        .await
        .map_err(subject_err(&subject))?;
    let compatibility = registry.compatibility(&subject).await;
    Ok(Json(json!({
        "subject": subject,
        "versions": versions,
        "latest": latest,
        "compatibility": compatibility,
    })))
}

/// `GET /api/schemas/ids/{id}/versions` — the subjects and versions a schema id
/// is registered under.
///
/// The event browser tags a decoded record with its schema **id**; the subject
/// page is addressed by **version**. This is the only thing that maps one to the
/// other, and the registry answers it from its own index (#112).
pub async fn schema_id_versions(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, ApiError> {
    let registry = registry(&state)?;
    let versions = registry.id_versions(id).await.map_err(|e| match e {
        SchemaError::NotFound => ApiError::new(
            StatusCode::NOT_FOUND,
            format!("no schema registered with id {id}"),
        ),
        other => other.into(),
    })?;
    Ok(Json(json!({ "id": id, "versions": versions })))
}

/// `GET /api/schemas/{subject}/versions/{version}` — a specific version's schema.
pub async fn schema_version(
    State(state): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let registry = registry(&state)?;
    let schema = registry
        .version(&subject, &version)
        .await
        .map_err(subject_err(&subject))?;
    Ok(Json(json!(schema)))
}

/// `GET /api/clusters/{cluster}/topics/{topic}/messages`
///
/// Reads records directly from S3 on user action. `offset` accepts
/// `earliest`, `latest`, a specific offset, or `timestamp:<ms>`, and sets which
/// way the read travels. Every response carries a `resume` point per partition;
/// handing those back as `cursor` returns the next page (#104). Confluent-framed
/// Avro keys/values are decoded against the schema registry (#8).
pub async fn messages(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
    Query(query): Query<MessagesQuery>,
) -> Result<Json<Value>, ApiError> {
    let source = cluster_source(&state, &cluster)?;
    let params = MessageParams {
        partition: query.partition,
        offset: query.offset,
        cursor: query.cursor,
        limit: query.limit,
        key_format: query.key_format,
        value_format: query.value_format,
        key_contains: query.key_contains,
        value_contains: query.value_contains,
        header_key: query.header_key,
        header_value: query.header_value,
        regex: query.regex,
        max_scan: query.max_scan,
    };
    let body = query::messages(source, state.registry.as_ref(), &topic, &params).await?;
    Ok(Json(body))
}
