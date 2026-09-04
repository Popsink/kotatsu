//! HTTP router construction.

use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

use crate::{api, config::Config, state::AppState};

/// Build the application router.
///
/// - `GET /health` — liveness probe.
/// - `GET /api/health` — liveness probe (API namespace).
/// - `GET /api/source` — configured source (no I/O).
/// - `GET /api/source/status` — live connectivity probe against the store.
/// - `GET /api/clusters/{cluster}/topics/{topic}/messages` — event browser.
/// - everything else — frontend static assets in production, when
///   `KOTATSU_STATIC_DIR` is set (SPA fallback to `index.html`).
pub fn router(config: &Config, state: AppState) -> Router {
    let api = Router::new()
        .route("/health", get(health))
        .route("/source", get(source))
        .route("/source/status", get(source_status))
        .route("/clusters", get(api::clusters))
        .route("/clusters/{cluster}", get(api::cluster))
        .route("/clusters/{cluster}/topic-tree", get(api::topic_tree))
        .route("/clusters/{cluster}/topics", get(api::topics))
        .route("/clusters/{cluster}/topics/{topic}", get(api::topic_detail))
        .route(
            "/clusters/{cluster}/topics/{topic}/groups",
            get(api::topic_groups),
        )
        .route(
            "/clusters/{cluster}/topics/{topic}/messages",
            get(api::messages),
        )
        .route("/clusters/{cluster}/groups", get(api::groups))
        .route("/clusters/{cluster}/groups/{group}", get(api::group_detail))
        .route("/schemas", get(api::schemas))
        .route("/schemas/ids/{id}/versions", get(api::schema_id_versions))
        .route("/schemas/{subject}", get(api::schema_subject))
        .route(
            "/schemas/{subject}/versions/{version}",
            get(api::schema_version),
        )
        .with_state(state);

    let mut app = Router::new()
        .route("/health", get(health))
        .nest("/api", api)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());

    if let Some(dir) = &config.static_dir {
        let index = format!("{dir}/index.html");
        let serve = ServeDir::new(dir).fallback(ServeFile::new(index));
        app = app.fallback_service(serve);
    }

    app
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": "kotatsu" }))
}

/// Reports the configured source: bucket, cluster, endpoint, region.
///
/// Pure configuration — no object-store call. Every page needs the cluster id
/// to build its URLs, so this used to carry a live S3 probe that every
/// navigation paid for and only the Overview screen displayed (#109). The probe
/// moved to `/api/source/status`.
async fn source(State(state): State<AppState>) -> impl IntoResponse {
    let Some(info) = &state.source_info else {
        return Json(json!({ "configured": false }));
    };

    Json(json!({
        "configured": true,
        "bucket": info.bucket,
        "cluster": info.cluster,
        "endpoint": info.endpoint,
        "region": info.region,
    }))
}

/// Probes the object store and reports whether it is reachable.
///
/// The one endpoint that costs an S3 round-trip to answer, so it is asked for
/// explicitly — on the Overview screen and on its re-check button — never as a
/// side effect of rendering another page, and never on a timer.
async fn source_status(State(state): State<AppState>) -> impl IntoResponse {
    let Some(source) = &state.source else {
        return Json(json!({ "configured": false, "connected": false }));
    };

    match source.check().await {
        Ok(()) => Json(json!({ "configured": true, "connected": true })),
        Err(err) => Json(json!({
            "configured": true,
            "connected": false,
            "error": err.to_string(),
        })),
    }
}
