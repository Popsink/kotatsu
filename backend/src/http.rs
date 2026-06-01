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
/// - `GET /api/source` — configured source + connectivity status.
/// - `GET /api/clusters/{cluster}/topics/{topic}/messages` — event browser.
/// - everything else — frontend static assets in production, when
///   `KOTATSU_STATIC_DIR` is set (SPA fallback to `index.html`).
pub fn router(config: &Config, state: AppState) -> Router {
    let api = Router::new()
        .route("/health", get(health))
        .route("/source", get(source))
        .route(
            "/clusters/{cluster}/topics/{topic}/messages",
            get(api::messages),
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

/// Reports the configured source and whether it is currently reachable.
/// The connectivity check is on-demand (this request), never on a timer.
async fn source(State(state): State<AppState>) -> impl IntoResponse {
    let (Some(source), Some(info)) = (&state.source, &state.source_info) else {
        return Json(json!({ "configured": false }));
    };

    let status = match source.check().await {
        Ok(()) => json!({ "connected": true }),
        Err(err) => json!({ "connected": false, "error": err.to_string() }),
    };

    Json(json!({
        "configured": true,
        "bucket": info.bucket,
        "cluster": info.cluster,
        "endpoint": info.endpoint,
        "region": info.region,
        "status": status,
    }))
}
