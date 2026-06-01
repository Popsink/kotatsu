//! HTTP router construction.

use axum::{routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

use crate::config::Config;

/// Build the application router.
///
/// - `GET /health` — liveness probe.
/// - `/api/*` — JSON API (endpoints land in #2+).
/// - everything else — frontend static assets in production, when
///   `KOTATSU_STATIC_DIR` is set (SPA fallback to `index.html`).
pub fn router(config: &Config) -> Router {
    let api = Router::new().route("/health", get(health));

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
