//! Kotatsu backend — read-only, on-demand browser over Tansu's native S3 storage.
//!
//! No Kafka client, no broker: every read is triggered by a UI action and goes
//! straight to the object store. See the GitHub issues for the design.

use anyhow::Context;
use kotatsu::{
    config::Config,
    http,
    state::{AppState, SourceInfo},
    storage::StorageSource,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::from_env().context("loading configuration")?;
    tracing::info!(?config, "starting kotatsu");

    let state = build_state(&config)?;

    let app = http::router(&config, state);

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .with_context(|| format!("binding {}", config.bind_addr))?;
    tracing::info!(addr = %config.bind_addr, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    Ok(())
}

/// Builds the shared app state, constructing the S3 source if configured.
///
/// Connectivity is not probed here — that happens on-demand per request — so a
/// misconfigured or down S3 never blocks startup (`/health` stays up).
fn build_state(config: &Config) -> anyhow::Result<AppState> {
    let Some(s3) = &config.s3 else {
        tracing::warn!("no S3 source configured (set KOTATSU_S3_BUCKET + KOTATSU_CLUSTER)");
        return Ok(AppState {
            source: None,
            source_info: None,
        });
    };

    let source = StorageSource::from_config(s3).context("building S3 source")?;
    tracing::info!(bucket = %s3.bucket, cluster = %s3.cluster, "S3 source configured");

    Ok(AppState {
        source: Some(source),
        source_info: Some(SourceInfo {
            bucket: s3.bucket.clone(),
            cluster: s3.cluster.clone(),
            endpoint: s3.endpoint.clone(),
            region: s3.region.clone(),
        }),
    })
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "kotatsu=info,tower_http=info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
