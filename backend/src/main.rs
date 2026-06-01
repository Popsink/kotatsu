//! Kotatsu backend — read-only, on-demand browser over Tansu's native S3 storage.
//!
//! No Kafka client, no broker: every read is triggered by a UI action and goes
//! straight to the object store. See the GitHub issues for the design.

mod config;
mod http;

use anyhow::Context;
use config::Config;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::from_env().context("loading configuration")?;
    tracing::info!(?config, "starting kotatsu");

    let app = http::router(&config);

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
