//! Runtime configuration, loaded from environment variables.

use std::{env, net::SocketAddr};

/// Backend configuration.
///
/// S3 source configuration (bucket, endpoint, credentials, cluster) will be
/// added here in #2 — kept out of the scaffold on purpose.
#[derive(Clone, Debug)]
pub struct Config {
    /// Address the HTTP server binds to (`KOTATSU_BIND`, default `0.0.0.0:8080`).
    pub bind_addr: SocketAddr,

    /// Directory of built frontend assets to serve in production
    /// (`KOTATSU_STATIC_DIR`). When unset, no static files are served — the
    /// frontend runs separately via its own dev server.
    pub static_dir: Option<String>,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let bind_addr = env::var("KOTATSU_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()?;

        let static_dir = env::var("KOTATSU_STATIC_DIR").ok().filter(|s| !s.is_empty());

        Ok(Self {
            bind_addr,
            static_dir,
        })
    }
}
