//! Runtime configuration, loaded from environment variables.

use std::{env, net::SocketAddr};

/// Backend configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Address the HTTP server binds to (`KOTATSU_BIND`, default `0.0.0.0:8080`).
    pub bind_addr: SocketAddr,

    /// Directory of built frontend assets to serve in production
    /// (`KOTATSU_STATIC_DIR`). When unset, no static files are served — the
    /// frontend runs separately via its own dev server.
    pub static_dir: Option<String>,

    /// S3 source configuration. `None` when not configured — the server still
    /// starts (so `/health` works), but storage-backed endpoints report that
    /// no source is configured.
    pub s3: Option<S3Config>,
}

/// Configuration for the single S3 source Kotatsu reads from.
#[derive(Clone)]
pub struct S3Config {
    /// Bucket holding Tansu's storage (`KOTATSU_S3_BUCKET`).
    pub bucket: String,
    /// Tansu cluster name = the `clusters/{cluster}/` prefix (`KOTATSU_CLUSTER`).
    pub cluster: String,
    /// Optional custom endpoint, e.g. MinIO/R2 (`KOTATSU_S3_ENDPOINT`).
    pub endpoint: Option<String>,
    /// Region (`KOTATSU_S3_REGION`, default `us-east-1`).
    pub region: String,
    /// Access key id (`KOTATSU_S3_ACCESS_KEY`).
    pub access_key: Option<String>,
    /// Secret access key (`KOTATSU_S3_SECRET_KEY`).
    pub secret_key: Option<String>,
    /// Use path-style addressing (`KOTATSU_S3_FORCE_PATH_STYLE`, default true —
    /// required by MinIO and most non-AWS S3s).
    pub force_path_style: bool,
    /// Allow plain HTTP endpoints (derived from the endpoint scheme).
    pub allow_http: bool,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never log credentials.
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("cluster", &self.cluster)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key", &self.access_key.as_ref().map(|_| "***"))
            .field("secret_key", &self.secret_key.as_ref().map(|_| "***"))
            .field("force_path_style", &self.force_path_style)
            .field("allow_http", &self.allow_http)
            .finish()
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let bind_addr = env::var("KOTATSU_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()?;

        let static_dir = env::var("KOTATSU_STATIC_DIR").ok().filter(|s| !s.is_empty());

        let s3 = S3Config::from_env();

        Ok(Self {
            bind_addr,
            static_dir,
            s3,
        })
    }
}

impl S3Config {
    /// Builds the S3 config from the environment. Returns `None` unless both a
    /// bucket and a cluster name are set.
    fn from_env() -> Option<Self> {
        let bucket = non_empty("KOTATSU_S3_BUCKET")?;
        let cluster = non_empty("KOTATSU_CLUSTER")?;
        let endpoint = non_empty("KOTATSU_S3_ENDPOINT");
        let allow_http = endpoint
            .as_deref()
            .map(|e| e.starts_with("http://"))
            .unwrap_or(false);

        Some(Self {
            bucket,
            cluster,
            region: non_empty("KOTATSU_S3_REGION").unwrap_or_else(|| "us-east-1".to_string()),
            access_key: non_empty("KOTATSU_S3_ACCESS_KEY"),
            secret_key: non_empty("KOTATSU_S3_SECRET_KEY"),
            force_path_style: env::var("KOTATSU_S3_FORCE_PATH_STYLE")
                .map(|v| v != "false" && v != "0")
                .unwrap_or(true),
            endpoint,
            allow_http,
        })
    }
}

fn non_empty(key: &str) -> Option<String> {
    env::var(key).ok().filter(|s| !s.is_empty())
}
