//! Shared application state.

use crate::{schema::SchemaRegistry, storage::StorageSource};

#[derive(Clone)]
pub struct AppState {
    /// The configured S3 source, if any.
    pub source: Option<StorageSource>,
    /// Source metadata for display (never contains credentials).
    pub source_info: Option<SourceInfo>,
    /// Schema registry (Kora) client, if configured.
    pub registry: Option<SchemaRegistry>,
}

/// Non-secret description of the configured source, surfaced via `/api/source`.
#[derive(Clone, serde::Serialize)]
pub struct SourceInfo {
    pub bucket: String,
    pub cluster: String,
    pub endpoint: Option<String>,
    pub region: String,
}
