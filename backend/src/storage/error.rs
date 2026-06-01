//! Storage-layer errors.

use object_store::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("no S3 source configured")]
    NotConfigured,

    #[error("S3 source is unreachable: {0}")]
    Unreachable(String),

    #[error("cluster '{0}' not found in the bucket")]
    ClusterNotFound(String),

    #[error("object not found: {0}")]
    NotFound(Path),

    #[error("failed to parse object {path}: {source}")]
    Parse {
        path: Path,
        #[source]
        source: serde_json::Error,
    },

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

impl StorageError {
    /// Maps an `object_store` error, turning a missing object into `NotFound`.
    pub fn from_object(err: object_store::Error, path: &Path) -> Self {
        match err {
            object_store::Error::NotFound { .. } => Self::NotFound(path.clone()),
            other => Self::ObjectStore(other),
        }
    }
}
