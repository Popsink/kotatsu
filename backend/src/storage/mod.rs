//! Object-storage access layer (S3 and GCS).
//!
//! Reads Tansu's native object storage directly via `object_store`. No Kafka
//! client, no broker. Every method is invoked on-demand from an API handler;
//! there are no background tasks or timers here.

mod cluster;
mod error;
mod groups;
mod keys;
mod model;
mod reader;
mod topics;

pub use cluster::ClusterSummary;
pub use error::StorageError;
pub use groups::{ConsumingGroup, GroupDetailView, GroupOffset, GroupSummary};
pub use keys::Keys;
pub use model::{decode_batch, BatchHeader, DecodedRecord, OffsetSpec, RecordHeader, Watermark};
pub use topics::{PartitionInfo, TopicDetail, TopicSummary};

use std::sync::Arc;

use futures::StreamExt;
use object_store::{aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, path::Path, ObjectStore};
use serde::de::DeserializeOwned;

use crate::config::{S3Config, StorageProvider};

/// A configured, ready-to-read S3 source bound to a single Tansu cluster.
#[derive(Clone)]
pub struct StorageSource {
    store: Arc<dyn ObjectStore>,
    keys: Keys,
}

impl StorageSource {
    /// Builds the source from config. Does not touch the network — connectivity
    /// is verified lazily via [`StorageSource::check`].
    ///
    /// **S3 credentials**: when explicit static keys are configured they win;
    /// otherwise `object_store` resolves them from the ambient AWS chain —
    /// environment, web identity (IRSA), ECS/EKS Pod Identity container
    /// credentials, then the EC2/ECS instance role (IMDS).
    ///
    /// **GCS credentials**: `object_store` reads `GOOGLE_SERVICE_ACCOUNT` (JSON
    /// key content), `GOOGLE_SERVICE_ACCOUNT_PATH`, or
    /// `GOOGLE_APPLICATION_CREDENTIALS` from the environment; on GKE, Workload
    /// Identity is picked up automatically.
    pub fn from_config(cfg: &S3Config) -> anyhow::Result<Self> {
        let store: Arc<dyn ObjectStore> = match cfg.provider {
            StorageProvider::Gcs => Arc::new(
                GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&cfg.bucket)
                    .build()?,
            ),
            StorageProvider::S3 => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(&cfg.bucket)
                    .with_region(&cfg.region)
                    .with_virtual_hosted_style_request(!cfg.force_path_style);

                if let Some(endpoint) = &cfg.endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                if cfg.allow_http {
                    builder = builder.with_allow_http(true);
                }

                // Explicit static keys take precedence over the ambient
                // credential chain. Set both together so a partial config
                // never shadows it.
                if let (Some(key), Some(secret)) = (&cfg.access_key, &cfg.secret_key) {
                    builder = builder
                        .with_access_key_id(key)
                        .with_secret_access_key(secret);
                    if let Some(token) = &cfg.session_token {
                        builder = builder.with_token(token);
                    }
                }

                Arc::new(builder.build()?)
            }
        };

        Ok(Self {
            store,
            keys: Keys::new(&cfg.cluster),
        })
    }

    pub fn keys(&self) -> &Keys {
        &self.keys
    }

    /// The underlying object store, for modules that need raw reads (#9).
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Verifies the source is reachable and the configured cluster exists.
    ///
    /// Probes `meta.json`; if absent, falls back to listing the cluster prefix
    /// so we can tell "bucket reachable, cluster missing" from "unreachable".
    pub async fn check(&self) -> Result<(), StorageError> {
        let meta = self.keys.meta();
        match self.store.head(&meta).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => {
                let prefix = Path::from(format!("clusters/{}", self.keys.cluster()));
                match self.store.list(Some(&prefix)).next().await {
                    Some(Ok(_)) => Ok(()),
                    Some(Err(e)) => Err(StorageError::Unreachable(e.to_string())),
                    None => Err(StorageError::ClusterNotFound(
                        self.keys.cluster().to_string(),
                    )),
                }
            }
            Err(e) => Err(StorageError::Unreachable(e.to_string())),
        }
    }

    /// Fetches and deserializes a JSON object (meta.json, watermark.json, …).
    pub async fn get_json<T: DeserializeOwned>(&self, path: &Path) -> Result<T, StorageError> {
        let result = self
            .store
            .get(path)
            .await
            .map_err(|e| StorageError::from_object(e, path))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| StorageError::from_object(e, path))?;
        serde_json::from_slice(&bytes).map_err(|source| StorageError::Parse {
            path: path.clone(),
            source,
        })
    }
}
