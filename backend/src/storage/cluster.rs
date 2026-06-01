//! Cluster listing and `meta.json` summary.
//!
//! Replaces the old broker view — there is no broker. We surface the cluster
//! names present in the bucket and a summary of each cluster's `meta.json`
//! (`{ producers, topics, transactions }`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{keys::Keys, StorageError, StorageSource};

/// Counts the three top-level maps in `meta.json`.
#[derive(Deserialize)]
struct MetaCounts {
    #[serde(default)]
    producers: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    topics: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    transactions: BTreeMap<String, serde_json::Value>,
}

/// Summary of a cluster's `meta.json`.
#[derive(Serialize)]
pub struct ClusterSummary {
    pub cluster: String,
    pub topics: usize,
    pub producers: usize,
    pub transactions: usize,
}

impl StorageSource {
    /// Lists cluster names present in the bucket (the `clusters/{name}/` prefixes).
    pub async fn list_clusters(&self) -> Result<Vec<String>, StorageError> {
        let listed = self
            .store()
            .list_with_delimiter(Some(&Keys::clusters_root()))
            .await?;

        let mut names: Vec<String> = listed
            .common_prefixes
            .iter()
            .filter_map(|p| p.parts().last().map(|seg| seg.as_ref().to_string()))
            .collect();
        names.sort();
        Ok(names)
    }

    /// Summarizes the configured cluster's `meta.json`.
    pub async fn cluster_summary(&self) -> Result<ClusterSummary, StorageError> {
        let counts: MetaCounts = self.get_json(&self.keys().meta()).await?;
        Ok(ClusterSummary {
            cluster: self.keys().cluster().to_string(),
            topics: counts.topics.len(),
            producers: counts.producers.len(),
            transactions: counts.transactions.len(),
        })
    }
}
