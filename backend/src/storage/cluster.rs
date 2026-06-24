//! Cluster listing and `meta.json` summary.
//!
//! Replaces the old broker view — there is no broker. We surface the cluster
//! names present in the bucket and a summary of each cluster's `meta.json`
//! (`{ producers, topics, transactions }`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{keys::Keys, StorageError, StorageSource};

/// Counts the producer/transaction maps in `meta.json`. Topics are no longer
/// sourced here — they live in per-topic objects (see [`StorageSource::topic_names`]).
#[derive(Default, Deserialize)]
struct MetaCounts {
    #[serde(default)]
    producers: BTreeMap<String, serde_json::Value>,
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

    /// Summarizes the configured cluster: topic count from the per-topic objects,
    /// producer/transaction counts from `meta.json` (absent on a fresh cluster
    /// with no producers/transactions yet → zero).
    pub async fn cluster_summary(&self) -> Result<ClusterSummary, StorageError> {
        let topics = self.topic_names().await?.len();
        let counts = match self.get_json::<MetaCounts>(&self.keys().meta()).await {
            Ok(counts) => counts,
            Err(StorageError::NotFound(_)) => MetaCounts::default(),
            Err(err) => return Err(err),
        };
        Ok(ClusterSummary {
            cluster: self.keys().cluster().to_string(),
            topics,
            producers: counts.producers.len(),
            transactions: counts.transactions.len(),
        })
    }
}
