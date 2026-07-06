//! Topic listing and detail, read from per-topic metadata + watermarks.
//!
//! Topic names and specs come from `clusters/{cluster}/topic-metadata/{name}.json`
//! (Tansu's decomposed metadata), falling back to the legacy monolithic
//! `meta.json` for clusters not yet migrated. Stats are limited to what
//! watermarks give (low/high, approximate count) — never a full `.batch` scan.

use std::collections::BTreeMap;

use futures::future::try_join_all;
use serde::{Deserialize, Serialize};

use super::{model::Watermark, StorageError, StorageSource};
use crate::pagination::{Page, Paged};

/// Minimal view of the legacy `meta.json` — only the topics map (fallback for
/// unmigrated clusters).
#[derive(Deserialize)]
struct MetaRaw {
    #[serde(default)]
    topics: BTreeMap<String, TopicEntry>,
}

/// One topic's metadata. Shared shape between a per-topic
/// `topic-metadata/{name}.json` object and a `meta.json` topics entry — both are
/// `{ id?, topic: { … } }`, and the unused `id` is ignored on deserialize.
#[derive(Deserialize)]
struct TopicEntry {
    topic: TopicSpec,
}

#[derive(Deserialize)]
struct TopicSpec {
    #[serde(default)]
    num_partitions: i32,
    #[serde(default)]
    replication_factor: i32,
    #[serde(default)]
    configs: Vec<ConfigEntry>,
}

/// A topic config entry from `meta.json` (Kafka `CreatableTopicConfig`).
#[derive(Clone, Deserialize, Serialize)]
pub struct ConfigEntry {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
}

/// One row in the topics list.
#[derive(Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub partitions: i32,
    /// Approximate message count = Σ(high − low) over partitions.
    pub messages: i64,
}

/// Per-partition offsets for the topic detail view.
#[derive(Serialize)]
pub struct PartitionInfo {
    pub partition: i32,
    pub low: i64,
    pub high: i64,
    pub messages: i64,
}

/// Topic detail: partition table + totals + configuration.
#[derive(Serialize)]
pub struct TopicDetail {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub messages: i64,
    pub replication_factor: i32,
    pub configs: Vec<ConfigEntry>,
}

impl StorageSource {
    /// Reads a partition watermark, treating a missing file (no data produced
    /// yet) as an empty partition rather than an error.
    pub(super) async fn watermark_or_empty(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Watermark, StorageError> {
        match self.watermark(topic, partition).await {
            Ok(wm) => Ok(wm),
            Err(StorageError::NotFound(_)) => Ok(Watermark { low: 0, high: 0 }),
            Err(err) => Err(err),
        }
    }

    /// Topic names for the configured cluster.
    ///
    /// Primary source is the per-topic metadata prefix
    /// (`topic-metadata/{name}.json`, written by Tansu's decomposed metadata). A
    /// cluster not yet migrated to per-topic objects (empty prefix) falls back to
    /// the legacy monolithic `meta.json` topics map.
    pub(super) async fn topic_names(&self) -> Result<Vec<String>, StorageError> {
        let listed = self
            .store()
            .list_with_delimiter(Some(&self.keys().topic_metadata_prefix()))
            .await?;

        let mut names: Vec<String> = listed
            .objects
            .iter()
            .filter_map(|o| {
                o.location
                    .filename()
                    .and_then(|f| f.strip_suffix(".json"))
                    .map(str::to_string)
            })
            .collect();

        if names.is_empty() {
            match self.get_json::<MetaRaw>(&self.keys().meta()).await {
                Ok(meta) => names = meta.topics.into_keys().collect(),
                Err(StorageError::NotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }

        names.sort();
        Ok(names)
    }

    /// A topic's partition count, or [`StorageError::TopicNotFound`] if the
    /// topic has no metadata object. Used to validate `partition` query params
    /// before touching the storage layout (so an out-of-range partition yields a
    /// clean error instead of a leaked object key — #63).
    pub async fn topic_partitions(&self, name: &str) -> Result<i32, StorageError> {
        Ok(self.topic_spec(name).await?.num_partitions.max(0))
    }

    /// A topic's spec, preferring the per-topic object and falling back to the
    /// legacy `meta.json` entry for an unmigrated cluster.
    async fn topic_spec(&self, name: &str) -> Result<TopicSpec, StorageError> {
        match self
            .get_json::<TopicEntry>(&self.keys().topic_metadata(name))
            .await
        {
            Ok(entry) => Ok(entry.topic),
            Err(StorageError::NotFound(_)) => {
                let mut meta: MetaRaw = self.get_json(&self.keys().meta()).await?;
                meta.topics
                    .remove(name)
                    .map(|entry| entry.topic)
                    .ok_or_else(|| StorageError::TopicNotFound(name.to_string()))
            }
            Err(err) => Err(err),
        }
    }

    /// Lists topics (name, partition count, approximate message count), filtered
    /// and paginated. Specs and watermarks are read only for the returned page.
    pub async fn list_topics(&self, page: &Page) -> Result<Paged<TopicSummary>, StorageError> {
        let (names, total) = page.select(self.topic_names().await?);

        let mut items = Vec::with_capacity(names.len());
        for name in names {
            let partitions = self.topic_spec(&name).await?.num_partitions.max(0);
            let watermarks =
                try_join_all((0..partitions).map(|p| self.watermark_or_empty(&name, p))).await?;
            let messages = watermarks.iter().map(Watermark::count).sum();
            items.push(TopicSummary {
                name,
                partitions,
                messages,
            });
        }
        Ok(Paged::new(items, total, page))
    }

    /// Reads a topic's per-partition watermarks.
    pub async fn topic_detail(&self, name: &str) -> Result<TopicDetail, StorageError> {
        let spec = self.topic_spec(name).await?;
        let partitions = spec.num_partitions.max(0);
        let replication_factor = spec.replication_factor;
        let configs = spec.configs.clone();

        let watermarks =
            try_join_all((0..partitions).map(|p| self.watermark_or_empty(name, p))).await?;

        let infos: Vec<PartitionInfo> = watermarks
            .into_iter()
            .enumerate()
            .map(|(p, wm)| PartitionInfo {
                partition: p as i32,
                low: wm.low,
                high: wm.high,
                messages: wm.count(),
            })
            .collect();

        let messages = infos.iter().map(|p| p.messages).sum();
        Ok(TopicDetail {
            name: name.to_string(),
            partitions: infos,
            messages,
            replication_factor,
            configs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real meta.json produced by Tansu.
    const META: &[u8] = include_bytes!("../../tests/fixtures/meta.json");

    #[test]
    fn parses_real_meta_topics() {
        let meta: MetaRaw = serde_json::from_slice(META).unwrap();
        let orders = meta.topics.get("orders").expect("orders topic present");
        assert_eq!(orders.topic.num_partitions, 1);
    }

    #[test]
    fn parses_per_topic_object() {
        // Shape of a `topic-metadata/{name}.json` object (Tansu's TopicMetadata
        // { id, topic }); the `id` is ignored, the topic spec is extracted.
        let json = serde_json::json!({
            "id": "019ec674-8c31-70f0-abf1-7a0a136214bd",
            "topic": {
                "name": "orders",
                "num_partitions": 3,
                "replication_factor": 1,
                "configs": [{ "name": "cleanup.policy", "value": "delete" }]
            }
        });
        let entry: TopicEntry = serde_json::from_value(json).unwrap();
        assert_eq!(entry.topic.num_partitions, 3);
        assert_eq!(entry.topic.replication_factor, 1);
        assert_eq!(entry.topic.configs.len(), 1);
    }
}
