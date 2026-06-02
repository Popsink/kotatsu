//! Topic listing and detail, read from `meta.json` + per-partition watermarks.
//!
//! A topic is a directory under `clusters/{cluster}/topics/`; its partition
//! count comes from `meta.json`. Stats are limited to what watermarks give
//! (low/high, approximate count) — never a full `.batch` scan.

use std::collections::BTreeMap;

use futures::future::try_join_all;
use serde::{Deserialize, Serialize};

use super::{model::Watermark, StorageError, StorageSource};

/// Minimal view of `meta.json` — only the topics map.
#[derive(Deserialize)]
struct MetaRaw {
    #[serde(default)]
    topics: BTreeMap<String, TopicEntry>,
}

#[derive(Deserialize)]
struct TopicEntry {
    topic: TopicSpec,
}

#[derive(Deserialize)]
struct TopicSpec {
    #[serde(default)]
    num_partitions: i32,
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

/// Topic detail: partition table + totals.
#[derive(Serialize)]
pub struct TopicDetail {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub messages: i64,
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

    /// Lists all topics with their partition count and approximate message count.
    pub async fn list_topics(&self) -> Result<Vec<TopicSummary>, StorageError> {
        let meta: MetaRaw = self.get_json(&self.keys().meta()).await?;

        let mut summaries = Vec::with_capacity(meta.topics.len());
        for (name, entry) in meta.topics {
            let partitions = entry.topic.num_partitions.max(0);
            let watermarks =
                try_join_all((0..partitions).map(|p| self.watermark_or_empty(&name, p))).await?;
            let messages = watermarks.iter().map(Watermark::count).sum();
            summaries.push(TopicSummary {
                name,
                partitions,
                messages,
            });
        }
        summaries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(summaries)
    }

    /// Reads a topic's per-partition watermarks.
    pub async fn topic_detail(&self, name: &str) -> Result<TopicDetail, StorageError> {
        let meta: MetaRaw = self.get_json(&self.keys().meta()).await?;
        let entry = meta
            .topics
            .get(name)
            .ok_or_else(|| StorageError::TopicNotFound(name.to_string()))?;
        let partitions = entry.topic.num_partitions.max(0);

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
}
