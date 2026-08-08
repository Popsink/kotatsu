//! Topic listing and detail, read from per-topic metadata + watermarks.
//!
//! Topic names and specs come from `clusters/{cluster}/topic-metadata/{name}.json`
//! (Tansu's decomposed metadata), falling back to the legacy monolithic
//! `meta.json` for clusters not yet migrated. Stats are limited to what
//! watermarks give (low/high, approximate count) — never a full `.batch` scan.

use std::collections::BTreeMap;

use futures::future::try_join_all;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use super::{catalog, keys::Keys, model::Watermark, StorageError, StorageSource};
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
#[derive(Clone, Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub partitions: i32,
    /// Approximate message count = Σ(high − low) over partitions.
    pub messages: i64,
    /// On-disk size in S3 (compressed bytes of the record segments) across all
    /// partitions. `0` for an empty topic.
    pub storage_bytes: i64,
}

/// One node in the prefix tree (an org, env, or connector level), or a terminal
/// topic surfaced directly at a group level.
#[derive(Clone, Serialize)]
pub struct TreeNode {
    /// The path component at this level (org, env, or connector name).
    pub segment: String,
    /// Full dotted path from the root to this node — what the UI drills into.
    pub path: String,
    /// Number of distinct topics beneath this node.
    pub topics: usize,
    /// Whether this node has deeper structure to drill into. `false` marks a
    /// terminal node that is itself a complete topic.
    pub group: bool,
    /// The full topic name when this node is a terminal topic (`group == false`),
    /// so the UI links straight to its detail instead of drilling deeper.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

/// Per-partition offsets for the topic detail view.
#[derive(Serialize)]
pub struct PartitionInfo {
    pub partition: i32,
    pub low: i64,
    pub high: i64,
    pub messages: i64,
    /// On-disk size in S3 (compressed bytes of this partition's record
    /// segments). `0` when the partition has no batches.
    pub storage_bytes: i64,
}

/// Topic detail: partition table + totals + configuration.
#[derive(Serialize)]
pub struct TopicDetail {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub messages: i64,
    /// On-disk size in S3 (compressed bytes of the record segments) across all
    /// partitions. `0` for an empty topic.
    pub storage_bytes: i64,
    pub replication_factor: i32,
    pub configs: Vec<ConfigEntry>,
}

/// Prefix depth at which a tree path names a full connector (`org.env.conn`,
/// Tansu's coalescing prefix) — below this we group by component, at or beyond
/// it we list the connector's topics. Matches [`Keys::prefix_of`].
pub const CONNECTOR_DEPTH: usize = 3;

/// Groups topic names into the next tree level below `prefix` (the already-chosen
/// path components). Pure over the name index — no storage reads. A name shorter
/// than or diverging from `prefix` is skipped; a name that ends exactly at this
/// level is a terminal topic (linkable directly), otherwise its component here is
/// a navigable group.
fn group_level(names: &[String], prefix: &[&str]) -> Vec<TreeNode> {
    struct Acc {
        topics: usize,
        has_deeper: bool,
        terminal: Option<String>,
    }

    let depth = prefix.len();
    let mut groups: BTreeMap<String, Acc> = BTreeMap::new();
    for name in names {
        let comps: Vec<&str> = name.split('.').collect();
        if comps.len() <= depth || comps[..depth] != *prefix {
            continue;
        }
        let acc = groups.entry(comps[depth].to_string()).or_insert(Acc {
            topics: 0,
            has_deeper: false,
            terminal: None,
        });
        acc.topics += 1;
        if comps.len() > depth + 1 {
            acc.has_deeper = true;
        } else {
            acc.terminal = Some(name.clone());
        }
    }

    groups
        .into_iter()
        .map(|(segment, acc)| {
            let path = if prefix.is_empty() {
                segment.clone()
            } else {
                format!("{}.{}", prefix.join("."), segment)
            };
            TreeNode {
                path,
                segment,
                topics: acc.topics,
                group: acc.has_deeper,
                // A pure terminal (no deeper components) links straight to its
                // topic detail; a group (even if a same-named topic also exists)
                // is drilled into, where that topic reappears as a leaf.
                topic: if acc.has_deeper { None } else { acc.terminal },
            }
        })
        .collect()
}

/// Whether `name` is the topic `prefix` itself or lives under `prefix.` — the
/// membership test for a connector's leaf listing.
fn topic_under(name: &str, prefix: &str) -> bool {
    name.strip_prefix(prefix)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
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

    /// Per-partition on-disk size (compressed bytes of the record segments),
    /// computed from S3 object metadata in a single recursive listing of the
    /// topic's `partitions/` prefix — cheap (object `size`, no content scan) and
    /// one list call regardless of partition count. Only `.batch` segment objects
    /// count; the tiny `watermark.json` sidecars are excluded. Partitions with no
    /// batches are simply absent from the map (callers default them to `0`).
    async fn partition_storage_bytes(
        &self,
        topic: &str,
    ) -> Result<BTreeMap<i32, i64>, StorageError> {
        let prefix = self.keys().partitions_prefix(topic);
        let mut sizes: BTreeMap<i32, i64> = BTreeMap::new();
        let mut stream = self.store().list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            if meta
                .location
                .parts()
                .last()
                .is_none_or(|f| !f.as_ref().ends_with(".batch"))
            {
                continue;
            }
            if let Some(p) = Keys::partition_from_records_path(&meta.location) {
                *sizes.entry(p).or_insert(0) += meta.size as i64;
            }
        }
        Ok(sizes)
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

    /// Whether `topic`'s `cleanup.policy` names `compact`, read off the stored
    /// config — a substring test, so `compact,delete` counts, exactly as the
    /// broker's `topic_is_compacted` does.
    ///
    /// Only the routing fallback needs this (#92), for a topic with no
    /// `topic-routing/` pin. A topic whose metadata has gone (deleted under us)
    /// reads as not compacted, which is the derivation the broker would also land
    /// on, rather than failing a page render.
    pub(super) async fn topic_is_compacted(&self, topic: &str) -> Result<bool, StorageError> {
        let spec = match self.topic_spec(topic).await {
            Ok(spec) => spec,
            Err(StorageError::NotFound(_) | StorageError::TopicNotFound(_)) => return Ok(false),
            Err(err) => return Err(err),
        };

        Ok(spec.configs.iter().any(|config| {
            config.name == "cleanup.policy"
                && config
                    .value
                    .as_deref()
                    .is_some_and(|value| value.contains("compact"))
        }))
    }

    /// Lists topics (name, partition count, approximate message count), filtered
    /// and paginated. Specs and watermarks are read only for the returned page.
    pub async fn list_topics(&self, page: &Page) -> Result<Paged<TopicSummary>, StorageError> {
        let (names, total) = page.select(self.catalog_topic_names().await?);

        let mut items = Vec::with_capacity(names.len());
        for name in names {
            let summary = match catalog::cached_summary(&self.topic_catalog, &name) {
                Some(summary) => summary,
                None => {
                    let summary = self.compute_topic_summary(&name).await?;
                    catalog::store_summary(&self.topic_catalog, name, summary.clone());
                    summary
                }
            };
            items.push(summary);
        }
        Ok(Paged::new(items, total, page))
    }

    /// Lists the next prefix-tree level below `prefix` (empty = root): the
    /// distinct org / env / connector components, with a per-node topic count.
    /// Pure grouping over the cached name index — no per-node storage reads — so
    /// it stays cheap at 15k topics. Filtered and paginated on the component name.
    pub async fn topic_groups_at(
        &self,
        prefix: &str,
        page: &Page,
    ) -> Result<Paged<TreeNode>, StorageError> {
        let names = self.catalog_topic_names().await?;
        let parts: Vec<&str> = if prefix.is_empty() {
            Vec::new()
        } else {
            prefix.split('.').collect()
        };
        let (items, total) = page.select_by(group_level(&names, &parts), |n| &n.segment);
        Ok(Paged::new(items, total, page))
    }

    /// Lists the topics under a connector `prefix` (`org.env.conn`) — the leaf
    /// level of the tree — filtered by name and paginated, with per-row summaries
    /// computed (and cached) only for the returned page, exactly like
    /// [`list_topics`](Self::list_topics).
    pub async fn list_topics_under(
        &self,
        prefix: &str,
        page: &Page,
    ) -> Result<Paged<TopicSummary>, StorageError> {
        let under: Vec<String> = self
            .catalog_topic_names()
            .await?
            .into_iter()
            .filter(|n| topic_under(n, prefix))
            .collect();
        let (names, total) = page.select(under);

        let mut items = Vec::with_capacity(names.len());
        for name in names {
            let summary = match catalog::cached_summary(&self.topic_catalog, &name) {
                Some(summary) => summary,
                None => {
                    let summary = self.compute_topic_summary(&name).await?;
                    catalog::store_summary(&self.topic_catalog, name, summary.clone());
                    summary
                }
            };
            items.push(summary);
        }
        Ok(Paged::new(items, total, page))
    }

    /// The topic-name index, served from the short-TTL catalog cache and
    /// re-listed only on a miss (#84) — so listing and every debounced search
    /// keystroke filter an in-memory list instead of re-scanning `topic-metadata/`.
    async fn catalog_topic_names(&self) -> Result<Vec<String>, StorageError> {
        if let Some(names) = catalog::fresh_names(&self.topic_catalog) {
            return Ok(names);
        }
        let names = self.topic_names().await?;
        catalog::set_names(&self.topic_catalog, names.clone());
        Ok(names)
    }

    /// Computes one topic's list-row summary (partition count, approximate
    /// message count, on-disk bytes) from S3. Cached per row by [`list_topics`].
    async fn compute_topic_summary(&self, name: &str) -> Result<TopicSummary, StorageError> {
        let partitions = self.topic_spec(name).await?.num_partitions.max(0);
        let watermarks =
            try_join_all((0..partitions).map(|p| self.watermark_or_empty(name, p))).await?;
        let messages = watermarks.iter().map(Watermark::count).sum();
        let storage_bytes = self.partition_storage_bytes(name).await?.values().sum();
        Ok(TopicSummary {
            name: name.to_string(),
            partitions,
            messages,
            storage_bytes,
        })
    }

    /// Reads a topic's per-partition watermarks.
    pub async fn topic_detail(&self, name: &str) -> Result<TopicDetail, StorageError> {
        let spec = self.topic_spec(name).await?;
        let partitions = spec.num_partitions.max(0);
        let replication_factor = spec.replication_factor;
        let configs = spec.configs.clone();

        let watermarks =
            try_join_all((0..partitions).map(|p| self.watermark_or_empty(name, p))).await?;
        let storage = self.partition_storage_bytes(name).await?;

        let infos: Vec<PartitionInfo> = watermarks
            .into_iter()
            .enumerate()
            .map(|(p, wm)| PartitionInfo {
                partition: p as i32,
                low: wm.low,
                high: wm.high,
                messages: wm.count(),
                storage_bytes: storage.get(&(p as i32)).copied().unwrap_or(0),
            })
            .collect();

        let messages = infos.iter().map(|p| p.messages).sum();
        let storage_bytes = infos.iter().map(|p| p.storage_bytes).sum();
        Ok(TopicDetail {
            name: name.to_string(),
            partitions: infos,
            messages,
            storage_bytes,
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

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn groups_root_by_org() {
        let ns = names(&[
            "acme.prod.db2.orders",
            "acme.staging.mssql.stock",
            "globex.prod.oracle.audit",
        ]);
        let nodes = group_level(&ns, &[]);
        let segs: Vec<&str> = nodes.iter().map(|n| n.segment.as_str()).collect();
        assert_eq!(segs, ["acme", "globex"]); // BTreeMap-sorted
        let acme = &nodes[0];
        assert_eq!(acme.path, "acme");
        assert_eq!(acme.topics, 2);
        assert!(acme.group);
        assert!(acme.topic.is_none());
    }

    #[test]
    fn groups_below_a_prefix() {
        let ns = names(&[
            "acme.prod.db2.orders",
            "acme.prod.db2.customers",
            "acme.prod.mssql.stock",
            "globex.prod.oracle.audit", // filtered out
        ]);
        let nodes = group_level(&ns, &["acme", "prod"]);
        let segs: Vec<&str> = nodes.iter().map(|n| n.segment.as_str()).collect();
        assert_eq!(segs, ["db2", "mssql"]);
        assert_eq!(nodes[0].path, "acme.prod.db2");
        assert_eq!(nodes[0].topics, 2);
        assert!(nodes[0].group);
    }

    #[test]
    fn terminal_topic_surfaces_as_a_leaf_node() {
        // A topic shorter than org.env.conn terminates early and must be a
        // directly-linkable leaf, not a dead-end group.
        let ns = names(&["orders", "acme.prod.db2.stock"]);
        let nodes = group_level(&ns, &[]);
        let orders = nodes.iter().find(|n| n.segment == "orders").unwrap();
        assert!(!orders.group);
        assert_eq!(orders.topic.as_deref(), Some("orders"));
        assert_eq!(orders.topics, 1);
        let acme = nodes.iter().find(|n| n.segment == "acme").unwrap();
        assert!(acme.group);
        assert!(acme.topic.is_none());
    }

    #[test]
    fn topic_under_matches_self_and_children_only() {
        assert!(topic_under("acme.prod.db2", "acme.prod.db2")); // exact
        assert!(topic_under("acme.prod.db2.orders", "acme.prod.db2")); // child
        assert!(!topic_under("acme.prod.db2x", "acme.prod.db2")); // not a boundary
        assert!(!topic_under("acme.prod.mssql", "acme.prod.db2"));
    }

    #[test]
    fn parses_real_meta_topics() {
        let meta: MetaRaw = serde_json::from_slice(META).unwrap();
        let orders = meta.topics.get("orders").expect("orders topic present");
        assert_eq!(orders.topic.num_partitions, 1);
    }

    #[tokio::test]
    async fn partition_storage_bytes_sums_batch_objects_per_partition() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        // Two batches on partition 0, one on partition 1.
        for (part, base, len) in [(0, 0_i64, 100_usize), (0, 10, 55), (1, 0, 40)] {
            store
                .put(
                    &src.keys().batch("orders", part, base),
                    PutPayload::from(vec![0u8; len]),
                )
                .await
                .unwrap();
        }
        // A watermark.json sidecar must NOT be counted as storage.
        store
            .put(
                &src.keys().watermark("orders", 0),
                PutPayload::from(vec![0u8; 999]),
            )
            .await
            .unwrap();

        let sizes = src.partition_storage_bytes("orders").await.unwrap();
        assert_eq!(sizes.get(&0), Some(&155)); // 100 + 55, watermark excluded
        assert_eq!(sizes.get(&1), Some(&40));
        // An empty topic yields an empty map (callers default to 0).
        assert!(src
            .partition_storage_bytes("empty")
            .await
            .unwrap()
            .is_empty());
    }

    /// #84: once warmed, listing and search are served from the in-process
    /// catalog within the TTL — no S3 re-scan and no per-row re-fetch. Proven by
    /// deleting every object after warming and still getting the right answer.
    #[tokio::test]
    async fn catalog_serves_listing_and_search_from_cache() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        let meta = serde_json::json!({
            "topic": { "name": "orders", "num_partitions": 1, "replication_factor": 1, "configs": [] }
        });
        store
            .put(
                &src.keys().topic_metadata("orders"),
                PutPayload::from(serde_json::to_vec(&meta).unwrap()),
            )
            .await
            .unwrap();
        store
            .put(
                &src.keys().watermark("orders", 0),
                PutPayload::from(br#"{"low":0,"high":5,"timestamps":null}"#.to_vec()),
            )
            .await
            .unwrap();

        // Warm the cache.
        let first = src.list_topics(&Page::new(None, 50, 0)).await.unwrap();
        assert_eq!(first.total, 1);
        assert_eq!(first.items[0].messages, 5);

        // Remove every object; within the TTL the catalog still answers.
        for p in [
            src.keys().topic_metadata("orders"),
            src.keys().watermark("orders", 0),
        ] {
            store.delete(&p).await.unwrap();
        }
        let cached = src.list_topics(&Page::new(None, 50, 0)).await.unwrap();
        assert_eq!(cached.total, 1, "name index served from cache");
        assert_eq!(cached.items[0].messages, 5, "row summary served from cache");

        // Search resolves against the cached name index (no re-scan).
        let hit = src
            .list_topics(&Page::new(Some("ord".into()), 50, 0))
            .await
            .unwrap();
        assert_eq!(hit.total, 1);
        let miss = src
            .list_topics(&Page::new(Some("zzz".into()), 50, 0))
            .await
            .unwrap();
        assert_eq!(miss.total, 0);
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
