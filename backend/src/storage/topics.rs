//! Topic listing and detail, read from per-topic metadata + watermarks.
//!
//! Topic names and specs come from `clusters/{cluster}/topic-metadata/{name}.json`
//! (Tansu's decomposed metadata), falling back to the legacy monolithic
//! `meta.json` for clusters not yet migrated. Stats are limited to what
//! watermarks give (low/high, approximate count) plus the segment footers
//! (per-sub-stream byte spans) — never a scan of record content.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use futures::{future::try_join_all, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use super::{
    catalog,
    model::Watermark,
    segview::{substream_segments, PrefixFooters, SegView, TopicSegments},
    StorageError, StorageSource, FANOUT,
};
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
///
/// The stats are absent when the listing was asked for without them (#130):
/// they are the only part of a row that costs object-store work beyond the
/// metadata, so a caller after names — the quick-jump palette, the first paint
/// of the Topics page — does not wait on them.
#[derive(Clone, Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub partitions: i32,
    #[serde(flatten)]
    pub stats: Option<TopicStats>,
}

/// The two columns of a topic row that are folded from its segments.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TopicStats {
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
/// it we list the connector's topics. Matches `Keys::prefix_of` — the grouping is
/// over topic *names*, unaffected by where their records are routed.
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
            Err(StorageError::NotFound(_)) => Ok(Watermark {
                low: 0,
                high: 0,
                served_end: None,
            }),
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

    /// Lists topics (name, partition count, and with `stats` the approximate
    /// message count and on-disk size), filtered and paginated. Only the returned
    /// page is read.
    pub async fn list_topics(
        &self,
        page: &Page,
        stats: bool,
    ) -> Result<Paged<TopicSummary>, StorageError> {
        let (names, total) = page.select(self.catalog_topic_names().await?);
        let items = self.topic_summaries(names, stats).await?;
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
        stats: bool,
    ) -> Result<Paged<TopicSummary>, StorageError> {
        let under: Vec<String> = self
            .catalog_topic_names()
            .await?
            .into_iter()
            .filter(|n| topic_under(n, prefix))
            .collect();
        let (names, total) = page.select(under);
        let items = self.topic_summaries(names, stats).await?;
        Ok(Paged::new(items, total, page))
    }

    /// Row summaries for one page of names, in name order: from the catalog
    /// where it can answer, computed `FANOUT` rows at a time otherwise.
    ///
    /// With `stats`, each prefix the uncached rows are routed under is listed
    /// **once**, up front, and every row under it is folded from that one
    /// listing. A leaf page is one connector's topics, so its 50 rows share a
    /// single prefix — which they used to list `P + 1` times each.
    async fn topic_summaries(
        &self,
        names: Vec<String>,
        stats: bool,
    ) -> Result<Vec<TopicSummary>, StorageError> {
        let cached: Vec<Option<TopicSummary>> = names
            .iter()
            .map(|name| catalog::cached_summary(&self.topic_catalog, name))
            .collect();
        // A row cached with stats answers either request; one cached without
        // them answers only a request that does not want them.
        let answers = |row: &TopicSummary| !stats || row.stats.is_some();

        // Footers per prefix, for the span of this listing only — the same
        // scope as the lag listing's memoised highs.
        let footers: HashMap<String, PrefixFooters> = if stats {
            // Owned names: a stream over borrows trips the `Send` bound the
            // router puts on the handler's future.
            let to_compute: Vec<String> = names
                .iter()
                .zip(&cached)
                .filter(|(_, row)| !row.as_ref().is_some_and(answers))
                .map(|(name, _)| name.clone())
                .collect();
            let prefixes: BTreeSet<String> = futures::stream::iter(to_compute)
                .map(
                    |name| async move { Ok::<_, StorageError>(self.route_of(&name).await?.prefix) },
                )
                .buffered(FANOUT)
                .try_collect()
                .await?;
            // Prefixes side by side, each reading its footers `FANOUT` at a
            // time: the bounds multiply on a cold pass only, as they do on any
            // multi-partition read, and a footer once read is cached for good.
            futures::stream::iter(prefixes)
                .map(|prefix| async move {
                    let listed = self.prefix_footers(&prefix).await?;
                    Ok::<_, StorageError>((prefix, listed))
                })
                .buffer_unordered(FANOUT)
                .try_collect()
                .await?
        } else {
            HashMap::new()
        };

        // `buffered`, not `buffer_unordered`: a name-ordered page must come back
        // in name order.
        futures::stream::iter(names.into_iter().zip(cached))
            .map(|(name, row)| {
                let footers = &footers;
                async move {
                    let partitions = match row {
                        Some(row) if answers(&row) => {
                            return Ok(if stats {
                                row
                            } else {
                                TopicSummary { stats: None, ..row }
                            });
                        }
                        // Cached without stats, by the stats-less listing the
                        // Topics page paints first: its partition count still
                        // holds, so the metadata is not read a second time.
                        Some(row) => row.partitions,
                        None => self.topic_spec(&name).await?.num_partitions.max(0),
                    };
                    let summary = TopicSummary {
                        stats: if stats {
                            Some(self.topic_stats(&name, partitions, footers).await?)
                        } else {
                            None
                        },
                        name,
                        partitions,
                    };
                    catalog::store_summary(
                        &self.topic_catalog,
                        summary.name.clone(),
                        summary.clone(),
                    );
                    Ok(summary)
                }
            })
            .buffered(FANOUT)
            .try_collect()
            .await
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

    /// One topic row's approximate message count and on-disk bytes, folded from
    /// the page's prefix footers.
    async fn topic_stats(
        &self,
        name: &str,
        partitions: i32,
        footers: &HashMap<String, PrefixFooters>,
    ) -> Result<TopicStats, StorageError> {
        let route = self.route_of(name).await?;
        let segments = footers
            .get(&route.prefix)
            .map(|f| substream_segments(f, &route.prefix, route.substream(name)))
            .unwrap_or_default();
        let watermarks = self
            .partition_watermarks(name, partitions, &segments)
            .await?;
        Ok(TopicStats {
            messages: watermarks.iter().map(Watermark::count).sum(),
            storage_bytes: segments.bytes.values().sum(),
        })
    }

    /// Every partition's watermark over views already folded from one listing,
    /// so only the `watermark.json` hints are left to read — concurrently.
    async fn partition_watermarks(
        &self,
        name: &str,
        partitions: i32,
        segments: &TopicSegments,
    ) -> Result<Vec<Watermark>, StorageError> {
        let empty = SegView::default();
        try_join_all(
            (0..partitions)
                .map(|p| self.watermark_over(name, p, segments.views.get(&p).unwrap_or(&empty))),
        )
        .await
    }

    /// Reads a topic's per-partition watermarks.
    pub async fn topic_detail(&self, name: &str) -> Result<TopicDetail, StorageError> {
        let spec = self.topic_spec(name).await?;
        let partitions = spec.num_partitions.max(0);
        let replication_factor = spec.replication_factor;
        let configs = spec.configs.clone();

        let segments = self.topic_segments(name).await?;
        let watermarks = self
            .partition_watermarks(name, partitions, &segments)
            .await?;

        let infos: Vec<PartitionInfo> = watermarks
            .into_iter()
            .enumerate()
            .map(|(p, wm)| PartitionInfo {
                partition: p as i32,
                low: wm.low,
                high: wm.high,
                messages: wm.count(),
                storage_bytes: segments.bytes.get(&(p as i32)).copied().unwrap_or(0),
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

    /// #93: on-disk size comes from the footers' per-sub-stream byte spans, so a
    /// segment-backed topic reports a size at all (listing `.batch` objects under
    /// `partitions/` reported 0 for every topic on a current cluster), and it is
    /// charged its own share of a shared segment — never a co-tenant's bytes.
    #[tokio::test]
    async fn storage_bytes_are_the_substream_spans_in_shared_segments() {
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        let topic = "acme.prod.db2.orders";
        let other = "acme.prod.db2.stock"; // co-tenant of the same prefix

        // Two segments under `acme.prod.db2`: our topic holds 100 + 55 bytes on
        // partition 0 and 40 on partition 1; the co-tenant holds 999.
        for (seq, regions) in [
            (
                0u64,
                vec![
                    (topic, 0, 0_i64, 1_i64, vec![0u8; 100], 1_i64),
                    (other, 0, 0, 1, vec![0u8; 999], 1),
                ],
            ),
            (
                1,
                vec![
                    (topic, 0, 1, 1, vec![0u8; 55], 2),
                    (topic, 1, 0, 1, vec![0u8; 40], 2),
                ],
            ),
        ] {
            let regions: Vec<super::super::segment::TestRegion> = regions
                .iter()
                .map(|(t, p, base, count, bytes, ts)| {
                    (*t, *p, *base, *count, bytes.as_slice(), *ts, None)
                })
                .collect();
            let bytes = super::super::segment::build_test_segment(3, 1, &regions);
            store
                .put(
                    &src.keys().segment("acme.prod.db2", seq),
                    PutPayload::from(bytes.to_vec()),
                )
                .await
                .unwrap();
        }

        let sizes = src.topic_segments(topic).await.unwrap().bytes;
        assert_eq!(
            sizes.get(&0),
            Some(&155),
            "100 + 55, the co-tenant excluded"
        );
        assert_eq!(sizes.get(&1), Some(&40));
        // A topic with no segment yields an empty map (callers default to 0).
        assert!(src
            .topic_segments("acme.prod.other.empty")
            .await
            .unwrap()
            .bytes
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
        // Five messages: one segment slice at offsets 0..5. The count comes from
        // the footer, so `watermark.json` — lazily persisted, `{"high":null}` on a
        // live partition — is seeded as it actually looks.
        store
            .put(
                &src.keys().watermark("orders", 0),
                PutPayload::from(br#"{"high":null}"#.to_vec()),
            )
            .await
            .unwrap();
        let segment = super::super::segment::build_test_segment(
            3,
            1,
            &[("orders", 0, 0, 5, &[0u8; 32], 1, None)],
        );
        store
            .put(
                &src.keys().segment("orders", 0),
                PutPayload::from(segment.to_vec()),
            )
            .await
            .unwrap();

        // Warm the cache.
        let first = src
            .list_topics(&Page::new(None, 50, 0), true)
            .await
            .unwrap();
        assert_eq!(first.total, 1);
        assert_eq!(first.items[0].stats, stats(5, 32));

        // Remove every object; within the TTL the catalog still answers.
        for p in [
            src.keys().topic_metadata("orders"),
            src.keys().watermark("orders", 0),
            src.keys().segment("orders", 0),
        ] {
            store.delete(&p).await.unwrap();
        }
        let cached = src
            .list_topics(&Page::new(None, 50, 0), true)
            .await
            .unwrap();
        assert_eq!(cached.total, 1, "name index served from cache");
        assert_eq!(
            cached.items[0].stats,
            stats(5, 32),
            "row summary served from cache"
        );

        // Search resolves against the cached name index (no re-scan).
        let hit = src
            .list_topics(&Page::new(Some("ord".into()), 50, 0), true)
            .await
            .unwrap();
        assert_eq!(hit.total, 1);
        let miss = src
            .list_topics(&Page::new(Some("zzz".into()), 50, 0), true)
            .await
            .unwrap();
        assert_eq!(miss.total, 0);
    }

    /// The in-memory store, counting the LISTs made under a segment prefix and
    /// the GETs of a topic's metadata.
    #[derive(Debug)]
    struct CountingStore {
        inner: object_store::memory::InMemory,
        segment_lists: std::sync::atomic::AtomicUsize,
        metadata_gets: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if location.as_ref().contains("/topic-metadata/") {
                self.metadata_gets
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }
        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            if prefix.is_some_and(|p| p.as_ref().ends_with("/segments")) {
                self.segment_lists
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    /// Three two-partition topics sharing the `acme.prod.db2` prefix: topic `i`
    /// holds `i + 1` records of 10 bytes on partition 0 and one of 7 bytes on
    /// partition 1, spread over two segments. Each is pinned to its prefix, as
    /// every topic created since Popsink/tansu#236 is, so resolving its route
    /// does not read its metadata.
    async fn seed_connector(store: &CountingStore, src: &StorageSource) -> [&'static str; 3] {
        use object_store::{ObjectStore, PutPayload};
        let topics = ["acme.prod.db2.a", "acme.prod.db2.b", "acme.prod.db2.c"];
        for topic in topics {
            let meta = serde_json::json!({ "topic": { "num_partitions": 2 } });
            let pin = serde_json::json!({ "prefix": "acme.prod.db2" });
            for (path, body) in [
                (src.keys().topic_metadata(topic), meta),
                (src.keys().topic_routing(topic), pin),
            ] {
                store
                    .put(&path, PutPayload::from(serde_json::to_vec(&body).unwrap()))
                    .await
                    .unwrap();
            }
        }
        let bytes = [vec![0u8; 10], vec![0u8; 20], vec![0u8; 30], vec![0u8; 7]];
        let seg0: Vec<super::super::segment::TestRegion> = topics
            .iter()
            .enumerate()
            .map(|(i, t)| (*t, 0, 0, i as i64 + 1, bytes[i].as_slice(), 1, None))
            .collect();
        let seg1: Vec<super::super::segment::TestRegion> = topics
            .iter()
            .map(|t| (*t, 1, 0, 1, bytes[3].as_slice(), 2, None))
            .collect();
        for (seq, regions) in [(0, seg0), (1, seg1)] {
            let segment = super::super::segment::build_test_segment(3, 1, &regions);
            store
                .put(
                    &src.keys().segment("acme.prod.db2", seq),
                    PutPayload::from(segment.to_vec()),
                )
                .await
                .unwrap();
        }
        topics
    }

    fn counting_source() -> (std::sync::Arc<CountingStore>, StorageSource) {
        let store = std::sync::Arc::new(CountingStore {
            inner: object_store::memory::InMemory::new(),
            segment_lists: Default::default(),
            metadata_gets: Default::default(),
        });
        let src = StorageSource::with_store(store.clone(), "c");
        (store, src)
    }

    fn segment_lists(store: &CountingStore) -> usize {
        store
            .segment_lists
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn metadata_gets(store: &CountingStore) -> usize {
        store
            .metadata_gets
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn stats(messages: i64, storage_bytes: i64) -> Option<TopicStats> {
        Some(TopicStats {
            messages,
            storage_bytes,
        })
    }

    /// A leaf page lists its connector's segment prefix once, and every
    /// row's count and size are folded from that one listing — it used to be
    /// `P + 1` listings per row, here 3 × (2 + 1) = 9.
    #[tokio::test]
    async fn a_leaf_page_lists_its_prefix_once() {
        let (store, src) = counting_source();
        let topics = seed_connector(&store, &src).await;

        let page = src
            .list_topics_under("acme.prod.db2", &Page::new(None, 50, 0), true)
            .await
            .unwrap();

        assert_eq!(segment_lists(&store), 1);
        let rows: Vec<(&str, Option<TopicStats>)> = page
            .items
            .iter()
            .map(|r| (r.name.as_str(), r.stats.clone()))
            .collect();
        assert_eq!(
            rows,
            [
                (topics[0], stats(1 + 1, 10 + 7)),
                (topics[1], stats(2 + 1, 20 + 7)),
                (topics[2], stats(3 + 1, 30 + 7)),
            ]
        );
    }

    /// Without stats a page reads no segment at all. A row cached that way does
    /// not pass for one with stats on the next request, but its partition count
    /// is reused: the Topics page's second request does not read the metadata
    /// again.
    #[tokio::test]
    async fn stats_are_opt_out_and_a_stats_less_row_only_lends_its_partitions() {
        let (store, src) = counting_source();
        seed_connector(&store, &src).await;
        let page = Page::new(None, 50, 0);

        let bare = src.list_topics(&page, false).await.unwrap();
        assert_eq!(segment_lists(&store), 0);
        assert_eq!(metadata_gets(&store), 3);
        assert!(bare
            .items
            .iter()
            .all(|r| r.partitions == 2 && r.stats.is_none()));

        let full = src.list_topics(&page, true).await.unwrap();
        assert_eq!(segment_lists(&store), 1);
        assert_eq!(metadata_gets(&store), 3, "partitions taken from the cache");
        assert_eq!(full.items[0].partitions, 2);
        assert_eq!(full.items[0].stats, stats(2, 17));
        // The wire shape: both figures at the row's top level, or neither.
        assert_eq!(
            serde_json::to_value(&full.items[0]).unwrap(),
            serde_json::json!({
                "name": "acme.prod.db2.a",
                "partitions": 2,
                "messages": 2,
                "storage_bytes": 17,
            })
        );
        assert_eq!(
            serde_json::to_value(&bare.items[0]).unwrap(),
            serde_json::json!({ "name": "acme.prod.db2.a", "partitions": 2 })
        );

        // A row cached with stats answers a stats-less request, figures dropped.
        let again = src.list_topics(&page, false).await.unwrap();
        assert_eq!(segment_lists(&store), 1, "served from the catalog");
        assert!(again.items[0].stats.is_none());
    }

    /// Topic detail folds its partitions from the same single listing,
    /// and agrees with the listing row for the same topic.
    #[tokio::test]
    async fn detail_agrees_with_the_listing_from_one_listing() {
        let (store, src) = counting_source();
        let topics = seed_connector(&store, &src).await;

        let detail = src.topic_detail(topics[2]).await.unwrap();
        assert_eq!(segment_lists(&store), 1, "was P + 1 = 3");
        let parts: Vec<(i64, i64)> = detail
            .partitions
            .iter()
            .map(|p| (p.messages, p.storage_bytes))
            .collect();
        assert_eq!(parts, [(3, 30), (1, 7)]);

        let row = src
            .list_topics(&Page::new(Some(topics[2].into()), 50, 0), true)
            .await
            .unwrap();
        assert_eq!(
            row.items[0].stats,
            stats(detail.messages, detail.storage_bytes)
        );
    }

    /// A flat page spans prefixes, and each row is folded from its
    /// *routed* prefix — a pinned one included — never from the one its name
    /// derives, where it would silently read as empty.
    #[tokio::test]
    async fn a_flat_page_folds_each_row_from_its_routed_prefix() {
        use object_store::{ObjectStore, PutPayload};
        let (store, src) = counting_source();
        seed_connector(&store, &src).await;

        let pinned = "globex.prod.oracle.audit";
        for (path, body) in [
            (
                src.keys().topic_metadata(pinned),
                r#"{"topic":{"num_partitions":1}}"#,
            ),
            (
                src.keys().topic_routing(pinned),
                r#"{"prefix":"pinned.somewhere.else"}"#,
            ),
        ] {
            store
                .put(&path, PutPayload::from(body.as_bytes().to_vec()))
                .await
                .unwrap();
        }
        let segment = super::super::segment::build_test_segment(
            3,
            1,
            &[(pinned, 0, 0, 4, &[0u8; 12], 1, None)],
        );
        store
            .put(
                &src.keys().segment("pinned.somewhere.else", 0),
                PutPayload::from(segment.to_vec()),
            )
            .await
            .unwrap();

        let page = src
            .list_topics(&Page::new(None, 50, 0), true)
            .await
            .unwrap();
        assert_eq!(page.total, 4);
        assert_eq!(segment_lists(&store), 2, "one listing per prefix");
        let audit = page.items.iter().find(|r| r.name == pinned).unwrap();
        assert_eq!(audit.stats, stats(4, 12));
        let a = page
            .items
            .iter()
            .find(|r| r.name == "acme.prod.db2.a")
            .unwrap();
        assert_eq!(a.stats, stats(2, 17));
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
