//! Consumer groups, read from S3. Groups exist in Tansu's storage even though
//! Kotatsu never connects to a broker.
//!
//! ```text
//! groups/consumers/{group}.json                                    → GroupDetail
//! groups/consumers/{group}/offsets/{topic}/partitions/{p:010}.json → OffsetCommitRequest
//! ```
//!
//! The JSON shapes mirror `tansu-storage`'s types (we don't depend on that
//! crate). Lag is `high_watermark − committed_offset`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use super::{catalog, keys::Keys, StorageError, StorageSource};
use crate::pagination::{Page, Paged};

// --- Mirrored `tansu-storage` JSON shapes (only the fields we use) ---

#[derive(Deserialize)]
struct GroupDetailRaw {
    #[serde(default)]
    generation_id: i32,
    #[serde(default)]
    members: BTreeMap<String, serde_json::Value>,
    state: GroupStateRaw,
}

/// Externally-tagged enum: `{"Forming": {...}}` or `{"Formed": {...}}`.
#[derive(Deserialize)]
enum GroupStateRaw {
    Forming {
        protocol_type: Option<String>,
        protocol_name: Option<String>,
        leader: Option<String>,
    },
    Formed {
        protocol_type: String,
        protocol_name: String,
        #[allow(dead_code)]
        leader: String,
        // member_id -> Kafka assignment blob. Kept as Value so deserialization
        // never fails regardless of how the bytes are encoded; decoded best-effort.
        #[serde(default)]
        assignments: BTreeMap<String, serde_json::Value>,
    },
}

#[derive(Deserialize)]
struct OffsetCommitRaw {
    offset: i64,
}

// --- API view types ---

#[derive(Clone, Debug, Serialize)]
pub struct GroupSummary {
    pub name: String,
    pub state: &'static str,
    pub members: usize,
    /// Absent unless the caller asked for lag — see [`LagMode`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<GroupLag>,
}

/// The lag figures a listing carries when the caller asks for them (#107).
///
/// Nested rather than three flat fields on [`GroupSummary`] because "nobody
/// asked" and "this group has committed nothing" are different answers and the
/// row has to distinguish them: no `lag` object at all is the first, `total:
/// null` is the second — which the UI renders as `—` rather than as a group that
/// is perfectly caught up. It also makes a cached row self-describing: one
/// stored without lag cannot serve a request that wants it.
#[derive(Clone, Debug, Serialize)]
pub struct GroupLag {
    /// Σ lag over every committed `(topic, partition)`, or `None` when the group
    /// has committed no offsets anywhere.
    pub total: Option<i64>,
    /// How many distinct topics the group has a committed offset on.
    pub topics: usize,
    /// The worst single partition. A group can sit at a low total lag with one
    /// partition stuck, and the total alone hides that.
    pub max_partition: Option<i64>,
}

/// What a group listing costs, chosen per request (#107).
///
/// Lag means reading the high watermark behind every committed offset, which is
/// a different order of cost from listing names — so the cheap listing has to
/// stay reachable. `Off` is what `/groups` answered before this existed, and is
/// still exactly what it answers for a caller that does not opt in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LagMode {
    /// No lag. Name order, and only the returned page is read.
    Off,
    /// Lag for the returned page, still in name order.
    Page,
    /// Lag for every match, worst-first. A ranking is only true over the whole
    /// result set — the most-behind group in the cluster is not necessarily on
    /// the page that name order puts first — so this reads every match, not just
    /// the page.
    RankAll,
}

impl LagMode {
    /// Resolves the wire form both front-ends accept: an opt-in `lag` flag and an
    /// optional `sort`.
    ///
    /// Ranking is the point of asking for lag — a groups page ordered by name is
    /// ordered by its least useful key — so lag ranks unless the caller says
    /// otherwise. A `sort` without `lag` has nothing to sort on and is ignored.
    pub fn from_request(lag: bool, sort: Option<&str>) -> Self {
        match (lag, sort) {
            (false, _) => LagMode::Off,
            (true, Some("name")) => LagMode::Page,
            (true, _) => LagMode::RankAll,
        }
    }
}

/// How many groups a lag listing reads at once.
///
/// The same decision as the message reader's partition fan-out: enough to hide
/// S3 round-trip latency, not so much that a cluster with thousands of groups
/// opens thousands of concurrent reads.
const LAG_FANOUT: usize = 8;

/// Ranking key for [`LagMode::RankAll`]. A group that has committed nothing
/// sorts *below* one that is exactly caught up, rather than tying with it: `—`
/// and `0` are different answers and the order should say so.
fn rank_key(summary: &GroupSummary) -> i64 {
    summary.lag.as_ref().and_then(|l| l.total).unwrap_or(-1)
}

#[derive(Debug, Serialize)]
pub struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: i64,
    pub high_watermark: i64,
    pub lag: i64,
}

/// A group that has committed offsets on a given topic.
#[derive(Debug, Serialize)]
pub struct ConsumingGroup {
    pub group: String,
    pub offsets: Vec<GroupOffset>,
}

/// A topic and the partitions a member is assigned.
#[derive(Debug, Serialize)]
pub struct AssignedTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// A group member with its (best-effort decoded) partition assignments.
#[derive(Debug, Serialize)]
pub struct MemberView {
    pub id: String,
    pub assignments: Vec<AssignedTopic>,
}

#[derive(Debug, Serialize)]
pub struct GroupDetailView {
    pub name: String,
    pub state: &'static str,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub generation_id: i32,
    pub members: Vec<MemberView>,
    pub offsets: Vec<GroupOffset>,
    pub total_lag: i64,
}

/// Interprets a JSON value as raw bytes (`bytes::Bytes` serializes as a `[u8]`
/// array). Returns `None` for any other shape.
fn value_to_bytes(v: &serde_json::Value) -> Option<Vec<u8>> {
    let arr = v.as_array()?;
    arr.iter()
        .map(|n| n.as_u64().filter(|x| *x <= 255).map(|x| x as u8))
        .collect()
}

/// Decodes a Kafka `ConsumerProtocolAssignment` blob (classic, non-flexible):
/// `version:i16, [topic:string, [partition:i32]], userdata:bytes`. Best-effort:
/// returns whatever it can parse, stopping on malformed input.
fn decode_assignment(bytes: &[u8]) -> Vec<AssignedTopic> {
    let mut pos = 0usize;
    let i16_at = |b: &[u8], p: usize| -> Option<i16> {
        b.get(p..p + 2).map(|s| i16::from_be_bytes([s[0], s[1]]))
    };
    let i32_at = |b: &[u8], p: usize| -> Option<i32> {
        b.get(p..p + 4)
            .map(|s| i32::from_be_bytes([s[0], s[1], s[2], s[3]]))
    };

    // version (i16) + topic count (i32)
    if i16_at(bytes, pos).is_none() {
        return Vec::new();
    }
    pos += 2;
    let Some(topic_count) = i32_at(bytes, pos) else {
        return Vec::new();
    };
    pos += 4;

    let mut topics = Vec::new();
    for _ in 0..topic_count.max(0) {
        let Some(len) = i16_at(bytes, pos) else {
            break;
        };
        pos += 2;
        let len = len.max(0) as usize;
        let Some(name) = bytes
            .get(pos..pos + len)
            .and_then(|s| std::str::from_utf8(s).ok())
        else {
            break;
        };
        pos += len;
        let Some(pcount) = i32_at(bytes, pos) else {
            break;
        };
        pos += 4;
        let mut partitions = Vec::new();
        let mut ok = true;
        for _ in 0..pcount.max(0) {
            match i32_at(bytes, pos) {
                Some(p) => {
                    partitions.push(p);
                    pos += 4;
                }
                None => {
                    ok = false;
                    break;
                }
            }
        }
        topics.push(AssignedTopic {
            topic: name.to_string(),
            partitions,
        });
        if !ok {
            break;
        }
    }
    topics
}

/// Derives the consumer group state, mirroring Tansu's mapping.
fn derive_state(detail: &GroupDetailRaw) -> &'static str {
    if detail.members.is_empty() {
        "Empty"
    } else {
        match detail.state {
            GroupStateRaw::Forming { leader: None, .. } => "Assigning",
            GroupStateRaw::Formed { .. } => "Stable",
            _ => "Unknown",
        }
    }
}

impl StorageSource {
    /// Lists consumer groups (one `{group}.json` per group), filtered and
    /// paginated. `GroupDetail` is read only for the rows that are returned.
    ///
    /// `mode` decides what a row costs and how the list is ordered — see
    /// [`LagMode`]. Under [`LagMode::RankAll`] the page cannot be sliced before
    /// the reads, because the ranking key is what those reads produce.
    pub async fn list_groups(
        &self,
        page: &Page,
        mode: LagMode,
    ) -> Result<Paged<GroupSummary>, StorageError> {
        let names = self.catalog_group_names().await?;

        if mode == LagMode::RankAll {
            let matched = page.matching(names);
            // Counted from the index, exactly as the cheap path counts it — so
            // the total does not change under the user when they flip the sort.
            let total = matched.len();
            let mut rows = self.group_summaries(matched, true).await?;
            // Worst lag first, name as the tie-break so equal rows keep a stable
            // order across requests.
            rows.sort_by(|a, b| {
                rank_key(b)
                    .cmp(&rank_key(a))
                    .then_with(|| a.name.cmp(&b.name))
            });
            let items = rows
                .into_iter()
                .skip(page.offset)
                .take(page.limit)
                .collect();
            return Ok(Paged::new(items, total, page));
        }

        let (names, total) = page.select(names);
        let items = self.group_summaries(names, mode == LagMode::Page).await?;
        Ok(Paged::new(items, total, page))
    }

    /// Resolves summaries for `names`, from the catalog cache where it can and
    /// from the store where it cannot, `LAG_FANOUT` at a time.
    ///
    /// A name whose objects have gone is dropped, not raised: the name index is
    /// bounded-stale by design (#84), so a group deleted inside the TTL window is
    /// an expected answer and not a reason to fail the whole listing. That was
    /// survivable while only a page was read; ranking reads every match, which
    /// would otherwise turn one deleted group into a failed page for everyone.
    ///
    /// Order is preserved (`buffered`, not `buffer_unordered`): a name-ordered
    /// page must come back in name order.
    async fn group_summaries(
        &self,
        names: Vec<String>,
        lag: bool,
    ) -> Result<Vec<GroupSummary>, StorageError> {
        // High watermarks memoised for the span of this listing only. Groups in a
        // cluster overwhelmingly commit on the same handful of topics, and
        // resolving a high means listing that topition's segment prefix — without
        // this, a 50-group page re-reads the same watermarks 50 times over.
        let highs = Mutex::new(HashMap::new());

        futures::stream::iter(names)
            .map(|name| {
                let highs = &highs;
                async move {
                    if let Some(cached) = catalog::cached_summary(&self.group_catalog, &name) {
                        // A row cached without lag cannot answer a request that
                        // wants it; one cached *with* lag answers either, so long
                        // as the figures nobody asked for are dropped first.
                        if !lag {
                            return Ok(Some(GroupSummary {
                                lag: None,
                                ..cached
                            }));
                        }
                        if cached.lag.is_some() {
                            return Ok(Some(cached));
                        }
                    }
                    match self.compute_group_summary(&name, lag, highs).await {
                        Ok(summary) => {
                            catalog::store_summary(&self.group_catalog, name, summary.clone());
                            Ok(Some(summary))
                        }
                        // Gone since the index was listed.
                        Err(StorageError::NotFound(_)) => Ok(None),
                        Err(err) => Err(err),
                    }
                }
            })
            .buffered(LAG_FANOUT)
            .try_collect::<Vec<_>>()
            .await
            .map(|rows| rows.into_iter().flatten().collect())
    }

    /// The group-name index, served from the short-TTL catalog cache and
    /// re-listed only on a miss (#84).
    async fn catalog_group_names(&self) -> Result<Vec<String>, StorageError> {
        if let Some(names) = catalog::fresh_names(&self.group_catalog) {
            return Ok(names);
        }
        let names = self.group_names().await?;
        catalog::set_names(&self.group_catalog, names.clone());
        Ok(names)
    }

    /// Lists consumer-group names (one `{group}.json` per group).
    async fn group_names(&self) -> Result<Vec<String>, StorageError> {
        let prefix = self.keys().groups_prefix();
        let listed = self.store().list_with_delimiter(Some(&prefix)).await?;
        let mut names: Vec<String> = listed
            .objects
            .iter()
            .filter_map(|meta| {
                meta.location
                    .filename()
                    .and_then(|f| f.strip_suffix(".json"))
                    .map(str::to_string)
            })
            .collect();
        names.sort();
        Ok(names)
    }

    /// Computes one group's list row from its `{group}.json`, plus its lag when
    /// the listing asked for it. Cached per row by [`list_groups`].
    async fn compute_group_summary(
        &self,
        name: &str,
        lag: bool,
        highs: &Mutex<HashMap<(String, i32), i64>>,
    ) -> Result<GroupSummary, StorageError> {
        let detail: GroupDetailRaw = self.get_json(&self.keys().group(name)).await?;
        let lag = match lag {
            true => Some(self.group_lag(name, highs).await?),
            false => None,
        };
        Ok(GroupSummary {
            state: derive_state(&detail),
            members: detail.members.len(),
            name: name.to_string(),
            lag,
        })
    }

    /// The three list figures for one group: Σ lag, distinct topics, and the
    /// worst single partition.
    async fn group_lag(
        &self,
        group: &str,
        highs: &Mutex<HashMap<(String, i32), i64>>,
    ) -> Result<GroupLag, StorageError> {
        let committed = self.group_committed(group).await?;
        if committed.is_empty() {
            // Committed nothing anywhere. Not the same as being caught up, and
            // reported as its own answer rather than as a zero.
            return Ok(GroupLag {
                total: None,
                topics: 0,
                max_partition: None,
            });
        }

        let mut total = 0i64;
        let mut worst = 0i64;
        let mut topics = BTreeSet::new();
        for (topic, partition) in committed {
            let commit: OffsetCommitRaw = self
                .get_json(&self.keys().group_offset(group, &topic, partition))
                .await?;
            let high = self.memoised_high(&topic, partition, highs).await?;
            let lag = (high - commit.offset).max(0);
            total += lag;
            worst = worst.max(lag);
            topics.insert(topic);
        }

        Ok(GroupLag {
            total: Some(total),
            topics: topics.len(),
            max_partition: Some(worst),
        })
    }

    /// The `(topic, partition)` pairs a group has committed an offset for.
    async fn group_committed(&self, group: &str) -> Result<Vec<(String, i32)>, StorageError> {
        let prefix = self.keys().group_offsets_prefix(group);
        let mut pairs = Vec::new();
        let mut stream = self.store().list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            if let Some(tp) = Keys::topic_partition_from_offset(&meta?.location) {
                pairs.push(tp);
            }
        }
        pairs.sort();
        Ok(pairs)
    }

    /// A topition's high watermark, read at most once per listing.
    ///
    /// The lock is never held across the read: two groups racing on the same
    /// topition simply both resolve it, which is idempotent and still cheaper
    /// than serialising every group behind one mutex.
    async fn memoised_high(
        &self,
        topic: &str,
        partition: i32,
        highs: &Mutex<HashMap<(String, i32), i64>>,
    ) -> Result<i64, StorageError> {
        let key = (topic.to_string(), partition);
        let hit = highs.lock().ok().and_then(|m| m.get(&key).copied());
        if let Some(high) = hit {
            return Ok(high);
        }
        let high = self.watermark_or_empty(topic, partition).await?.high;
        if let Ok(mut m) = highs.lock() {
            m.insert(key, high);
        }
        Ok(high)
    }

    /// Reads a group's metadata, committed offsets and lag.
    pub async fn group_detail(&self, group: &str) -> Result<GroupDetailView, StorageError> {
        let detail: GroupDetailRaw =
            self.get_json(&self.keys().group(group))
                .await
                .map_err(|e| match e {
                    StorageError::NotFound(_) => StorageError::GroupNotFound(group.to_string()),
                    other => other,
                })?;

        let (protocol_type, protocol_name) = match &detail.state {
            GroupStateRaw::Forming {
                protocol_type,
                protocol_name,
                ..
            } => (protocol_type.clone(), protocol_name.clone()),
            GroupStateRaw::Formed {
                protocol_type,
                protocol_name,
                ..
            } => (Some(protocol_type.clone()), Some(protocol_name.clone())),
        };

        // member_id -> assignment blob (only in the Formed state).
        let assignments = match &detail.state {
            GroupStateRaw::Formed { assignments, .. } => assignments.clone(),
            GroupStateRaw::Forming { .. } => BTreeMap::new(),
        };
        let members: Vec<MemberView> = detail
            .members
            .keys()
            .map(|id| MemberView {
                id: id.clone(),
                assignments: assignments
                    .get(id)
                    .and_then(value_to_bytes)
                    .map(|b| decode_assignment(&b))
                    .unwrap_or_default(),
            })
            .collect();

        let topic_partitions = self.group_committed(group).await?;

        let mut offsets = Vec::with_capacity(topic_partitions.len());
        for (topic, partition) in topic_partitions {
            let commit: OffsetCommitRaw = self
                .get_json(&self.keys().group_offset(group, &topic, partition))
                .await?;
            let high = self.watermark_or_empty(&topic, partition).await?.high;
            offsets.push(GroupOffset {
                topic,
                partition,
                committed_offset: commit.offset,
                high_watermark: high,
                lag: (high - commit.offset).max(0),
            });
        }

        let total_lag = offsets.iter().map(|o| o.lag).sum();
        Ok(GroupDetailView {
            name: group.to_string(),
            state: derive_state(&detail),
            protocol_type,
            protocol_name,
            generation_id: detail.generation_id,
            members,
            offsets,
            total_lag,
        })
    }

    /// Lists consumer groups that have committed offsets on `topic`, with their
    /// per-partition committed/high/lag. Scans every group's offsets — meant to
    /// be called lazily (opt-in) from the topic detail page.
    pub async fn groups_consuming(&self, topic: &str) -> Result<Vec<ConsumingGroup>, StorageError> {
        let listed = self
            .store()
            .list_with_delimiter(Some(&self.keys().groups_prefix()))
            .await?;
        let mut group_names: Vec<String> = listed
            .objects
            .iter()
            .filter_map(|m| {
                m.location
                    .filename()
                    .and_then(|f| f.strip_suffix(".json"))
                    .map(str::to_string)
            })
            .collect();
        group_names.sort();

        let mut consuming = Vec::new();
        for group in group_names {
            // Partitions this group committed for the target topic.
            let mut partitions = Vec::new();
            let mut stream = self
                .store()
                .list(Some(&self.keys().group_offsets_prefix(&group)));
            while let Some(meta) = stream.next().await {
                let meta = meta?;
                if let Some((t, p)) = Keys::topic_partition_from_offset(&meta.location) {
                    if t == topic {
                        partitions.push(p);
                    }
                }
            }
            if partitions.is_empty() {
                continue;
            }
            partitions.sort_unstable();

            let mut offsets = Vec::with_capacity(partitions.len());
            for partition in partitions {
                let commit: OffsetCommitRaw = self
                    .get_json(&self.keys().group_offset(&group, topic, partition))
                    .await?;
                let high = self.watermark_or_empty(topic, partition).await?.high;
                offsets.push(GroupOffset {
                    topic: topic.to_string(),
                    partition,
                    committed_offset: commit.offset,
                    high_watermark: high,
                    lag: (high - commit.offset).max(0),
                });
            }
            consuming.push(ConsumingGroup { group, offsets });
        }
        Ok(consuming)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GROUP: &[u8] = include_bytes!("../../tests/fixtures/group.json");

    #[test]
    fn parses_real_group_and_derives_state() {
        let detail: GroupDetailRaw = serde_json::from_slice(GROUP).unwrap();
        // The fixture group has no members and is Forming → Empty.
        assert!(detail.members.is_empty());
        assert_eq!(derive_state(&detail), "Empty");
        match detail.state {
            GroupStateRaw::Forming { protocol_name, .. } => {
                assert_eq!(protocol_name.as_deref(), Some("range"));
            }
            _ => panic!("expected Forming"),
        }
    }

    #[test]
    fn decodes_consumer_protocol_assignment() {
        // version=0, 1 topic "orders" with partitions [0, 1].
        let mut blob = vec![0x00, 0x00]; // version i16
        blob.extend_from_slice(&1i32.to_be_bytes()); // topic count
        blob.extend_from_slice(&6i16.to_be_bytes()); // "orders".len()
        blob.extend_from_slice(b"orders");
        blob.extend_from_slice(&2i32.to_be_bytes()); // partition count
        blob.extend_from_slice(&0i32.to_be_bytes());
        blob.extend_from_slice(&1i32.to_be_bytes());

        let topics = decode_assignment(&blob);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "orders");
        assert_eq!(topics[0].partitions, vec![0, 1]);

        // Malformed input never panics.
        assert!(decode_assignment(&[0x00]).is_empty());
    }

    #[test]
    fn value_to_bytes_accepts_only_byte_arrays() {
        assert_eq!(
            value_to_bytes(&serde_json::json!([0, 1, 255])),
            Some(vec![0, 1, 255])
        );
        assert_eq!(value_to_bytes(&serde_json::json!([300])), None);
        assert_eq!(value_to_bytes(&serde_json::json!("nope")), None);
    }

    /// #84: the consumer-group catalog is cached like the topic catalog — after
    /// warming, listing and search are served from memory within the TTL.
    #[tokio::test]
    async fn group_catalog_serves_listing_from_cache() {
        use crate::pagination::Page;
        use object_store::{memory::InMemory, ObjectStore, PutPayload};
        let store = std::sync::Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        store
            .put(&src.keys().group("g1"), PutPayload::from(GROUP.to_vec()))
            .await
            .unwrap();

        let first = src
            .list_groups(&Page::new(None, 50, 0), LagMode::Off)
            .await
            .unwrap();
        assert_eq!(first.total, 1);
        let warmed_state = first.items[0].state;

        // Remove the object; within the TTL the catalog still answers.
        store.delete(&src.keys().group("g1")).await.unwrap();
        let cached = src
            .list_groups(&Page::new(None, 50, 0), LagMode::Off)
            .await
            .unwrap();
        assert_eq!(cached.total, 1, "name index served from cache");
        assert_eq!(
            cached.items[0].state, warmed_state,
            "summary served from cache"
        );

        let miss = src
            .list_groups(&Page::new(Some("zzz".into()), 50, 0), LagMode::Off)
            .await
            .unwrap();
        assert_eq!(miss.total, 0, "search resolves against the cached index");
    }

    // --- Lag in the listing (#107) ---

    use object_store::memory::InMemory;
    use object_store::{ObjectStore, PutPayload};
    use std::sync::Arc;

    use crate::pagination::Page;

    fn source() -> (Arc<InMemory>, StorageSource) {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        (store, src)
    }

    async fn put(store: &Arc<InMemory>, path: &object_store::path::Path, body: Vec<u8>) {
        store.put(path, PutPayload::from(body)).await.unwrap();
    }

    /// Seeds a group that has committed `offset` on `(topic, partition)`, whose
    /// log ends at `high` — so its lag there is `high - offset`.
    async fn seed_commit(
        store: &Arc<InMemory>,
        src: &StorageSource,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        high: i64,
    ) {
        put(store, &src.keys().group(group), GROUP.to_vec()).await;
        put(
            store,
            &src.keys().group_offset(group, topic, partition),
            format!(r#"{{"offset":{offset}}}"#).into_bytes(),
        )
        .await;
        put(
            store,
            &src.keys().watermark(topic, partition),
            format!(r#"{{"high":{high}}}"#).into_bytes(),
        )
        .await;
    }

    #[test]
    fn lag_is_opt_in_and_ranks_unless_told_otherwise() {
        // Not asking is the cheap listing, whatever `sort` says.
        assert_eq!(LagMode::from_request(false, None), LagMode::Off);
        assert_eq!(LagMode::from_request(false, Some("lag")), LagMode::Off);
        // Asking ranks by default — a groups page in name order is in its least
        // useful order — but name order stays reachable.
        assert_eq!(LagMode::from_request(true, None), LagMode::RankAll);
        assert_eq!(LagMode::from_request(true, Some("lag")), LagMode::RankAll);
        assert_eq!(LagMode::from_request(true, Some("name")), LagMode::Page);
    }

    #[test]
    fn a_group_that_committed_nothing_ranks_below_one_that_is_caught_up() {
        let row = |lag| GroupSummary {
            name: "g".into(),
            state: "Empty",
            members: 0,
            lag,
        };
        let nothing = row(Some(GroupLag {
            total: None,
            topics: 0,
            max_partition: None,
        }));
        let caught_up = row(Some(GroupLag {
            total: Some(0),
            topics: 1,
            max_partition: Some(0),
        }));
        assert!(rank_key(&nothing) < rank_key(&caught_up));
    }

    #[tokio::test]
    async fn a_listing_that_did_not_ask_carries_no_lag_at_all() {
        let (store, src) = source();
        seed_commit(&store, &src, "g1", "orders", 0, 40, 100).await;

        let page = src
            .list_groups(&Page::new(None, 50, 0), LagMode::Off)
            .await
            .unwrap();

        assert!(page.items[0].lag.is_none());
        // Absent from the payload, not merely null: the pre-#107 shape, unchanged.
        let json = serde_json::to_value(&page.items[0]).unwrap();
        assert!(json.get("lag").is_none(), "got {json}");
    }

    #[tokio::test]
    async fn lag_sums_every_partition_and_names_the_worst_one() {
        let (store, src) = source();
        seed_commit(&store, &src, "g1", "orders", 0, 40, 100).await; // 60
        seed_commit(&store, &src, "g1", "orders", 1, 95, 100).await; // 5
        seed_commit(&store, &src, "g1", "events", 0, 0, 10).await; //  10

        let page = src
            .list_groups(&Page::new(None, 50, 0), LagMode::RankAll)
            .await
            .unwrap();
        let lag = page.items[0].lag.as_ref().unwrap();

        assert_eq!(lag.total, Some(75));
        assert_eq!(lag.topics, 2, "distinct topics, not partitions");
        // The number the total hides: one stuck partition inside an otherwise
        // healthy group.
        assert_eq!(lag.max_partition, Some(60));
    }

    #[tokio::test]
    async fn a_group_with_no_committed_offsets_reports_nothing_not_zero() {
        let (store, src) = source();
        put(&store, &src.keys().group("g1"), GROUP.to_vec()).await;

        let page = src
            .list_groups(&Page::new(None, 50, 0), LagMode::RankAll)
            .await
            .unwrap();
        let lag = page.items[0].lag.as_ref().unwrap();

        assert_eq!(lag.total, None, "`—`, not a group that is caught up");
        assert_eq!(lag.topics, 0);
        assert_eq!(lag.max_partition, None);
    }

    /// The acceptance criterion the paging order exists for: the most-behind
    /// group in the cluster must reach the first page even when name order would
    /// bury it on the last one.
    #[tokio::test]
    async fn ranking_covers_the_whole_result_set_not_just_the_page() {
        let (store, src) = source();
        seed_commit(&store, &src, "aaa", "orders", 0, 99, 100).await; // 1
        seed_commit(&store, &src, "bbb", "orders", 1, 90, 100).await; // 10
        seed_commit(&store, &src, "zzz", "orders", 2, 0, 100).await; // 100

        // One row per page: name order would answer `aaa`.
        let first = src
            .list_groups(&Page::new(None, 1, 0), LagMode::RankAll)
            .await
            .unwrap();

        assert_eq!(first.total, 3, "total counts every match, not the page");
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.items[0].name, "zzz");

        let second = src
            .list_groups(&Page::new(None, 1, 1), LagMode::RankAll)
            .await
            .unwrap();
        assert_eq!(second.items[0].name, "bbb");
    }

    /// A group deleted inside the catalog's TTL window is still in the name
    /// index. Reading every match to rank them makes that a near-certainty on a
    /// busy cluster, so it must cost that group's row and not the whole page.
    #[tokio::test]
    async fn a_group_deleted_since_the_index_was_listed_is_dropped_not_fatal() {
        let (store, src) = source();
        seed_commit(&store, &src, "gone", "orders", 0, 40, 100).await;
        seed_commit(&store, &src, "stays", "orders", 0, 10, 100).await;

        // Warm the name index with a listing that stores no lag, so the ranked
        // listing below has to go back to the store for both rows.
        src.list_groups(&Page::new(None, 50, 0), LagMode::Off)
            .await
            .unwrap();
        store.delete(&src.keys().group("gone")).await.unwrap();

        let page = src
            .list_groups(&Page::new(None, 50, 0), LagMode::RankAll)
            .await
            .expect("one vanished group must not fail the listing");

        let names: Vec<&str> = page.items.iter().map(|g| g.name.as_str()).collect();
        assert_eq!(names, ["stays"]);
        // The count still comes from the index, which is stale by design (#84).
        assert_eq!(page.total, 2);
    }

    /// The trap the catalog cache sets: its rows are keyed by name alone, so a
    /// row warmed by a listing that did not want lag must not be handed to one
    /// that does — it would answer `—` for a group that is behind.
    #[tokio::test]
    async fn a_row_cached_without_lag_is_not_reused_for_a_lag_listing() {
        let (store, src) = source();
        seed_commit(&store, &src, "g1", "orders", 0, 40, 100).await;

        let warm = src
            .list_groups(&Page::new(None, 50, 0), LagMode::Off)
            .await
            .unwrap();
        assert!(warm.items[0].lag.is_none());

        let asked = src
            .list_groups(&Page::new(None, 50, 0), LagMode::RankAll)
            .await
            .unwrap();
        assert_eq!(
            asked.items[0].lag.as_ref().unwrap().total,
            Some(60),
            "the cached lag-less row was recomputed, not reused"
        );

        // And the richer row does serve a cheap listing afterwards — stripped,
        // so opting out still means opting out.
        let cheap = src
            .list_groups(&Page::new(None, 50, 0), LagMode::Off)
            .await
            .unwrap();
        assert!(cheap.items[0].lag.is_none());
    }
}
