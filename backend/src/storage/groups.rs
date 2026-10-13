//! Consumer groups, read from S3. Groups exist in Tansu's storage even though
//! Kotatsu never connects to a broker.
//!
//! A group's membership is in one of two layouts, picked by what the cluster
//! declares in `schema/groups.json`:
//!
//! ```text
//! legacy (no schema object):
//! groups/consumers/{group}.json                                    → GroupDetail
//!
//! decomposed ({"version": 2}, Popsink/kodansu#359):
//! groups/consumers/{group}/generation.json                         → GenerationDoc
//! groups/consumers/{group}/assignment/{generation_id:010}.json     → AssignmentDoc
//! ```
//!
//! Committed offsets are read the same way under either:
//!
//! ```text
//! groups/consumers/{group}/offsets.json                            → GroupOffsets
//! groups/consumers/{group}/offsets/{topic}/partitions/{p:010}.json → OffsetCommitRequest
//! ```
//!
//! `offsets.json` replaced the per-partition objects in Popsink/kodansu#415,
//! which neither migrated nor deleted them. So a partition is read from
//! `offsets.json` first and from its own object only when `offsets.json` lacks
//! it, and the partitions a group committed are the union of both — the
//! broker's own `offset_fetch` order.
//!
//! The JSON shapes mirror `tansu-storage`'s types (we don't depend on that
//! crate), keep only the fields we use and ignore the rest. Lag is
//! `high_watermark − committed_offset`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use super::{catalog, keys::Keys, StorageError, StorageSource, FANOUT};
use crate::pagination::{Page, Paged};

// --- Mirrored `tansu-storage` JSON shapes (only the fields we use) ---

/// The legacy `{group}.json`.
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

/// `schema/groups.json`.
#[derive(Deserialize)]
struct GroupSchemaRaw {
    version: u32,
}

/// `generation.json`. It stores no state: the broker derives it, and so do we
/// ([`derive_decomposed_state`]).
#[derive(Deserialize)]
struct GenerationRaw {
    #[serde(default)]
    generation_id: i32,
    #[serde(default)]
    protocol_type: Option<String>,
    #[serde(default)]
    protocol_name: Option<String>,
    #[serde(default)]
    leader: Option<String>,
    /// `member_id → {group_instance_id?}`.
    #[serde(default)]
    members: BTreeMap<String, serde_json::Value>,
}

/// `assignment/{generation_id:010}.json`: the leader's assignment blobs, in the
/// same `[u8]`-array encoding `Formed` gives them.
#[derive(Deserialize)]
struct AssignmentRaw {
    #[serde(default)]
    assignments: BTreeMap<String, serde_json::Value>,
}

/// `offsets.json`: `topic → partition → commit`. The partition is a JSON object
/// key, so a string on the wire; serde parses it back to the number.
#[derive(Deserialize)]
struct GroupOffsetsRaw {
    #[serde(default)]
    committed: BTreeMap<String, BTreeMap<i32, OffsetCommitRaw>>,
}

#[derive(Deserialize)]
struct OffsetCommitRaw {
    offset: i64,
}

/// The consumer-group layout a cluster is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GroupLayout {
    /// One `{group}.json` per group: every broker before Popsink/kodansu#359.
    Legacy,
    /// `generation.json` and `assignment/` under `{group}/`.
    Decomposed,
}

/// How long "this cluster has no `schema/groups.json`" is trusted before it is
/// asked again. The broker writes the object when it starts on the new layout,
/// so a cached legacy answer has to give way soon after an upgrade.
const LEGACY_LAYOUT_TTL: Duration = Duration::from_secs(60);

/// A group's assignment blobs, stamped with the generation they belong to.
pub(super) type CachedAssignment = (i32, Arc<BTreeMap<String, serde_json::Value>>);

/// A group's membership, whichever layout it was read from.
struct Membership {
    state: &'static str,
    generation_id: i32,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
    members: Vec<MemberView>,
}

impl Membership {
    /// A group that exists only through what it left behind — committed
    /// offsets, a `{group}.json` from before the upgrade — and so has no
    /// members.
    fn empty() -> Self {
        Self {
            state: "Empty",
            generation_id: 0,
            protocol_type: None,
            protocol_name: None,
            members: Vec::new(),
        }
    }
}

impl From<GroupDetailRaw> for Membership {
    fn from(detail: GroupDetailRaw) -> Self {
        let state = derive_state(&detail);
        let (protocol_type, protocol_name, assignments) = match detail.state {
            GroupStateRaw::Forming {
                protocol_type,
                protocol_name,
                ..
            } => (protocol_type, protocol_name, BTreeMap::new()),
            GroupStateRaw::Formed {
                protocol_type,
                protocol_name,
                assignments,
                ..
            } => (Some(protocol_type), Some(protocol_name), assignments),
        };
        Self {
            state,
            generation_id: detail.generation_id,
            protocol_type,
            protocol_name,
            members: member_views(detail.members.keys(), &assignments),
        }
    }
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

/// Pairs each member with its (best-effort decoded) assignment blob, if any.
fn member_views<'a>(
    ids: impl Iterator<Item = &'a String>,
    assignments: &BTreeMap<String, serde_json::Value>,
) -> Vec<MemberView> {
    ids.map(|id| MemberView {
        id: id.clone(),
        assignments: assignments
            .get(id)
            .and_then(value_to_bytes)
            .map(|b| decode_assignment(&b))
            .unwrap_or_default(),
    })
    .collect()
}

/// Derives a legacy group's state, mirroring Tansu's mapping.
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

/// Derives a decomposed group's state exactly as the broker's
/// `GenerationDoc::state` does. `assignment_present` is whether
/// `assignment/{generation_id}` exists.
fn derive_decomposed_state(generation: &GenerationRaw, assignment_present: bool) -> &'static str {
    if generation.members.is_empty() {
        "Empty"
    } else if generation.leader.is_none() {
        "Assigning"
    } else if assignment_present {
        "Stable"
    } else {
        "CompletingRebalance"
    }
}

impl StorageSource {
    /// Lists consumer groups, filtered and paginated. Membership is read only
    /// for the rows that are returned.
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
    /// from the store where it cannot, `FANOUT` at a time.
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
                        Ok(Some(summary)) => {
                            catalog::store_summary(&self.group_catalog, name, summary.clone());
                            Ok(Some(summary))
                        }
                        // Gone since the index was listed.
                        Ok(None) | Err(StorageError::NotFound(_)) => Ok(None),
                        Err(err) => Err(err),
                    }
                }
            })
            .buffered(FANOUT)
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

    /// The consumer-group layout the cluster declares in `schema/groups.json`:
    /// decomposed from `version` 2 on, legacy when the object is absent (an
    /// older or self-hosted broker).
    async fn group_layout(&self) -> Result<GroupLayout, StorageError> {
        let cached = self.group_layout.lock().ok().and_then(|c| *c);
        if let Some((layout, read_at)) = cached {
            if layout == GroupLayout::Decomposed || read_at.elapsed() < LEGACY_LAYOUT_TTL {
                return Ok(layout);
            }
        }
        let layout = match self
            .get_json::<GroupSchemaRaw>(&self.keys().group_schema())
            .await
        {
            Ok(schema) if schema.version >= 2 => GroupLayout::Decomposed,
            Ok(_) | Err(StorageError::NotFound(_)) => GroupLayout::Legacy,
            Err(err) => return Err(err),
        };
        if let Ok(mut c) = self.group_layout.lock() {
            *c = Some((layout, Instant::now()));
        }
        Ok(layout)
    }

    /// Lists consumer-group names.
    ///
    /// The legacy layout is listed by its `{group}.json` objects, as it always
    /// was. The decomposed one also takes every `{group}/` prefix: a group
    /// formed since the upgrade has no `{group}.json`, and a group that only
    /// ever committed offsets (manual `assign()`, no membership) has no
    /// `generation.json` either. The union is what the broker's own
    /// `ListGroups` answers.
    async fn group_names(&self) -> Result<Vec<String>, StorageError> {
        let layout = self.group_layout().await?;
        let prefix = self.keys().groups_prefix();
        let listed = self.store().list_with_delimiter(Some(&prefix)).await?;
        let mut names: BTreeSet<String> = listed
            .objects
            .iter()
            .filter_map(|meta| {
                meta.location
                    .filename()
                    .and_then(|f| f.strip_suffix(".json"))
                    .map(str::to_string)
            })
            .collect();
        if layout == GroupLayout::Decomposed {
            names.extend(
                listed
                    .common_prefixes
                    .iter()
                    .filter_map(|p| p.filename().map(str::to_string)),
            );
        }
        Ok(names.into_iter().collect())
    }

    /// Reads a group's membership from the layout the cluster is in, or `None`
    /// when nothing of the group exists.
    async fn membership(&self, group: &str) -> Result<Option<Membership>, StorageError> {
        match self.group_layout().await? {
            GroupLayout::Legacy => match self
                .get_json::<GroupDetailRaw>(&self.keys().group(group))
                .await
            {
                Ok(detail) => Ok(Some(Membership::from(detail))),
                Err(StorageError::NotFound(_)) => Ok(None),
                Err(err) => Err(err),
            },
            GroupLayout::Decomposed => self.decomposed_membership(group).await,
        }
    }

    /// Membership from `generation.json` and the current generation's
    /// assignment. A group without a `generation.json` that still has
    /// something under its name is `Empty`, not missing.
    async fn decomposed_membership(&self, group: &str) -> Result<Option<Membership>, StorageError> {
        let generation: GenerationRaw =
            match self.get_json(&self.keys().group_generation(group)).await {
                Ok(generation) => generation,
                Err(StorageError::NotFound(_)) => {
                    return Ok(self
                        .group_remnants_exist(group)
                        .await?
                        .then(Membership::empty));
                }
                Err(err) => return Err(err),
            };

        // The assignment only decides the state of a group that has members and
        // a leader; the state is already known for any other.
        let assignment = match !generation.members.is_empty() && generation.leader.is_some() {
            true => self.assignment(group, generation.generation_id).await?,
            false => None,
        };
        let state = derive_decomposed_state(&generation, assignment.is_some());
        let members = member_views(
            generation.members.keys(),
            assignment.as_deref().unwrap_or(&BTreeMap::new()),
        );
        Ok(Some(Membership {
            state,
            generation_id: generation.generation_id,
            protocol_type: generation.protocol_type,
            protocol_name: generation.protocol_name,
            members,
        }))
    }

    /// Whether a group with no `generation.json` exists anyway: something under
    /// its prefix (committed offsets, typically), or a `{group}.json` left from
    /// before the upgrade. That object is read for its existence only — the
    /// members it names were gone when the broker was cut over.
    async fn group_remnants_exist(&self, group: &str) -> Result<bool, StorageError> {
        let prefix = self.keys().group_prefix(group);
        if let Some(first) = self.store().list(Some(&prefix)).next().await {
            first?;
            return Ok(true);
        }
        match self.store().head(&self.keys().group(group)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// A generation's assignment blobs, or `None` while the leader has not
    /// written them yet. The object is create-only and immutable, so a read is
    /// cached for as long as the group stays on that generation; an absence is
    /// not, because it is about to change.
    async fn assignment(
        &self,
        group: &str,
        generation_id: i32,
    ) -> Result<Option<Arc<BTreeMap<String, serde_json::Value>>>, StorageError> {
        let hit = self.group_assignments.lock().ok().and_then(|c| {
            c.get(group)
                .filter(|(generation, _)| *generation == generation_id)
                .map(|(_, assignments)| assignments.clone())
        });
        if hit.is_some() {
            return Ok(hit);
        }
        let path = self.keys().group_assignment(group, generation_id);
        match self.get_json::<AssignmentRaw>(&path).await {
            Ok(doc) => {
                let assignments = Arc::new(doc.assignments);
                if let Ok(mut c) = self.group_assignments.lock() {
                    c.insert(group.to_string(), (generation_id, assignments.clone()));
                }
                Ok(Some(assignments))
            }
            Err(StorageError::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Computes one group's list row from its membership, plus its lag when
    /// the listing asked for it. `None` when the group is gone. Cached per row
    /// by [`list_groups`].
    async fn compute_group_summary(
        &self,
        name: &str,
        lag: bool,
        highs: &Mutex<HashMap<(String, i32), i64>>,
    ) -> Result<Option<GroupSummary>, StorageError> {
        let Some(membership) = self.membership(name).await? else {
            return Ok(None);
        };
        let lag = match lag {
            true => Some(self.group_lag(name, highs).await?),
            false => None,
        };
        Ok(Some(GroupSummary {
            state: membership.state,
            members: membership.members.len(),
            name: name.to_string(),
            lag,
        }))
    }

    /// The three list figures for one group: Σ lag, distinct topics, and the
    /// worst single partition.
    async fn group_lag(
        &self,
        group: &str,
        highs: &Mutex<HashMap<(String, i32), i64>>,
    ) -> Result<GroupLag, StorageError> {
        let committed = self.committed_offsets(group, None).await?;
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
        for ((topic, partition), offset) in committed {
            let high = self.memoised_high(&topic, partition, highs).await?;
            let lag = (high - offset).max(0);
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

    /// Every offset a group has committed, on `topic` only when given, keyed by
    /// `(topic, partition)`.
    ///
    /// `offsets.json` first; a partition's own object only for the partitions
    /// `offsets.json` lacks, so a stale pre-upgrade commit never shadows a newer
    /// one, and a group that has not committed since the upgrade still reports
    /// what it had. The set is the union of both, as the broker's.
    async fn committed_offsets(
        &self,
        group: &str,
        topic: Option<&str>,
    ) -> Result<BTreeMap<(String, i32), i64>, StorageError> {
        let mut committed = BTreeMap::new();
        match self
            .get_json::<GroupOffsetsRaw>(&self.keys().group_offsets_doc(group))
            .await
        {
            Ok(doc) => {
                for (t, partitions) in doc.committed {
                    if topic.is_some_and(|wanted| wanted != t) {
                        continue;
                    }
                    for (partition, commit) in partitions {
                        committed.insert((t.clone(), partition), commit.offset);
                    }
                }
            }
            Err(StorageError::NotFound(_)) => {}
            Err(err) => return Err(err),
        }

        let prefix = match topic {
            Some(topic) => self.keys().group_topic_offsets_prefix(group, topic),
            None => self.keys().group_offsets_prefix(group),
        };
        let mut missing = Vec::new();
        let mut stream = self.store().list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            if let Some(tp) = Keys::topic_partition_from_offset(&meta?.location) {
                if !committed.contains_key(&tp) {
                    missing.push(tp);
                }
            }
        }

        let fallback: Vec<_> = futures::stream::iter(missing)
            .map(|(topic, partition)| async move {
                let path = self.keys().group_offset(group, &topic, partition);
                match self.get_json::<OffsetCommitRaw>(&path).await {
                    Ok(commit) => Ok(Some(((topic, partition), commit.offset))),
                    // Deleted since it was listed.
                    Err(StorageError::NotFound(_)) => Ok(None),
                    Err(err) => Err(err),
                }
            })
            .buffer_unordered(FANOUT)
            .try_collect()
            .await?;
        committed.extend(fallback.into_iter().flatten());
        Ok(committed)
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
        let membership = self
            .membership(group)
            .await?
            .ok_or_else(|| StorageError::GroupNotFound(group.to_string()))?;

        let committed = self.committed_offsets(group, None).await?;
        let mut offsets = Vec::with_capacity(committed.len());
        for ((topic, partition), committed_offset) in committed {
            let high = self.watermark_or_empty(&topic, partition).await?.high;
            offsets.push(GroupOffset {
                topic,
                partition,
                committed_offset,
                high_watermark: high,
                lag: (high - committed_offset).max(0),
            });
        }

        let total_lag = offsets.iter().map(|o| o.lag).sum();
        Ok(GroupDetailView {
            name: group.to_string(),
            state: membership.state,
            protocol_type: membership.protocol_type,
            protocol_name: membership.protocol_name,
            generation_id: membership.generation_id,
            members: membership.members,
            offsets,
            total_lag,
        })
    }

    /// Lists consumer groups that have committed offsets on `topic`, with their
    /// per-partition committed/high/lag. Scans every group's offsets — meant to
    /// be called lazily (opt-in) from the topic detail page.
    pub async fn groups_consuming(&self, topic: &str) -> Result<Vec<ConsumingGroup>, StorageError> {
        let group_names = self.group_names().await?;
        // Every group here reads the same topic's watermarks.
        let highs = Mutex::new(HashMap::new());

        futures::stream::iter(group_names)
            .map(|group| {
                let highs = &highs;
                async move {
                    let committed = self.committed_offsets(&group, Some(topic)).await?;
                    if committed.is_empty() {
                        return Ok(None);
                    }
                    let mut offsets = Vec::with_capacity(committed.len());
                    for ((_, partition), committed_offset) in committed {
                        let high = self.memoised_high(topic, partition, highs).await?;
                        offsets.push(GroupOffset {
                            topic: topic.to_string(),
                            partition,
                            committed_offset,
                            high_watermark: high,
                            lag: (high - committed_offset).max(0),
                        });
                    }
                    Ok(Some(ConsumingGroup { group, offsets }))
                }
            })
            .buffered(FANOUT)
            .try_collect::<Vec<_>>()
            .await
            .map(|rows| rows.into_iter().flatten().collect())
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

    /// The third mode, and the issue's "visible page only" reading: the figures
    /// without the full-set read that ranking needs.
    #[tokio::test]
    async fn page_mode_keeps_name_order_and_reads_only_the_page() {
        let (store, src) = source();
        seed_commit(&store, &src, "aaa", "orders", 0, 99, 100).await; // 1
        seed_commit(&store, &src, "zzz", "orders", 1, 0, 100).await; // 100

        let page = src
            .list_groups(&Page::new(None, 1, 0), LagMode::Page)
            .await
            .unwrap();

        // Name order, so the worst group does *not* come first here.
        assert_eq!(page.items[0].name, "aaa");
        assert_eq!(page.items[0].lag.as_ref().unwrap().total, Some(1));
        assert_eq!(page.total, 2);
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
    // --- The decomposed layout and `offsets.json` (#133) ---

    const GENERATION: &[u8] = include_bytes!("../../tests/fixtures/group_generation.json");
    const ASSIGNMENT: &[u8] = include_bytes!("../../tests/fixtures/group_assignment.json");
    const OFFSETS: &[u8] = include_bytes!("../../tests/fixtures/group_offsets.json");

    /// A source over a cluster that declares the decomposed layout.
    async fn decomposed_source() -> (Arc<InMemory>, StorageSource) {
        let (store, src) = source();
        put(
            &store,
            &src.keys().group_schema(),
            br#"{"version":2}"#.to_vec(),
        )
        .await;
        (store, src)
    }

    async fn watermark(
        store: &Arc<InMemory>,
        src: &StorageSource,
        topic: &str,
        partition: i32,
        high: i64,
    ) {
        put(
            store,
            &src.keys().watermark(topic, partition),
            format!(r#"{{"high":{high}}}"#).into_bytes(),
        )
        .await;
    }

    #[test]
    fn parses_the_decomposed_documents_and_ignores_fields_it_does_not_model() {
        // The fixtures carry fields Kotatsu does not read (`seq`, `swept_at_ms`,
        // an `assignor` no version models yet); none of them may fail the parse.
        let generation: GenerationRaw = serde_json::from_slice(GENERATION).unwrap();
        assert_eq!(generation.generation_id, 7);
        assert_eq!(generation.leader.as_deref(), Some("m-1"));
        assert_eq!(generation.members.len(), 2);

        let assignment: AssignmentRaw = serde_json::from_slice(ASSIGNMENT).unwrap();
        let blob = value_to_bytes(&assignment.assignments["m-2"]).unwrap();
        let topics = decode_assignment(&blob);
        assert_eq!(topics[0].topic, "orders");
        assert_eq!(topics[0].partitions, vec![2]);

        let offsets: GroupOffsetsRaw = serde_json::from_slice(OFFSETS).unwrap();
        assert_eq!(offsets.committed["orders"][&0].offset, 40);
        assert_eq!(offsets.committed["orders"][&1].offset, 95);
    }

    #[test]
    fn the_decomposed_state_is_derived_as_the_broker_derives_it() {
        let mut generation: GenerationRaw = serde_json::from_slice(GENERATION).unwrap();
        assert_eq!(derive_decomposed_state(&generation, true), "Stable");
        assert_eq!(
            derive_decomposed_state(&generation, false),
            "CompletingRebalance"
        );

        generation.leader = None;
        assert_eq!(derive_decomposed_state(&generation, false), "Assigning");

        // An assignment left by a generation the group has since emptied does
        // not make an empty group stable.
        generation.members.clear();
        assert_eq!(derive_decomposed_state(&generation, true), "Empty");
    }

    /// The acceptance criterion: a group that formed on a current broker has
    /// no `{group}.json` and must still be listed, described, and found among
    /// a topic's consumers.
    #[tokio::test]
    async fn a_group_formed_on_the_decomposed_layout_lists_describes_and_consumes() {
        let (store, src) = decomposed_source().await;
        put(
            &store,
            &src.keys().group_generation("g1"),
            GENERATION.to_vec(),
        )
        .await;
        put(
            &store,
            &src.keys().group_assignment("g1", 7),
            ASSIGNMENT.to_vec(),
        )
        .await;
        put(
            &store,
            &src.keys().group_offsets_doc("g1"),
            OFFSETS.to_vec(),
        )
        .await;
        watermark(&store, &src, "orders", 0, 100).await;
        watermark(&store, &src, "orders", 1, 100).await;

        let page = src
            .list_groups(&Page::new(None, 50, 0), LagMode::RankAll)
            .await
            .unwrap();
        assert_eq!(page.total, 1);
        let row = &page.items[0];
        assert_eq!(row.name, "g1");
        assert_eq!(row.state, "Stable");
        assert_eq!(row.members, 2);
        assert_eq!(row.lag.as_ref().unwrap().total, Some(65));

        let detail = src.group_detail("g1").await.unwrap();
        assert_eq!(detail.state, "Stable");
        assert_eq!(detail.generation_id, 7);
        assert_eq!(detail.protocol_type.as_deref(), Some("consumer"));
        assert_eq!(detail.protocol_name.as_deref(), Some("range"));
        let members: Vec<(&str, Vec<i32>)> = detail
            .members
            .iter()
            .map(|m| (m.id.as_str(), m.assignments[0].partitions.clone()))
            .collect();
        assert_eq!(members, [("m-1", vec![0, 1]), ("m-2", vec![2])]);
        let offsets: Vec<(i32, i64, i64)> = detail
            .offsets
            .iter()
            .map(|o| (o.partition, o.committed_offset, o.lag))
            .collect();
        assert_eq!(offsets, [(0, 40, 60), (1, 95, 5)]);
        assert_eq!(detail.total_lag, 65);

        let consuming = src.groups_consuming("orders").await.unwrap();
        assert_eq!(consuming.len(), 1);
        assert_eq!(consuming[0].group, "g1");
        assert_eq!(consuming[0].offsets.len(), 2);
        assert!(src.groups_consuming("events").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_led_group_whose_assignment_is_not_written_yet_is_completing_its_rebalance() {
        let (store, src) = decomposed_source().await;
        put(
            &store,
            &src.keys().group_generation("g1"),
            GENERATION.to_vec(),
        )
        .await;

        let detail = src.group_detail("g1").await.unwrap();
        assert_eq!(detail.state, "CompletingRebalance");
        assert_eq!(detail.members.len(), 2);
        assert!(detail.members.iter().all(|m| m.assignments.is_empty()));
    }

    /// The bug the lag figures had: a group that existed before the upgrade
    /// reported its last pre-upgrade commit, so its lag grew without bound.
    #[tokio::test]
    async fn offsets_json_wins_over_a_stale_per_partition_commit_and_the_set_is_the_union() {
        let (store, src) = decomposed_source().await;
        put(
            &store,
            &src.keys().group_generation("g1"),
            GENERATION.to_vec(),
        )
        .await;
        // `offsets.json` holds partitions 0 and 1; the per-partition layout
        // left a stale 0 behind, and a 2 committed before the upgrade only.
        put(
            &store,
            &src.keys().group_offsets_doc("g1"),
            OFFSETS.to_vec(),
        )
        .await;
        put(
            &store,
            &src.keys().group_offset("g1", "orders", 0),
            br#"{"offset":10}"#.to_vec(),
        )
        .await;
        put(
            &store,
            &src.keys().group_offset("g1", "orders", 2),
            br#"{"offset":7}"#.to_vec(),
        )
        .await;
        for p in 0..3 {
            watermark(&store, &src, "orders", p, 100).await;
        }

        let detail = src.group_detail("g1").await.unwrap();
        let offsets: Vec<(i32, i64)> = detail
            .offsets
            .iter()
            .map(|o| (o.partition, o.committed_offset))
            .collect();
        assert_eq!(offsets, [(0, 40), (1, 95), (2, 7)]);

        let consuming = src.groups_consuming("orders").await.unwrap();
        let offsets: Vec<(i32, i64)> = consuming[0]
            .offsets
            .iter()
            .map(|o| (o.partition, o.committed_offset))
            .collect();
        assert_eq!(offsets, [(0, 40), (1, 95), (2, 7)]);
    }

    /// A consumer that `assign()`s and commits never joins, so it has no
    /// `generation.json` — and is still a group.
    #[tokio::test]
    async fn a_group_that_only_commits_offsets_is_listed_as_empty_not_missing() {
        let (store, src) = decomposed_source().await;
        put(
            &store,
            &src.keys().group_offsets_doc("manual"),
            OFFSETS.to_vec(),
        )
        .await;
        watermark(&store, &src, "orders", 0, 100).await;
        watermark(&store, &src, "orders", 1, 100).await;

        let page = src
            .list_groups(&Page::new(None, 50, 0), LagMode::Off)
            .await
            .unwrap();
        let names: Vec<&str> = page.items.iter().map(|g| g.name.as_str()).collect();
        assert_eq!(names, ["manual"]);
        assert_eq!(page.items[0].state, "Empty");

        let detail = src.group_detail("manual").await.unwrap();
        assert_eq!(detail.state, "Empty");
        assert!(detail.members.is_empty());
        assert_eq!(detail.total_lag, 65);
    }

    /// A `{group}.json` left from before the upgrade names members the
    /// cut-over quiesced: it proves the group exists, and nothing else.
    #[tokio::test]
    async fn a_leftover_legacy_object_counts_for_existence_not_for_its_members() {
        let (store, src) = decomposed_source().await;
        let stale = br#"{"generation_id":3,"members":{"gone":{}},"state":{"Formed":{"protocol_type":"consumer","protocol_name":"range","leader":"gone","assignments":{}}}}"#;
        put(&store, &src.keys().group("old"), stale.to_vec()).await;
        // And one that has re-formed since: its generation is what counts.
        put(&store, &src.keys().group("reformed"), stale.to_vec()).await;
        put(
            &store,
            &src.keys().group_generation("reformed"),
            GENERATION.to_vec(),
        )
        .await;

        let page = src
            .list_groups(&Page::new(None, 50, 0), LagMode::Off)
            .await
            .unwrap();
        let names: Vec<&str> = page.items.iter().map(|g| g.name.as_str()).collect();
        assert_eq!(names, ["old", "reformed"], "each listed once");

        let old = src.group_detail("old").await.unwrap();
        assert_eq!(old.state, "Empty");
        assert!(old.members.is_empty());
        assert_eq!(old.generation_id, 0);

        let reformed = src.group_detail("reformed").await.unwrap();
        assert_eq!(reformed.generation_id, 7);
        assert_eq!(reformed.members.len(), 2);
    }

    #[tokio::test]
    async fn an_unknown_group_on_the_decomposed_layout_is_not_found() {
        let (_store, src) = decomposed_source().await;
        let err = src.group_detail("nope").await.unwrap_err();
        assert!(matches!(err, StorageError::GroupNotFound(_)), "got {err}");
    }

    /// An assignment object never changes, so it is read once per generation;
    /// a new generation is a new object and is read again.
    #[tokio::test]
    async fn an_assignment_is_read_once_per_generation() {
        let (store, src) = decomposed_source().await;
        put(
            &store,
            &src.keys().group_generation("g1"),
            GENERATION.to_vec(),
        )
        .await;
        put(
            &store,
            &src.keys().group_assignment("g1", 7),
            ASSIGNMENT.to_vec(),
        )
        .await;
        assert_eq!(src.group_detail("g1").await.unwrap().state, "Stable");

        store
            .delete(&src.keys().group_assignment("g1", 7))
            .await
            .unwrap();
        assert_eq!(
            src.group_detail("g1").await.unwrap().state,
            "Stable",
            "served from the cache"
        );

        let next = String::from_utf8(GENERATION.to_vec())
            .unwrap()
            .replace(r#""generation_id":7"#, r#""generation_id":8"#);
        put(
            &store,
            &src.keys().group_generation("g1"),
            next.into_bytes(),
        )
        .await;
        assert_eq!(
            src.group_detail("g1").await.unwrap().state,
            "CompletingRebalance",
            "generation 8 has no assignment yet"
        );
    }

    /// Without `schema/groups.json` the cluster is read exactly as before: a
    /// `{group}/` prefix is not a group of its own.
    #[tokio::test]
    async fn a_cluster_without_a_schema_object_keeps_the_legacy_layout() {
        let (store, src) = source();
        seed_commit(&store, &src, "g1", "orders", 0, 40, 100).await;
        put(
            &store,
            &src.keys().group_offsets_doc("orphan"),
            OFFSETS.to_vec(),
        )
        .await;

        let page = src
            .list_groups(&Page::new(None, 50, 0), LagMode::Off)
            .await
            .unwrap();
        let names: Vec<&str> = page.items.iter().map(|g| g.name.as_str()).collect();
        assert_eq!(names, ["g1"]);
        assert_eq!(src.group_detail("g1").await.unwrap().state, "Empty");
    }
}
