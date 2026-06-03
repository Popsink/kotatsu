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

use std::collections::BTreeMap;

use futures::StreamExt;
use serde::{Deserialize, Serialize};

use super::{keys::Keys, StorageError, StorageSource};
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

#[derive(Debug, Serialize)]
pub struct GroupSummary {
    pub name: String,
    pub state: &'static str,
    pub members: usize,
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
    /// paginated. `GroupDetail` is read only for the returned page.
    pub async fn list_groups(&self, page: &Page) -> Result<Paged<GroupSummary>, StorageError> {
        let prefix = self.keys().groups_prefix();
        let listed = self.store().list_with_delimiter(Some(&prefix)).await?;

        let names: Vec<String> = listed
            .objects
            .iter()
            .filter_map(|meta| {
                meta.location
                    .filename()
                    .and_then(|f| f.strip_suffix(".json"))
                    .map(str::to_string)
            })
            .collect();
        let (names, total) = page.select(names);

        let mut items = Vec::with_capacity(names.len());
        for name in names {
            let detail: GroupDetailRaw = self.get_json(&self.keys().group(&name)).await?;
            items.push(GroupSummary {
                state: derive_state(&detail),
                members: detail.members.len(),
                name,
            });
        }
        Ok(Paged::new(items, total, page))
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

        // Committed offsets: list the group's offsets prefix, parse (topic, partition).
        let mut topic_partitions = Vec::new();
        let offsets_prefix = self.keys().group_offsets_prefix(group);
        let mut stream = self.store().list(Some(&offsets_prefix));
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            if let Some(tp) = Keys::topic_partition_from_offset(&meta.location) {
                topic_partitions.push(tp);
            }
        }
        topic_partitions.sort();

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
}
