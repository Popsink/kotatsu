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

#[derive(Debug, Serialize)]
pub struct GroupDetailView {
    pub name: String,
    pub state: &'static str,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub generation_id: i32,
    pub members: Vec<String>,
    pub offsets: Vec<GroupOffset>,
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
    /// Lists consumer groups (one `{group}.json` per group).
    pub async fn list_groups(&self) -> Result<Vec<GroupSummary>, StorageError> {
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

        let mut summaries = Vec::with_capacity(names.len());
        for name in names {
            let detail: GroupDetailRaw = self.get_json(&self.keys().group(&name)).await?;
            summaries.push(GroupSummary {
                state: derive_state(&detail),
                members: detail.members.len(),
                name,
            });
        }
        Ok(summaries)
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

        Ok(GroupDetailView {
            name: group.to_string(),
            state: derive_state(&detail),
            protocol_type,
            protocol_name,
            generation_id: detail.generation_id,
            members: detail.members.into_keys().collect(),
            offsets,
        })
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
}
