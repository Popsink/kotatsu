//! Builders for Tansu's S3 key layout.
//!
//! Reverse-engineered from `tansu-storage::dynostore`:
//!
//! ```text
//! clusters/{cluster}/meta.json
//! clusters/{cluster}/topics/{topic}/partitions/{partition:010}/watermark.json
//! clusters/{cluster}/topics/{topic}/partitions/{partition:010}/records/{base_offset:020}.batch
//! clusters/{cluster}/groups/consumers/{group}.json
//! clusters/{cluster}/groups/consumers/{group}/offsets/{topic}/partitions/{partition:010}.json
//! ```
//!
//! Partitions are zero-padded to 10 digits and base offsets to 20, matching
//! Tansu — so lexicographic listing order equals numeric order.

use object_store::path::Path;

/// Builds S3 keys for a single Tansu cluster.
#[derive(Clone, Debug)]
pub struct Keys {
    cluster: String,
}

impl Keys {
    pub fn new(cluster: impl Into<String>) -> Self {
        Self {
            cluster: cluster.into(),
        }
    }

    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    /// `clusters/{cluster}/meta.json`
    pub fn meta(&self) -> Path {
        Path::from(format!("clusters/{}/meta.json", self.cluster))
    }

    /// `clusters/{cluster}/topics/` — prefix for listing topics.
    pub fn topics_prefix(&self) -> Path {
        Path::from(format!("clusters/{}/topics/", self.cluster))
    }

    /// `clusters/{cluster}/topics/{topic}/partitions/` — prefix for listing partitions.
    pub fn partitions_prefix(&self, topic: &str) -> Path {
        Path::from(format!("clusters/{}/topics/{}/partitions/", self.cluster, topic))
    }

    /// `.../partitions/{partition:010}/watermark.json`
    pub fn watermark(&self, topic: &str, partition: i32) -> Path {
        Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/watermark.json",
            self.cluster, topic, partition
        ))
    }

    /// `.../partitions/{partition:010}/records/` — prefix for listing record batches.
    pub fn records_prefix(&self, topic: &str, partition: i32) -> Path {
        Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/",
            self.cluster, topic, partition
        ))
    }

    /// `.../records/{base_offset:020}.batch`
    pub fn batch(&self, topic: &str, partition: i32, base_offset: i64) -> Path {
        Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
            self.cluster, topic, partition, base_offset
        ))
    }

    /// `clusters/{cluster}/groups/consumers/` — prefix for listing groups.
    pub fn groups_prefix(&self) -> Path {
        Path::from(format!("clusters/{}/groups/consumers/", self.cluster))
    }

    /// `clusters/{cluster}/groups/consumers/{group}.json`
    pub fn group(&self, group: &str) -> Path {
        Path::from(format!(
            "clusters/{}/groups/consumers/{}.json",
            self.cluster, group
        ))
    }

    /// `.../groups/consumers/{group}/offsets/{topic}/partitions/{partition:010}.json`
    pub fn group_offset(&self, group: &str, topic: &str, partition: i32) -> Path {
        Path::from(format!(
            "clusters/{}/groups/consumers/{}/offsets/{}/partitions/{:0>10}.json",
            self.cluster, group, topic, partition
        ))
    }

    /// The base offset encoded in a record batch filename, e.g.
    /// `.../records/00000000000000001234.batch` → `1234`.
    pub fn base_offset_from_batch(path: &Path) -> Option<i64> {
        let name = path.parts().last()?;
        let name = name.as_ref().strip_suffix(".batch")?;
        name.get(..20)?.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_layout_keys() {
        let k = Keys::new("c1");
        assert_eq!(k.meta().as_ref(), "clusters/c1/meta.json");
        assert_eq!(
            k.watermark("orders", 3).as_ref(),
            "clusters/c1/topics/orders/partitions/0000000003/watermark.json"
        );
        assert_eq!(
            k.batch("orders", 3, 1234).as_ref(),
            "clusters/c1/topics/orders/partitions/0000000003/records/00000000000000001234.batch"
        );
        assert_eq!(
            k.group_offset("g1", "orders", 0).as_ref(),
            "clusters/c1/groups/consumers/g1/offsets/orders/partitions/0000000000.json"
        );
        assert_eq!(k.groups_prefix().as_ref(), "clusters/c1/groups/consumers");
    }

    #[test]
    fn parses_base_offset_from_batch_name() {
        let k = Keys::new("c1");
        let p = k.batch("orders", 3, 1234);
        assert_eq!(Keys::base_offset_from_batch(&p), Some(1234));

        let not_batch = k.watermark("orders", 3);
        assert_eq!(Keys::base_offset_from_batch(&not_batch), None);
    }
}
