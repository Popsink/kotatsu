//! Builders for Tansu's S3 key layout.
//!
//! Reverse-engineered from `tansu-storage::dynostore`:
//!
//! ```text
//! clusters/{cluster}/meta.json
//! clusters/{cluster}/topic-metadata/{topic}.json
//! clusters/{cluster}/topic-routing/{topic}.json
//! clusters/{cluster}/topics/{topic}/partitions/{partition:010}/watermark.json
//! clusters/{cluster}/prefixes/{prefix}/segments/{seq:020}.seg
//! clusters/{cluster}/groups/consumers/{group}.json
//! clusters/{cluster}/groups/consumers/{group}/offsets/{topic}/partitions/{partition:010}.json
//! ```
//!
//! Partitions are zero-padded to 10 digits and segment sequences to 20, matching
//! Tansu — so lexicographic listing order equals numeric order.
//!
//! Records live only in segments (#93): the pre-segment
//! `partitions/{partition}/records/{base_offset:020}.batch` layout has no builder
//! here any more, because the broker neither writes it, reads it, nor probes for
//! it (Popsink/tansu#179 / #199).

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

    /// `clusters/` — root prefix for listing cluster names.
    pub fn clusters_root() -> Path {
        Path::from("clusters/")
    }

    /// `clusters/{cluster}/meta.json`
    pub fn meta(&self) -> Path {
        Path::from(format!("clusters/{}/meta.json", self.cluster))
    }

    /// `clusters/{cluster}/topics/` — prefix for listing topics.
    pub fn topics_prefix(&self) -> Path {
        Path::from(format!("clusters/{}/topics/", self.cluster))
    }

    /// `clusters/{cluster}/topic-metadata/` — prefix for listing per-topic
    /// metadata objects (Tansu's decomposed topic metadata).
    pub fn topic_metadata_prefix(&self) -> Path {
        Path::from(format!("clusters/{}/topic-metadata/", self.cluster))
    }

    /// `clusters/{cluster}/topic-metadata/{topic}.json`
    pub fn topic_metadata(&self, topic: &str) -> Path {
        Path::from(format!(
            "clusters/{}/topic-metadata/{}.json",
            self.cluster, topic
        ))
    }

    /// `.../partitions/{partition:010}/watermark.json`
    pub fn watermark(&self, topic: &str, partition: i32) -> Path {
        Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/watermark.json",
            self.cluster, topic, partition
        ))
    }

    /// `clusters/{cluster}/topic-routing/{topic}.json` — the pinned routing
    /// prefix (Popsink/tansu#236), `{"prefix": "…"}`. Written create-only with the
    /// topic, immutable for its lifetime, deleted with it.
    pub fn topic_routing(&self, topic: &str) -> Path {
        Path::from(format!(
            "clusters/{}/topic-routing/{}.json",
            self.cluster, topic
        ))
    }

    /// The connector prefix Tansu **derives** for a topic (`prefix_of`,
    /// Popsink/tansu#57): the first three dot-separated components
    /// (`org.env.conn`) — the tenant/retention/isolation boundary the
    /// virtual-topics epic groups on. A topic with fewer than three components is
    /// its own prefix.
    ///
    /// This is **not** the mapping from a topic to where its records live: since
    /// Popsink/tansu#236 that is pinned per topic at creation, in
    /// [`Self::topic_routing`], because the derivation depends on `cleanup.policy`
    /// (a compacted topic routes under its own name) and `AlterConfigs` can flip
    /// that after records have been written. Resolve through the pin
    /// (`route_of`); this remains only the fallback derivation for a topic
    /// created before pinning existed, and the grouping the topic tree navigates
    /// by name ([`super::CONNECTOR_DEPTH`]).
    pub fn prefix_of(topic: &str) -> String {
        let mut parts = topic.split('.');
        let mut prefix = String::new();
        for i in 0..3 {
            match parts.next() {
                Some(part) => {
                    if i > 0 {
                        prefix.push('.');
                    }
                    prefix.push_str(part);
                }
                None => return topic.to_owned(),
            }
        }
        prefix
    }

    /// `clusters/{cluster}/prefixes/{prefix}/segments/` — prefix for listing a
    /// connector's virtual-topic segment objects.
    pub fn segment_prefix(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/segments/",
            self.cluster, prefix
        ))
    }

    /// `clusters/{cluster}/prefixes/{prefix}/segments/{seq:020}.seg`
    pub fn segment(&self, prefix: &str, seq: u64) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/segments/{:0>20}.seg",
            self.cluster, prefix, seq
        ))
    }

    /// The segment sequence encoded in a segment object filename, e.g.
    /// `.../segments/00000000000000000042.seg` → `42`.
    pub fn seq_from_segment(path: &Path) -> Option<u64> {
        let name = path.parts().last()?;
        let name = name.as_ref().strip_suffix(".seg")?;
        name.parse().ok()
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

    /// `clusters/{cluster}/groups/consumers/{group}/offsets/` — prefix for listing committed offsets.
    pub fn group_offsets_prefix(&self, group: &str) -> Path {
        Path::from(format!(
            "clusters/{}/groups/consumers/{}/offsets/",
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

    /// Parses `(topic, partition)` from a committed-offset object path
    /// `.../{group}/offsets/{topic}/partitions/{partition:010}.json`.
    pub fn topic_partition_from_offset(path: &Path) -> Option<(String, i32)> {
        let parts: Vec<String> = path.parts().map(|p| p.as_ref().to_string()).collect();
        let idx = parts.iter().position(|p| p == "offsets")?;
        // expect [..., offsets, {topic}, partitions, {partition:010}.json]
        let topic = parts.get(idx + 1)?.clone();
        let partition = parts.get(idx + 3)?.strip_suffix(".json")?.parse().ok()?;
        Some((topic, partition))
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
            k.topic_routing("orders").as_ref(),
            "clusters/c1/topic-routing/orders.json"
        );
        assert_eq!(
            k.group_offset("g1", "orders", 0).as_ref(),
            "clusters/c1/groups/consumers/g1/offsets/orders/partitions/0000000000.json"
        );
        assert_eq!(k.groups_prefix().as_ref(), "clusters/c1/groups/consumers");
    }

    #[test]
    fn prefix_of_takes_first_three_dotted_components() {
        assert_eq!(Keys::prefix_of("org.env.conn.orders"), "org.env.conn");
        assert_eq!(Keys::prefix_of("org.env.conn"), "org.env.conn");
        // Fewer than three components ⇒ the topic is its own prefix.
        assert_eq!(Keys::prefix_of("orders"), "orders");
        assert_eq!(Keys::prefix_of("a.b"), "a.b");
        // A fourth component and beyond stays out of the prefix.
        assert_eq!(Keys::prefix_of("a.b.c.d.e"), "a.b.c");
    }

    #[test]
    fn builds_and_parses_segment_keys() {
        let k = Keys::new("c1");
        assert_eq!(
            k.segment_prefix("org.env.conn").as_ref(),
            "clusters/c1/prefixes/org.env.conn/segments"
        );
        let seg = k.segment("org.env.conn", 42);
        assert_eq!(
            seg.as_ref(),
            "clusters/c1/prefixes/org.env.conn/segments/00000000000000000042.seg"
        );
        assert_eq!(Keys::seq_from_segment(&seg), Some(42));
        // A non-segment path yields None.
        assert_eq!(Keys::seq_from_segment(&k.meta()), None);
    }
}
