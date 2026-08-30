//! Resolving **where a topic's records are, and what they are keyed by** (#92,
//! #118).
//!
//! Every topic is segment-routed (Popsink/tansu#199), so this is the mapping that
//! decides which `prefixes/{prefix}/segments/` a topition's records are read from.
//! It is not derivable from the topic name: since Popsink/tansu#236 the broker
//! resolves it from a **pinned** object,
//!
//! ```text
//! clusters/{cluster}/topic-routing/{topic}.json
//! {"prefix": "org.env.conn", "substream_id": "0192…"}
//! ```
//!
//! written create-only with the topic, immutable for its lifetime, and deleted
//! with it. Deriving it instead gets a compacted topic wrong: it is routed under
//! its **full topic name**, not its connector prefix, so that its segments never
//! share an object with a sibling whose whole-segment retention would delete the
//! compacted topic's old-but-latest keys. `AlterConfigs` can also flip
//! `cleanup.policy` after creation — the records stay where they were written, and
//! only the pin still says where that is.
//!
//! The same object also carries the topic's **sub-stream identity** since segment
//! footer v4 (#118): `substream_id` is present for an id-keyed topic and absent
//! for a name-keyed one, which is every topic created before the flip. Like the
//! prefix it is decided at creation and immutable for the topic's lifetime, so it
//! shares the prefix's memo and its no-staleness argument.
//!
//! Kotatsu is read-only: it reads the pin and never writes one, unlike the broker,
//! which lazily pins a pre-#236 topic's derivation on first resolution.

use serde::Deserialize;
use uuid::Uuid;

use super::{keys::Keys, segment::SubstreamId, StorageError, StorageSource};

/// The pinned routing object's shape. Anything the broker adds beyond these two
/// is ignored rather than a parse failure.
#[derive(Deserialize)]
struct TopicRouting {
    prefix: String,
    /// Absent for a name-keyed topic — every topic created before the v4 flip.
    #[serde(default)]
    substream_id: Option<Uuid>,
}

/// Where a topic's records live and how they are keyed inside the segments they
/// share: the routed prefix, plus the sub-stream identity its footer entries
/// carry. Resolved once per topic per process and memoized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TopicRoute {
    /// The `prefixes/{prefix}/segments/` its records are filed under.
    pub prefix: String,
    /// `Some` for an id-keyed topic, `None` for a name-keyed one (#118).
    pub substream_id: Option<Uuid>,
}

impl TopicRoute {
    /// The key to match footer entries against. `topic` is used only when the
    /// route is name-keyed, and is then the only thing that identifies it.
    pub fn substream<'a>(&'a self, topic: &'a str) -> SubstreamId<'a> {
        match self.substream_id {
            Some(id) => SubstreamId::Id(id),
            None => SubstreamId::Name(topic),
        }
    }
}

impl StorageSource {
    /// How `topic`'s records are located: the prefix they are segment-routed
    /// under and the identity their sub-stream entries carry.
    ///
    /// Two steps — the same the broker's `routed_prefix_of` takes, so the two
    /// cannot resolve differently:
    ///
    /// 1. The permanent memo. Sound because the pin is immutable for the topic's
    ///    lifetime — no TTL and no staleness argument, the same reasoning as the
    ///    footer cache.
    /// 2. Otherwise read the pin. A topic created before pinning existed has none,
    ///    and then the fallback reproduces exactly the broker's: [`Keys::prefix_of`]
    ///    plus the compacted verdict, read off the same `cleanup.policy` the broker
    ///    reads, and name-keyed because a topic that predates the pin predates v4
    ///    by years. Reproducing it is the point — a different answer would look for
    ///    a topic's segments under a prefix they are not filed under, which does not
    ///    read as an error but as an empty topic.
    ///
    /// There used to be a third, cheaper step in front: a topic whose connector
    /// prefix already equals its own name (fewer than three dotted components)
    /// answered without reading anything, both routings agreeing. That shortcut
    /// **cannot survive v4** — whether a topic is id-keyed is not derivable from
    /// its name, and answering "name-keyed" without looking serves an id-keyed
    /// topic's reads against a key its records were never written under, which
    /// renders as an empty topic. The broker removed the same shortcut for the
    /// same reason. The cost is one GET per topic per process, memoized
    /// permanently, and the pin exists for every topic created since
    /// Popsink/tansu#236 — a hit, not a 404, in the common case.
    pub(super) async fn route_of(&self, topic: &str) -> Result<TopicRoute, StorageError> {
        if let Some(memoized) = self
            .topic_routes
            .lock()
            .ok()
            .and_then(|memo| memo.get(topic).cloned())
        {
            return Ok(memoized);
        }

        let resolved = match self.read_routing_pin(topic).await? {
            Some(routing) => TopicRoute {
                prefix: routing.prefix,
                substream_id: routing.substream_id.filter(|id| !id.is_nil()),
            },
            // Pre-#236 topic: derive as the broker does when it finds no pin.
            None => TopicRoute {
                prefix: if self.topic_is_compacted(topic).await? {
                    topic.to_owned()
                } else {
                    Keys::prefix_of(topic)
                },
                substream_id: None,
            },
        };

        if let Ok(mut memo) = self.topic_routes.lock() {
            memo.insert(topic.to_owned(), resolved.clone());
        }
        Ok(resolved)
    }

    /// The pinned routing for `topic`, or `None` when the object does not exist (a
    /// topic created before Popsink/tansu#236). One GET; the caller memoizes.
    async fn read_routing_pin(&self, topic: &str) -> Result<Option<TopicRouting>, StorageError> {
        match self
            .get_json::<TopicRouting>(&self.keys().topic_routing(topic))
            .await
        {
            Ok(routing) => Ok(Some(routing)),
            Err(StorageError::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{memory::InMemory, ObjectStore, PutPayload};
    use std::sync::Arc;

    async fn put(store: &InMemory, path: &object_store::path::Path, body: &str) {
        store
            .put(path, PutPayload::from(body.as_bytes().to_vec()))
            .await
            .unwrap();
    }

    /// The pin wins over the derivation — the #92 case: a compacted topic with
    /// four components is routed under its own name, not `org.env.conn`.
    #[tokio::test]
    async fn pin_overrides_the_derivation() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        let topic = "acme.prod.db2.dbz_config";
        put(
            &store,
            &src.keys().topic_routing(topic),
            &format!(r#"{{"prefix":"{topic}"}}"#),
        )
        .await;

        assert_eq!(src.route_of(topic).await.unwrap().prefix, topic);
        assert_eq!(
            Keys::prefix_of(topic),
            "acme.prod.db2",
            "the derivation this replaces"
        );
    }

    /// #118: the pin also carries the sub-stream identity. Present ⇒ id-keyed,
    /// and the route matches footer entries by that uuid rather than by name.
    #[tokio::test]
    async fn pin_carries_the_substream_id() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        let topic = "acme.prod.db2.orders";
        let id = Uuid::from_u128(0x0192);
        put(
            &store,
            &src.keys().topic_routing(topic),
            &format!(r#"{{"prefix":"acme.prod.db2","substream_id":"{id}"}}"#),
        )
        .await;

        let route = src.route_of(topic).await.unwrap();
        assert_eq!(route.prefix, "acme.prod.db2");
        assert_eq!(route.substream_id, Some(id));
        assert_eq!(route.substream(topic), SubstreamId::Id(id));
    }

    /// A pin without `substream_id` — every topic created before the flip — is
    /// name-keyed, and the field's absence is not a parse failure.
    #[tokio::test]
    async fn a_pin_without_an_id_is_name_keyed() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        let topic = "acme.prod.db2.orders";
        put(
            &store,
            &src.keys().topic_routing(topic),
            r#"{"prefix":"acme.prod.db2"}"#,
        )
        .await;

        let route = src.route_of(topic).await.unwrap();
        assert_eq!(route.substream_id, None);
        assert_eq!(route.substream(topic), SubstreamId::Name(topic));
    }

    /// The pin is read at most once per topic per process: after the first
    /// resolution the object can vanish and the answer is unchanged.
    #[tokio::test]
    async fn pin_is_read_once_and_memoized() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        let topic = "acme.prod.db2.orders";
        put(
            &store,
            &src.keys().topic_routing(topic),
            r#"{"prefix":"pinned.somewhere.else"}"#,
        )
        .await;

        assert_eq!(
            src.route_of(topic).await.unwrap().prefix,
            "pinned.somewhere.else"
        );
        store
            .delete(&src.keys().topic_routing(topic))
            .await
            .unwrap();
        assert_eq!(
            src.route_of(topic).await.unwrap().prefix,
            "pinned.somewhere.else",
            "served from the memo, not re-read"
        );
    }

    /// #118: a topic that is its own prefix used to answer without reading
    /// anything. It cannot any more — the prefix agrees either way, but whether
    /// the topic is id-keyed is not in its name, and guessing "name-keyed" reads
    /// an id-keyed topic as empty.
    #[tokio::test]
    async fn a_short_topic_still_reads_its_pin() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        let id = Uuid::from_u128(3);
        put(
            &store,
            &src.keys().topic_routing("orders"),
            &format!(r#"{{"prefix":"orders","substream_id":"{id}"}}"#),
        )
        .await;

        let route = src.route_of("orders").await.unwrap();
        assert_eq!(route.prefix, "orders");
        assert_eq!(
            route.substream_id,
            Some(id),
            "the shortcut would have answered name-keyed without looking"
        );
    }

    /// With no pin, a topic that is its own prefix still resolves to itself.
    #[tokio::test]
    async fn short_topic_without_a_pin_is_its_own_prefix() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store, "c");
        for topic in ["orders", "a.b"] {
            let route = src.route_of(topic).await.unwrap();
            assert_eq!(route.prefix, topic);
            assert_eq!(route.substream_id, None);
        }
    }

    /// No pin (a pre-#236 topic): resolve exactly as before — the derivation for a
    /// delete-policy topic, its own name when `cleanup.policy` contains `compact`.
    #[tokio::test]
    async fn without_a_pin_the_compacted_verdict_decides() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");

        for (topic, policy) in [
            ("acme.prod.db2.orders", "delete"),
            ("acme.prod.db2.dbz_config", "compact"),
        ] {
            let meta = serde_json::json!({
                "topic": {
                    "name": topic,
                    "num_partitions": 1,
                    "replication_factor": 1,
                    "configs": [{ "name": "cleanup.policy", "value": policy }]
                }
            });
            put(
                &store,
                &src.keys().topic_metadata(topic),
                &serde_json::to_string(&meta).unwrap(),
            )
            .await;
        }

        assert_eq!(
            src.route_of("acme.prod.db2.orders").await.unwrap().prefix,
            "acme.prod.db2"
        );
        assert_eq!(
            src.route_of("acme.prod.db2.dbz_config")
                .await
                .unwrap()
                .prefix,
            "acme.prod.db2.dbz_config",
            "compacted ⇒ routed under its own name"
        );
    }

    /// A `compact,delete` policy contains `compact`, so it routes as compacted —
    /// the broker's test is a substring, not equality.
    #[tokio::test]
    async fn compact_and_delete_counts_as_compacted() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store.clone(), "c");
        let topic = "acme.prod.db2.mixed";
        let meta = serde_json::json!({
            "topic": {
                "name": topic,
                "num_partitions": 1,
                "configs": [{ "name": "cleanup.policy", "value": "compact,delete" }]
            }
        });
        put(
            &store,
            &src.keys().topic_metadata(topic),
            &serde_json::to_string(&meta).unwrap(),
        )
        .await;

        assert_eq!(src.route_of(topic).await.unwrap().prefix, topic);
    }

    /// Neither pin nor metadata (a topic being deleted under us): resolve to the
    /// derivation rather than failing the read.
    #[tokio::test]
    async fn missing_pin_and_missing_metadata_fall_back_to_the_derivation() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store, "c");
        assert_eq!(
            src.route_of("acme.prod.db2.orders").await.unwrap().prefix,
            "acme.prod.db2"
        );
    }
}
