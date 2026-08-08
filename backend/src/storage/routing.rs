//! Resolving the **prefix a topic's records are segment-routed under** (#92).
//!
//! Every topic is segment-routed (Popsink/tansu#199), so this is the mapping that
//! decides which `prefixes/{prefix}/segments/` a topition's records are read from.
//! It is not derivable from the topic name: since Popsink/tansu#236 the broker
//! resolves it from a **pinned** object,
//!
//! ```text
//! clusters/{cluster}/topic-routing/{topic}.json    {"prefix": "..."}
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
//! Kotatsu is read-only: it reads the pin and never writes one, unlike the broker,
//! which lazily pins a pre-#236 topic's derivation on first resolution.

use serde::Deserialize;

use super::{keys::Keys, StorageError, StorageSource};

/// The pinned routing object's shape. Only `prefix` matters; anything else the
/// broker adds later is ignored rather than a parse failure.
#[derive(Deserialize)]
struct TopicRouting {
    prefix: String,
}

impl StorageSource {
    /// The prefix `topic`'s records are segment-routed under.
    ///
    /// Three steps, in cost order — the same three the broker's
    /// `routed_prefix_of` takes, so the two cannot resolve differently:
    ///
    /// 1. A topic whose connector prefix already equals its own name (fewer than
    ///    three dotted components) needs nothing: both routings agree, so there is
    ///    no decision to pin and no request to make.
    /// 2. The permanent memo. Sound because the pin is immutable for the topic's
    ///    lifetime — no TTL and no staleness argument, the same reasoning as the
    ///    footer cache.
    /// 3. Otherwise read the pin. A topic created before pinning existed has none,
    ///    and then the fallback reproduces exactly the broker's: [`Keys::prefix_of`]
    ///    plus the compacted verdict, read off the same `cleanup.policy` the broker
    ///    reads. Reproducing it is the point — a different answer would look for a
    ///    topic's segments under a prefix they are not filed under, which does not
    ///    read as an error but as an empty topic.
    pub(super) async fn routed_prefix_of(&self, topic: &str) -> Result<String, StorageError> {
        let derived = Keys::prefix_of(topic);
        if derived == topic {
            return Ok(derived);
        }

        if let Some(pinned) = self
            .routing_prefixes
            .lock()
            .ok()
            .and_then(|memo| memo.get(topic).cloned())
        {
            return Ok(pinned);
        }

        let resolved = match self.read_routing_pin(topic).await? {
            Some(pinned) => pinned,
            // Pre-#236 topic: derive as the broker does when it finds no pin.
            None if self.topic_is_compacted(topic).await? => topic.to_owned(),
            None => derived,
        };

        if let Ok(mut memo) = self.routing_prefixes.lock() {
            memo.insert(topic.to_owned(), resolved.clone());
        }
        Ok(resolved)
    }

    /// The pinned prefix for `topic`, or `None` when the object does not exist (a
    /// topic created before Popsink/tansu#236). One GET; the caller memoizes.
    async fn read_routing_pin(&self, topic: &str) -> Result<Option<String>, StorageError> {
        match self
            .get_json::<TopicRouting>(&self.keys().topic_routing(topic))
            .await
        {
            Ok(routing) => Ok(Some(routing.prefix)),
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

        assert_eq!(src.routed_prefix_of(topic).await.unwrap(), topic);
        assert_eq!(
            Keys::prefix_of(topic),
            "acme.prod.db2",
            "the derivation this replaces"
        );
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
            src.routed_prefix_of(topic).await.unwrap(),
            "pinned.somewhere.else"
        );
        store
            .delete(&src.keys().topic_routing(topic))
            .await
            .unwrap();
        assert_eq!(
            src.routed_prefix_of(topic).await.unwrap(),
            "pinned.somewhere.else",
            "served from the memo, not re-read"
        );
    }

    /// A topic that is its own prefix needs no pin read at all — proven by
    /// resolving it against a store holding nothing.
    #[tokio::test]
    async fn short_topic_resolves_without_reading_anything() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store, "c");
        assert_eq!(src.routed_prefix_of("orders").await.unwrap(), "orders");
        assert_eq!(src.routed_prefix_of("a.b").await.unwrap(), "a.b");
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
            src.routed_prefix_of("acme.prod.db2.orders").await.unwrap(),
            "acme.prod.db2"
        );
        assert_eq!(
            src.routed_prefix_of("acme.prod.db2.dbz_config")
                .await
                .unwrap(),
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

        assert_eq!(src.routed_prefix_of(topic).await.unwrap(), topic);
    }

    /// Neither pin nor metadata (a topic being deleted under us): resolve to the
    /// derivation rather than failing the read.
    #[tokio::test]
    async fn missing_pin_and_missing_metadata_fall_back_to_the_derivation() {
        let store = Arc::new(InMemory::new());
        let src = StorageSource::with_store(store, "c");
        assert_eq!(
            src.routed_prefix_of("acme.prod.db2.orders").await.unwrap(),
            "acme.prod.db2"
        );
    }
}
