//! End-to-end consumer-group tests against a local MinIO.
//!
//! Self-contained: the test seeds a synthetic group (a `GroupDetail`, a
//! committed offset, and a watermark) directly into the bucket, asserts the
//! read path, then cleans up — so it doesn't depend on demo data.
//!
//! Gated behind `#[ignore]`. To run:
//!
//! ```bash
//! docker compose up -d minio createbucket
//! cargo test --test groups_integration -- --ignored
//! ```

use bytes::Bytes;
use object_store::{ObjectStore, PutPayload};

use kotatsu::{
    config::S3Config,
    pagination::Page,
    storage::{LagMode, StorageSource},
};

const GROUP: &str = "kotatsu-it-group";
const TOPIC: &str = "kotatsu-it-topic";

fn demo_source() -> StorageSource {
    source_for("demo")
}

fn source_for(cluster: &str) -> StorageSource {
    let cfg = S3Config {
        provider: kotatsu::config::StorageProvider::S3,
        bucket: "tansu".into(),
        cluster: cluster.into(),
        endpoint: Some("http://127.0.0.1:9000".into()),
        region: "us-east-1".into(),
        access_key: Some("minioadmin".into()),
        secret_key: Some("minioadmin".into()),
        session_token: None,
        force_path_style: true,
        allow_http: true,
    };
    StorageSource::from_config(&cfg).expect("build source")
}

async fn put(source: &StorageSource, path: &object_store::path::Path, body: &str) {
    source
        .store()
        .put(path, PutPayload::from(Bytes::from(body.to_string())))
        .await
        .expect("put fixture");
}

#[tokio::test]
#[ignore = "requires local MinIO (docker compose up -d minio createbucket)"]
async fn seeded_group_lists_with_offsets_and_lag() {
    let source = demo_source();
    let keys = source.keys();

    let group_path = keys.group(GROUP);
    let offset_path = keys.group_offset(GROUP, TOPIC, 0);
    let watermark_path = keys.watermark(TOPIC, 0);

    // Seed: an Empty (no members) Forming group, committed offset 4, high 10.
    put(
        &source,
        &group_path,
        r#"{"members":{},"generation_id":0,"state":{"Forming":{"protocol_type":"consumer","protocol_name":"range","leader":null}}}"#,
    )
    .await;
    put(&source, &offset_path, r#"{"offset":4}"#).await;
    put(
        &source,
        &watermark_path,
        r#"{"low":0,"high":10,"timestamps":null}"#,
    )
    .await;

    // list_groups includes our group, derived state Empty. No lag asked for, so
    // none reported — the listing that existed before #107, at its old cost.
    let groups = source
        .list_groups(&Page::new(None, 200, 0), LagMode::Off)
        .await
        .expect("list groups")
        .items;
    let summary = groups
        .iter()
        .find(|g| g.name == GROUP)
        .expect("seeded group present");
    assert_eq!(summary.state, "Empty");
    assert_eq!(summary.members, 0);
    assert!(summary.lag.is_none());

    // Asked for, the list's figure is the same lag the detail page computes —
    // against a real store, where the high watermark comes from the segments and
    // not from a hand-written fixture (#107).
    let ranked = source
        .list_groups(&Page::new(Some(GROUP.into()), 200, 0), LagMode::RankAll)
        .await
        .expect("list groups with lag")
        .items;
    let lag = ranked
        .iter()
        .find(|g| g.name == GROUP)
        .expect("seeded group present")
        .lag
        .as_ref()
        .expect("lag requested");
    assert_eq!(lag.total, Some(6));
    assert_eq!(lag.topics, 1);
    assert_eq!(lag.max_partition, Some(6));

    // group_detail computes lag = high - committed = 10 - 4 = 6.
    let detail = source.group_detail(GROUP).await.expect("group detail");
    assert_eq!(detail.offsets.len(), 1);
    let o = &detail.offsets[0];
    assert_eq!(o.topic, TOPIC);
    assert_eq!(o.partition, 0);
    assert_eq!(o.committed_offset, 4);
    assert_eq!(o.high_watermark, 10);
    assert_eq!(o.lag, 6);

    // Cleanup.
    for path in [&group_path, &offset_path, &watermark_path] {
        let _ = source.store().delete(path).await;
    }
}

#[tokio::test]
#[ignore = "requires local MinIO"]
async fn unknown_group_errors() {
    let err = demo_source()
        .group_detail("kotatsu-it-does-not-exist")
        .await
        .expect_err("should not be found");
    assert!(err.to_string().contains("not found"), "got: {err}");
}

/// The layout a current broker writes (Popsink/kodansu#359, #415): no
/// `{group}.json`, membership in `generation.json` + `assignment/`, offsets in
/// `offsets.json` with a stale per-partition object beside it. Seeded under a
/// cluster of its own, because `schema/groups.json` switches the whole cluster.
#[tokio::test]
#[ignore = "requires local MinIO"]
async fn decomposed_group_lists_describes_and_prefers_offsets_json() {
    let source = source_for("kotatsu-it-decomposed");
    let keys = source.keys();

    let schema = keys.group_schema();
    let generation = keys.group_generation(GROUP);
    let assignment = keys.group_assignment(GROUP, 3);
    let offsets = keys.group_offsets_doc(GROUP);
    let stale = keys.group_offset(GROUP, TOPIC, 0);
    let watermark = keys.watermark(TOPIC, 0);

    put(&source, &schema, r#"{"version":2}"#).await;
    put(
        &source,
        &generation,
        r#"{"seq":1,"generation_id":3,"protocol_type":"consumer","protocol_name":"range","leader":"m-1","members":{"m-1":{}},"members_changed_at_ms":0,"state_since_ms":0,"swept_at_ms":0,"session_timeout_ms":45000,"inception_ms":0}"#,
    )
    .await;
    // version 0, one topic of 16 bytes, partition [0], no user data.
    let mut blob = vec![0u8, 0, 0, 0, 0, 1, 0, TOPIC.len() as u8];
    blob.extend_from_slice(TOPIC.as_bytes());
    blob.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 0, 255, 255, 255, 255]);
    put(
        &source,
        &assignment,
        &format!(
            r#"{{"generation_id":3,"leader":"m-1","protocol_type":"consumer","protocol_name":"range","assignments":{{"m-1":{blob:?}}},"assigned_at_ms":0}}"#
        ),
    )
    .await;
    put(
        &source,
        &offsets,
        &format!(r#"{{"committed":{{"{TOPIC}":{{"0":{{"offset":8,"leader_epoch":null,"timestamp":null,"metadata":null}}}}}}}}"#),
    )
    .await;
    put(&source, &stale, r#"{"offset":4}"#).await;
    put(
        &source,
        &watermark,
        r#"{"low":0,"high":10,"timestamps":null}"#,
    )
    .await;

    let groups = source
        .list_groups(&Page::new(Some(GROUP.into()), 200, 0), LagMode::Off)
        .await
        .expect("list groups")
        .items;
    let summary = groups
        .iter()
        .find(|g| g.name == GROUP)
        .expect("a group with no {group}.json is listed");
    assert_eq!(summary.state, "Stable");
    assert_eq!(summary.members, 1);

    let detail = source.group_detail(GROUP).await.expect("group detail");
    assert_eq!(detail.generation_id, 3);
    assert_eq!(detail.members.len(), 1);
    assert_eq!(detail.members[0].assignments[0].partitions, vec![0]);
    assert_eq!(detail.offsets.len(), 1);
    assert_eq!(
        detail.offsets[0].committed_offset, 8,
        "offsets.json, not the stale object"
    );
    assert_eq!(detail.offsets[0].lag, 2);

    let consuming = source.groups_consuming(TOPIC).await.expect("consuming");
    assert!(consuming.iter().any(|g| g.group == GROUP));

    for path in [
        &schema,
        &generation,
        &assignment,
        &offsets,
        &stale,
        &watermark,
    ] {
        let _ = source.store().delete(path).await;
    }
}
