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

use kotatsu::{config::S3Config, storage::StorageSource};

const GROUP: &str = "kotatsu-it-group";
const TOPIC: &str = "kotatsu-it-topic";

fn demo_source() -> StorageSource {
    let cfg = S3Config {
        bucket: "tansu".into(),
        cluster: "demo".into(),
        endpoint: Some("http://127.0.0.1:9000".into()),
        region: "us-east-1".into(),
        access_key: Some("minioadmin".into()),
        secret_key: Some("minioadmin".into()),
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
    put(&source, &watermark_path, r#"{"low":0,"high":10,"timestamps":null}"#).await;

    // list_groups includes our group, derived state Empty.
    let groups = source.list_groups().await.expect("list groups");
    let summary = groups
        .iter()
        .find(|g| g.name == GROUP)
        .expect("seeded group present");
    assert_eq!(summary.state, "Empty");
    assert_eq!(summary.members, 0);

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
