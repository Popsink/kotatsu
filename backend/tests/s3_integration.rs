//! End-to-end reader tests against a local MinIO seeded by Tansu.
//!
//! Gated behind `#[ignore]` so CI without the stack stays green. To run:
//!
//! ```bash
//! docker compose up -d minio createbucket tansu
//! # produce a few messages to topic `orders` (see README), then:
//! cargo test --test s3_integration -- --ignored
//! ```

use kotatsu::{
    config::S3Config,
    storage::{OffsetSpec, StorageSource},
};

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

fn key_of(record: &kotatsu::storage::DecodedRecord) -> String {
    String::from_utf8(record.key.as_ref().unwrap().to_vec()).unwrap()
}

#[tokio::test]
#[ignore = "requires local MinIO + Tansu demo data"]
async fn watermark_reports_five_messages() {
    let wm = demo_source().watermark("orders", 0).await.unwrap();
    assert_eq!(wm.low, 0);
    assert_eq!(wm.high, 5);
    assert_eq!(wm.count(), 5);
}

#[tokio::test]
#[ignore = "requires local MinIO + Tansu demo data"]
async fn fetch_earliest_returns_all_in_order() {
    let records = demo_source()
        .fetch("orders", 0, OffsetSpec::Earliest, 100)
        .await
        .unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(records[0].offset, 0);
    assert_eq!(key_of(&records[0]), "key-1");
    assert_eq!(records[4].offset, 4);
    assert_eq!(key_of(&records[4]), "key-5");
}

#[tokio::test]
#[ignore = "requires local MinIO + Tansu demo data"]
async fn fetch_latest_returns_tail() {
    let records = demo_source()
        .fetch("orders", 0, OffsetSpec::Latest, 2)
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].offset, 3);
    assert_eq!(records[1].offset, 4);
}

#[tokio::test]
#[ignore = "requires local MinIO + Tansu demo data"]
async fn fetch_at_mid_batch_uses_predecessor() {
    // Offset 2 sits at a batch boundary here; reading from it must return 2..4.
    let records = demo_source()
        .fetch("orders", 0, OffsetSpec::At(2), 100)
        .await
        .unwrap();
    assert_eq!(records.first().unwrap().offset, 2);
    assert_eq!(records.len(), 3);
}
