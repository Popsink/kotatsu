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
async fn watermark_reports_offsets() {
    let wm = demo_source().watermark("orders", 0).await.unwrap();
    // The demo seed produces at least 5 messages; more may be added over time.
    assert_eq!(wm.low, 0);
    assert!(wm.high >= 5, "high = {}", wm.high);
}

#[tokio::test]
#[ignore = "requires local MinIO + Tansu demo data"]
async fn fetch_earliest_returns_first_records_in_order() {
    let records = demo_source()
        .fetch("orders", 0, OffsetSpec::Earliest, 5)
        .await
        .unwrap();
    assert_eq!(records.len(), 5);
    // The first five produced messages: offsets 0..4, keys key-1..key-5.
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.offset, i as i64);
        assert_eq!(key_of(r), format!("key-{}", i + 1));
    }
}

#[tokio::test]
#[ignore = "requires local MinIO + Tansu demo data"]
async fn fetch_latest_returns_tail() {
    let source = demo_source();
    let high = source.watermark("orders", 0).await.unwrap().high;
    let records = source.fetch("orders", 0, OffsetSpec::Latest, 2).await.unwrap();
    assert_eq!(records.len(), 2);
    // The last two offsets, contiguous up to the high watermark.
    assert_eq!(records[0].offset, high - 2);
    assert_eq!(records[1].offset, high - 1);
}

#[tokio::test]
#[ignore = "requires local MinIO + Tansu demo data"]
async fn fetch_at_mid_batch_uses_predecessor() {
    let source = demo_source();
    let high = source.watermark("orders", 0).await.unwrap().high;
    // Reading from offset 2 must start exactly at 2 and run to the end.
    let records = source.fetch("orders", 0, OffsetSpec::At(2), 1000).await.unwrap();
    assert_eq!(records.first().unwrap().offset, 2);
    assert_eq!(records.len(), (high - 2) as usize);
    // Offsets are contiguous.
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.offset, 2 + i as i64);
    }
}
