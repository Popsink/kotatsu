//! End-to-end schema-registry tests against a local Kora.
//!
//! Self-contained: registers its own schema in Kora, then asserts the client
//! reads it back and that a Confluent-framed Avro datum decodes correctly.
//! Registration is idempotent, so the test is repeatable.
//!
//! Gated behind `#[ignore]`. To run:
//!
//! ```bash
//! docker compose up -d kora kora-db
//! cargo test --test schema_integration -- --ignored
//! ```

use apache_avro::{types::Record, Schema};
use bytes::Bytes;

use kotatsu::schema::{decode_field, SchemaRegistry};

const SUBJECT: &str = "kotatsu-it-value";
const SCHEMA: &str = r#"{"type":"record","name":"ItTest","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;

fn kora_url() -> String {
    std::env::var("KORA_URL").unwrap_or_else(|_| "http://127.0.0.1:8085".to_string())
}

/// Registers (idempotently) the schema in Kora and returns its id.
async fn register_schema() -> u32 {
    let body = serde_json::json!({ "schema": SCHEMA, "schemaType": "AVRO" });
    let resp = reqwest::Client::new()
        .post(format!("{}/subjects/{SUBJECT}/versions", kora_url()))
        .header("content-type", "application/vnd.schemaregistry.v1+json")
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .expect("register request")
        .error_for_status()
        .expect("register ok");
    let v: serde_json::Value = resp.json().await.expect("register json");
    v["id"].as_u64().expect("schema id") as u32
}

#[tokio::test]
#[ignore = "requires local Kora (docker compose up -d kora kora-db)"]
async fn registers_lists_and_reads_schema() {
    let id = register_schema().await;
    let registry = SchemaRegistry::new(kora_url());

    let subjects = registry.subjects().await.expect("subjects");
    assert!(
        subjects.contains(&SUBJECT.to_string()),
        "subjects: {subjects:?}"
    );

    let versions = registry.versions(SUBJECT).await.expect("versions");
    assert!(!versions.is_empty());

    let latest = registry.version(SUBJECT, "latest").await.expect("latest");
    assert_eq!(latest.schema_type, "AVRO");
    assert_eq!(latest.id as u32, id);
    Schema::parse_str(&latest.schema).expect("schema parses");
}

#[tokio::test]
#[ignore = "requires local Kora"]
async fn decodes_confluent_framed_avro() {
    let id = register_schema().await;
    let registry = SchemaRegistry::new(kora_url());

    // Build a Confluent-framed Avro datum: 0x00 + 4-byte BE id + Avro body.
    let schema = Schema::parse_str(SCHEMA).unwrap();
    let mut record = Record::new(&schema).unwrap();
    record.put("id", 7);
    record.put("name", "it");
    let datum = apache_avro::to_avro_datum(&schema, record).expect("encode avro");

    let mut framed = vec![0x00];
    framed.extend_from_slice(&id.to_be_bytes());
    framed.extend_from_slice(&datum);

    let decoded = decode_field(Some(&registry), &Some(Bytes::from(framed))).await;
    assert_eq!(decoded["kind"], "avro");
    assert_eq!(decoded["schemaId"].as_u64(), Some(id as u64));
    assert_eq!(decoded["data"]["id"], 7);
    assert_eq!(decoded["data"]["name"], "it");
}

#[tokio::test]
#[ignore = "requires local Kora"]
async fn non_framed_bytes_fall_back_to_utf8() {
    let registry = SchemaRegistry::new(kora_url());
    let decoded = decode_field(Some(&registry), &Some(Bytes::from_static(b"hello"))).await;
    assert_eq!(decoded["kind"], "utf8");
    assert_eq!(decoded["data"], "hello");
}
