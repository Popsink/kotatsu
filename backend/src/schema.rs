//! Schema registry client (Popsink Kora — Confluent-compatible REST) and
//! Avro decoding for the event browser.
//!
//! Avro values are Confluent-framed: a `0x00` magic byte, a 4-byte big-endian
//! schema id, then the Avro body. Schema ids are immutable, so resolved schemas
//! are cached with no TTL.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("no schema registry configured")]
    NotConfigured,
    #[error("subject '{0}' not found")]
    SubjectNotFound(String),
    #[error("schema registry request failed: {0}")]
    Request(String),
}

/// A cached schema entry, keyed by Confluent schema id.
#[derive(Clone)]
struct Cached {
    schema_type: String,
    /// Parsed Avro schema (only for `AVRO` types).
    avro: Option<Arc<apache_avro::Schema>>,
}

/// Raw schema-by-id response from Kora.
#[derive(Deserialize)]
struct SchemaByIdResponse {
    schema: String,
    #[serde(rename = "schemaType", default = "default_type")]
    schema_type: String,
}

/// Schema version response from Kora.
#[derive(Deserialize, serde::Serialize)]
pub struct SchemaVersion {
    pub subject: String,
    pub id: i64,
    pub version: i32,
    #[serde(rename = "schemaType", default = "default_type")]
    pub schema_type: String,
    pub schema: String,
}

fn default_type() -> String {
    "AVRO".to_string()
}

/// Client for a Confluent-compatible schema registry, with an id→schema cache.
#[derive(Clone)]
pub struct SchemaRegistry {
    base_url: String,
    http: reqwest::Client,
    cache: Arc<Mutex<HashMap<u32, Cached>>>,
}

impl SchemaRegistry {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, SchemaError> {
        let url = format!("{}{path}", self.base_url);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| SchemaError::Request(e.to_string()))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(SchemaError::SubjectNotFound(path.to_string()));
        }
        let resp = resp
            .error_for_status()
            .map_err(|e| SchemaError::Request(e.to_string()))?;
        resp.json::<T>()
            .await
            .map_err(|e| SchemaError::Request(e.to_string()))
    }

    pub async fn subjects(&self) -> Result<Vec<String>, SchemaError> {
        self.get_json("/subjects").await
    }

    pub async fn versions(&self, subject: &str) -> Result<Vec<i32>, SchemaError> {
        self.get_json(&format!("/subjects/{subject}/versions"))
            .await
    }

    pub async fn version(
        &self,
        subject: &str,
        version: &str,
    ) -> Result<SchemaVersion, SchemaError> {
        self.get_json(&format!("/subjects/{subject}/versions/{version}"))
            .await
    }

    /// Resolves a schema id to a (cached) parsed entry.
    async fn schema_by_id(&self, id: u32) -> Result<Cached, SchemaError> {
        if let Some(hit) = self.cache.lock().unwrap().get(&id).cloned() {
            return Ok(hit);
        }
        let resp: SchemaByIdResponse = self.get_json(&format!("/schemas/ids/{id}")).await?;
        let avro = if resp.schema_type == "AVRO" {
            apache_avro::Schema::parse_str(&resp.schema)
                .ok()
                .map(Arc::new)
        } else {
            None
        };
        let cached = Cached {
            schema_type: resp.schema_type,
            avro,
        };
        self.cache.lock().unwrap().insert(id, cached.clone());
        Ok(cached)
    }
}

/// Decodes a record field (key or value) into a display value.
///
/// Confluent-framed Avro is decoded to JSON; otherwise the bytes are shown as
/// UTF-8 or hex. `registry` is `None` when no schema registry is configured.
pub async fn decode_field(registry: Option<&SchemaRegistry>, field: &Option<Bytes>) -> Value {
    let Some(bytes) = field else {
        return Value::Null;
    };

    // Confluent wire format: 0x00 + 4-byte big-endian schema id + payload.
    if bytes.len() >= 5 && bytes[0] == 0x00 {
        if let Some(registry) = registry {
            let id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
            if let Ok(cached) = registry.schema_by_id(id).await {
                if let Some(schema) = &cached.avro {
                    let mut payload = &bytes[5..];
                    if let Ok(value) = apache_avro::from_avro_datum(schema, &mut payload, None) {
                        if let Ok(data) = apache_avro::from_value::<Value>(&value) {
                            return json!({ "kind": "avro", "schemaId": id, "data": data });
                        }
                    }
                }
                // Framed but non-Avro or decode failed → surface the id + raw.
                return json!({ "kind": cached.schema_type.to_lowercase(), "schemaId": id, "data": raw_text(bytes) });
            }
        }
    }

    raw_field(bytes)
}

/// Encodes raw bytes as `{kind: utf8|hex, data}` (no schema involved).
pub fn raw_field(bytes: &Bytes) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(text) => json!({ "kind": "utf8", "data": text }),
        Err(_) => json!({ "kind": "hex", "data": hex(bytes) }),
    }
}

fn raw_text(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_string(),
        Err(_) => hex(bytes),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
