//! Schema registry client (Popsink Kora — Confluent-compatible REST) and
//! Avro decoding for the event browser.
//!
//! Avro values are Confluent-framed: a `0x00` magic byte, a 4-byte big-endian
//! schema id, then the Avro body. Schema ids are immutable, so resolved schemas
//! are cached with no TTL.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("no schema registry configured")]
    NotConfigured,
    /// The registry returned 404. User-facing context (which subject) is added
    /// by the caller; internal route/URL details stay in the logs.
    #[error("not found")]
    NotFound,
    #[error("schema registry is unreachable")]
    Unreachable,
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

/// Confluent `/config` response (compatibility level).
#[derive(Deserialize)]
struct ConfigResponse {
    #[serde(rename = "compatibilityLevel", alias = "compatibility")]
    compatibility_level: Option<String>,
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
        // Bounded timeouts so an unreachable registry (e.g. a Service scaled to
        // 0 that drops packets) fails fast instead of hanging on OS-level TCP
        // timeouts (~25–30 s). Read-only browsing must stay responsive.
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, SchemaError> {
        let url = format!("{}{path}", self.base_url);
        let resp = self.http.get(&url).send().await.map_err(|e| {
            tracing::warn!(%url, error = %e, "schema registry request failed");
            SchemaError::Unreachable
        })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(SchemaError::NotFound);
        }
        let resp = resp.error_for_status().map_err(|e| {
            tracing::warn!(%url, error = %e, "schema registry returned an error");
            SchemaError::Unreachable
        })?;
        resp.json::<T>().await.map_err(|e| {
            tracing::warn!(%url, error = %e, "schema registry response decode failed");
            SchemaError::Unreachable
        })
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

    /// The subject's compatibility level, falling back to the global config.
    /// Best-effort: returns `None` if the registry doesn't expose `/config`.
    pub async fn compatibility(&self, subject: &str) -> Option<String> {
        if let Ok(c) = self
            .get_json::<ConfigResponse>(&format!("/config/{subject}"))
            .await
        {
            return c.compatibility_level;
        }
        self.get_json::<ConfigResponse>("/config")
            .await
            .ok()
            .and_then(|c| c.compatibility_level)
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

/// How a record field should be deserialized in the event browser.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FieldFormat {
    /// Confluent-framed → Avro via the registry, otherwise UTF-8/hex.
    #[default]
    Auto,
    /// Force Confluent-Avro decode (raw with a note if not framed).
    Avro,
    /// Parse the bytes as plain JSON (no registry).
    Json,
    /// Bytes as UTF-8 or hex, never decoded.
    Raw,
}

impl FieldFormat {
    /// Parses a query value; unknown/empty → `Auto`.
    pub fn parse(s: Option<&str>) -> Self {
        match s.unwrap_or("auto") {
            "avro" => Self::Avro,
            "json" => Self::Json,
            "raw" => Self::Raw,
            _ => Self::Auto,
        }
    }
}

/// Decodes a record field (key or value) into a display value, according to the
/// chosen [`FieldFormat`]. Errors (no registry, schema fetch, decode) are
/// surfaced in the result so failures are diagnosable rather than silently
/// shown as hex.
pub async fn decode_field(
    registry: Option<&SchemaRegistry>,
    field: &Option<Bytes>,
    format: FieldFormat,
) -> Value {
    let Some(bytes) = field else {
        return Value::Null;
    };

    let framed = bytes.len() >= 5 && bytes[0] == 0x00;
    match format {
        FieldFormat::Raw => raw_field(bytes),
        FieldFormat::Json => json_field(bytes),
        FieldFormat::Avro if !framed => json!({ "kind": "raw", "data": raw_text(bytes),
            "error": "not Confluent-framed (no 0x00 magic byte)" }),
        FieldFormat::Avro => avro_field(registry, bytes).await,
        FieldFormat::Auto if framed => avro_field(registry, bytes).await,
        FieldFormat::Auto => raw_field(bytes),
    }
}

/// Decodes a Confluent-framed (`0x00` + 4-byte id) Avro payload via the registry.
async fn avro_field(registry: Option<&SchemaRegistry>, bytes: &Bytes) -> Value {
    let id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    let payload = &bytes[5..];

    let Some(registry) = registry else {
        return json!({ "kind": "hex", "schemaId": id, "data": hex(bytes),
            "error": "no schema registry configured (set KOTATSU_KORA_URL)" });
    };

    match registry.schema_by_id(id).await {
        Ok(cached) => match &cached.avro {
            Some(schema) => match apache_avro::from_avro_datum(schema, &mut &payload[..], None) {
                Ok(value) => {
                    json!({ "kind": "avro", "schemaId": id, "data": avro_to_json(&value) })
                }
                Err(e) => json!({ "kind": "avro", "schemaId": id, "data": hex(payload),
                    "error": format!("avro decode failed: {e}") }),
            },
            // Schema exists but isn't Avro (JSON Schema / Protobuf).
            None => json!({ "kind": cached.schema_type.to_lowercase(), "schemaId": id,
                "data": raw_text(payload) }),
        },
        Err(e) => json!({ "kind": "hex", "schemaId": id, "data": hex(bytes),
            "error": format!("schema id {id}: {e}") }),
    }
}

/// Parses the bytes as plain JSON; falls back to UTF-8/hex with a note.
fn json_field(bytes: &Bytes) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(text) => match serde_json::from_str::<Value>(text) {
            Ok(data) => json!({ "kind": "json", "data": data }),
            Err(e) => {
                json!({ "kind": "utf8", "data": text, "error": format!("not valid JSON: {e}") })
            }
        },
        Err(_) => json!({ "kind": "hex", "data": hex(bytes), "error": "not valid JSON (binary)" }),
    }
}

/// Converts a decoded Avro [`apache_avro::types::Value`] into JSON.
///
/// Unlike `apache_avro::from_value::<serde_json::Value>` (which errors on
/// `Decimal`/`Bytes`/`Fixed`), this handles every variant — binary as hex,
/// logical types as their underlying scalar, unions unwrapped.
fn avro_to_json(value: &apache_avro::types::Value) -> Value {
    use apache_avro::types::Value as A;
    match value {
        A::Null => Value::Null,
        A::Boolean(b) => json!(b),
        A::Int(i) | A::Date(i) | A::TimeMillis(i) => json!(i),
        A::Long(l)
        | A::TimeMicros(l)
        | A::TimestampMillis(l)
        | A::TimestampMicros(l)
        | A::TimestampNanos(l)
        | A::LocalTimestampMillis(l)
        | A::LocalTimestampMicros(l)
        | A::LocalTimestampNanos(l) => json!(l),
        A::Float(f) => json!(f),
        A::Double(f) => json!(f),
        A::Bytes(b) | A::Fixed(_, b) => json!(hex(b)),
        A::String(s) | A::Enum(_, s) => json!(s),
        A::Uuid(u) => json!(u.to_string()),
        A::Union(_, inner) => avro_to_json(inner),
        A::Array(items) => Value::Array(items.iter().map(avro_to_json).collect()),
        A::Map(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), avro_to_json(v)))
                .collect(),
        ),
        A::Record(fields) => Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_to_json(v)))
                .collect(),
        ),
        // Unscaled integer value (the decimal scale lives in the schema, not the value).
        A::Decimal(d) => match <Vec<u8>>::try_from(d) {
            Ok(be) => json!(twos_complement_to_string(&be)),
            Err(_) => Value::Null,
        },
        A::BigDecimal(bd) => json!(bd.to_string()),
        A::Duration(d) => json!({
            "months": u32::from(d.months()),
            "days": u32::from(d.days()),
            "millis": u32::from(d.millis()),
        }),
    }
}

/// Big-endian two's-complement bytes → decimal string (fits within i128, else hex).
fn twos_complement_to_string(be: &[u8]) -> String {
    if be.is_empty() {
        return "0".to_string();
    }
    if be.len() <= 16 {
        let mut v: i128 = if be[0] & 0x80 != 0 { -1 } else { 0 };
        for &byte in be {
            v = (v << 8) | i128::from(byte);
        }
        v.to_string()
    } else {
        format!("0x{}", hex(be))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{
        types::{Record, Value as AvroValue},
        Decimal, Schema,
    };

    // A record mixing the types that broke `from_value`: decimal, raw bytes,
    // fixed — alongside a logical timestamp, a union and an enum.
    const SCHEMA: &str = r#"
    {"type":"record","name":"Cdc","fields":[
      {"name":"id","type":"long"},
      {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
      {"name":"raw","type":"bytes"},
      {"name":"key","type":{"type":"fixed","name":"K","size":4}},
      {"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}},
      {"name":"opt","type":["null","string"]},
      {"name":"color","type":{"type":"enum","name":"Color","symbols":["RED","GREEN"]}}
    ]}"#;

    #[test]
    fn avro_to_json_handles_decimal_bytes_fixed_and_logical_types() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut rec = Record::new(&schema).unwrap();
        rec.put("id", 42i64);
        rec.put(
            "amount",
            AvroValue::Decimal(Decimal::from(vec![0x04, 0xd2])),
        ); // unscaled 1234
        rec.put("raw", AvroValue::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
        rec.put("key", AvroValue::Fixed(4, vec![1, 2, 3, 4]));
        rec.put("ts", AvroValue::TimestampMicros(1_700_000_000_000_000));
        rec.put(
            "opt",
            AvroValue::Union(1, Box::new(AvroValue::String("x".into()))),
        );
        rec.put("color", AvroValue::Enum(1, "GREEN".into()));
        let datum = apache_avro::to_avro_datum(&schema, rec).unwrap();

        let value = apache_avro::from_avro_datum(&schema, &mut &datum[..], None).unwrap();
        let j = avro_to_json(&value);

        assert_eq!(j["id"], 42);
        assert_eq!(j["amount"], "1234"); // unscaled decimal
        assert_eq!(j["raw"], "deadbeef"); // bytes as hex
        assert_eq!(j["key"], "01020304"); // fixed as hex
        assert_eq!(j["ts"], 1_700_000_000_000_000i64);
        assert_eq!(j["opt"], "x"); // union unwrapped
        assert_eq!(j["color"], "GREEN"); // enum symbol
    }

    #[test]
    fn twos_complement_handles_sign() {
        assert_eq!(twos_complement_to_string(&[0x04, 0xd2]), "1234");
        assert_eq!(twos_complement_to_string(&[0xff]), "-1");
        assert_eq!(twos_complement_to_string(&[]), "0");
    }

    #[test]
    fn field_format_parses() {
        assert_eq!(FieldFormat::parse(Some("avro")), FieldFormat::Avro);
        assert_eq!(FieldFormat::parse(Some("json")), FieldFormat::Json);
        assert_eq!(FieldFormat::parse(Some("raw")), FieldFormat::Raw);
        assert_eq!(FieldFormat::parse(Some("nonsense")), FieldFormat::Auto);
        assert_eq!(FieldFormat::parse(None), FieldFormat::Auto);
    }

    #[test]
    fn json_field_parses_and_falls_back() {
        let ok = json_field(&Bytes::from_static(br#"{"a":1}"#));
        assert_eq!(ok["kind"], "json");
        assert_eq!(ok["data"]["a"], 1);

        let bad = json_field(&Bytes::from_static(b"not json"));
        assert_eq!(bad["kind"], "utf8");
        assert!(bad.get("error").is_some());
    }
}
