# Test Case: SCH-001

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | SCH-001 |
| **Test Case Title** | Decode Confluent-Framed Avro Records and List the Schema Subject |
| **Test Type** | Functional, Integration, End-to-End |
| **Priority** | Critical |
| **Estimated Duration** | 5-7 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that Confluent-framed Avro records produced to Tansu are decoded by
Kotatsu against the Kora registry — the message value is returned as a structured
object with `kind: "avro"` and the resolving `schemaId` — and that the subject is
discoverable via the schemas API.

## Requirements Traceability

- **User Story**: As a user, I want Avro messages decoded into readable objects so that I can inspect their fields without a schema tool.
- **Requirement ID**: SCH-REQ-001 (Avro decode via registry)
- **Business Rule**: A Confluent-framed value (magic byte + schema id) is resolved against Kora; the decoded value carries `kind: "avro"` and the `schemaId` used; the subject appears under `/api/schemas`.

## Preconditions

1. **System State**: Stack up; source connected; Kora reachable (`KOTATSU_KORA_URL=http://kora:8080`).
2. **Test Data**: Avro schema `Order {int id, string item}`; records `{"id":1,"item":"widget"}`, `{"id":2,"item":"gadget"}`.
3. **Environment**: Base URL `http://localhost:8080`; `curl`; Avro producer image `confluentinc/cp-schema-registry:7.6.0`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | **Create the topic first** | `kafka-topics.sh --create --topic avro-orders --partitions 1 ...` | `Created topic avro-orders.` |
| 2 | Produce 2 Avro records | `kafka-avro-console-producer` with `schema.registry.url=http://kora:8080` | Producer exits without a metadata timeout |
| 3 | List schema subjects | `GET /api/schemas` | `items` contains `avro-orders-value`; `registry: "http://kora:8080"` |
| 4 | Get subject detail | `GET /api/schemas/avro-orders-value` | `schemaType: "AVRO"`, `version: 1`, schema text is the `Order` record |
| 5 | Read messages (auto decode) | `GET .../topics/avro-orders/messages?partition=0&offset=earliest` | `count: 2`; each value `kind: "avro"`, `schemaId: 1`, `data: {id, item}` |
| 6 | Verify field values | inspect records | offset 0 → `{id:1, item:"widget"}`; offset 1 → `{id:2, item:"gadget"}` |
| 7 | UI decode | open `avro-orders` in the event browser | Values render as structured objects; **Schemas** view lists `avro-orders-value` |

## Expected Results

### Primary Verification Points

1. Decoded value is `{"kind": "avro", "schemaId": 1, "data": { "id": ..., "item": ... }}`.
2. Field values match what was produced.
3. `/api/schemas` lists `avro-orders-value`; the subject detail returns the correct Avro schema, version 1.

### Secondary Verification Points

4. The UI event browser shows decoded objects, and the Schemas view lists the subject.
5. Registry base URL reported as `http://kora:8080`.

## Test Data

```json
{
  "topic": "avro-orders",
  "schema": { "type": "record", "name": "Order", "fields": [ { "name": "id", "type": "int" }, { "name": "item", "type": "string" } ] },
  "records": [ { "id": 1, "item": "widget" }, { "id": 2, "item": "gadget" } ],
  "expected": {
    "subject": "avro-orders-value",
    "value_kind": "avro",
    "schemaId": 1,
    "decoded": [ { "id": 1, "item": "widget" }, { "id": 2, "item": "gadget" } ]
  }
}
```

### Reference commands

```bash
# 1) create the topic FIRST (Tansu does not auto-create topics)
docker run --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 \
  --create --topic avro-orders --partitions 1 --replication-factor 1

# 2) produce Confluent-framed Avro (schema auto-registered in Kora)
printf '{"id":1,"item":"widget"}\n{"id":2,"item":"gadget"}\n' | \
  docker run -i --rm --network kotatsu_default confluentinc/cp-schema-registry:7.6.0 \
  kafka-avro-console-producer --bootstrap-server tansu:9092 --topic avro-orders \
  --property schema.registry.url=http://kora:8080 \
  --property value.schema='{"type":"record","name":"Order","fields":[{"name":"id","type":"int"},{"name":"item","type":"string"}]}'

# 3) verify
curl -s http://localhost:8080/api/schemas
curl -s http://localhost:8080/api/schemas/avro-orders-value
curl -s "http://localhost:8080/api/clusters/demo/topics/avro-orders/messages?partition=0&offset=earliest"
```

## Post-conditions

1. Topic `avro-orders` holds 2 decoded-Avro records; subject `avro-orders-value` registered in Kora.

## Cleanup Steps

1. None, or `docker compose down -v` (also clears the Kora Postgres volume).

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Producer times out ("topic not present in metadata") | High | High | **Create the topic first** — Tansu does not auto-create topics |
| Kora unreachable / marked unhealthy | Medium | High | Confirm `GET /api/schemas` returns 200 and the registry URL before producing |
| Schema id differs from 1 | Medium | Low | Assert `kind: "avro"` and matching `data`; treat exact `schemaId` as informative on a fresh registry |

## Dependencies

- Kora + its Postgres running; `KOTATSU_KORA_URL` set to `http://kora:8080`.
- `confluentinc/cp-schema-registry:7.6.0` image for the Avro producer.

## Notes

- **Key learning:** the Avro producer fails with a metadata timeout if the topic does not already exist — always create `avro-orders` before producing. On a fresh Kora, the first registered schema gets `schemaId: 1`.
