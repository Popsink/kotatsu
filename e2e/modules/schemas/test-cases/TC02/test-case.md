# Test Case: SCH-002

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | SCH-002 |
| **Test Case Title** | Retrieve a Specific Schema Subject Version |
| **Test Type** | Functional, Integration |
| **Priority** | Medium |
| **Estimated Duration** | 2-3 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-29 |
| **Last Modified** | 2026-07-29 |

## Test Objective

Verify that the schema subject and its versions can be retrieved from the Kora
registry through Kotatsu — the subject detail lists available versions and
compatibility, and the version endpoint returns the exact schema text, id and
type for a given version.

## Requirements Traceability

- **User Story**: As a user, I want to inspect a schema and its versions so that I understand how a topic's records are structured.
- **Requirement ID**: SCH-REQ-002 (Schema version retrieval)
- **Business Rule**: `GET /api/schemas/{subject}` returns `versions[]`, `latest` and `compatibility`; `GET /api/schemas/{subject}/versions/{version}` returns that version's `id`, `schemaType`, `schema` and `version`.

## Preconditions

1. **System State**: Stack up; Kora reachable; subject `avro-orders-value` registered (see SCH-001).
2. **Test Data**: subject `avro-orders-value`, version `1`, `Order {int id, string item}`.
3. **Environment**: Base URL `http://localhost:8080`; `curl`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Get subject detail | `GET /api/schemas/avro-orders-value` | HTTP 200; `subject: "avro-orders-value"`, `versions: [1]`, `compatibility: "BACKWARD"`, `latest.version: 1` |
| 2 | Verify latest schema | inspect `latest` | `schemaType: "AVRO"`, `id: 1`, schema is the `Order` record |
| 3 | Get version 1 | `GET /api/schemas/avro-orders-value/versions/1` | HTTP 200; `version: 1`, `id: 1`, `schemaType: "AVRO"`, schema text matches |
| 4 | Get a nonexistent version | `GET .../versions/99` | Error / not-found (no 5xx) |
| 5 | UI schema view | open the subject in the Schemas view | Version(s) and schema text render correctly |

## Expected Results

### Primary Verification Points

1. Subject detail lists `versions`, `latest` and `compatibility`.
2. The version endpoint returns the exact `schema`, `id`, `schemaType`, `version`.
3. A nonexistent version returns a clean error, not a 5xx.

### Secondary Verification Points

4. Schema text is byte-equivalent to what was registered by the producer.
5. UI renders the schema and its version list.

## Test Data

```json
{
  "subject": "avro-orders-value",
  "expected_detail": { "versions": [1], "compatibility": "BACKWARD", "latest": { "version": 1, "id": 1, "schemaType": "AVRO" } },
  "expected_v1": {
    "id": 1,
    "version": 1,
    "schemaType": "AVRO",
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"item\",\"type\":\"string\"}]}"
  }
}
```

## Post-conditions

1. No change (read-only against the registry).

## Cleanup Steps

1. None.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Compatibility default differs | Low | Low | Assert the field exists; treat `BACKWARD` as the observed default |
| Subject not yet registered | Medium | High | Run SCH-001 first so the subject exists |

## Dependencies

- SCH-001 executed so `avro-orders-value` exists in Kora.

## Notes

- Observed defaults on a fresh Kora: single version `1`, schema id `1`, compatibility `BACKWARD`.
