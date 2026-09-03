# Test Case: MSG-001

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-001 |
| **Test Case Title** | Read Back Produced Records From a Single-Partition Topic |
| **Test Type** | Functional, Integration, End-to-End |
| **Priority** | Critical |
| **Estimated Duration** | 3-5 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that records produced to a Tansu topic are persisted to S3 and read back
faithfully by Kotatsu — with correct key, value, offset, partition and watermark
values — via both the API and the event browser UI, without any Kafka client on
the read path.

## Requirements Traceability

- **User Story**: As a user, I want to browse the messages of a topic so that I can inspect what Tansu persisted to S3 without connecting a Kafka consumer.
- **Requirement ID**: MSG-REQ-001 (Message read-back)
- **Business Rule**: Records returned by Kotatsu must match, byte-for-byte in key/value, what was produced; offsets are contiguous from the low watermark; `high` watermark equals the number of produced records for a fresh topic.

## Preconditions

1. **System State**:
   - Docker stack is up (`docker compose ps` shows all services `Up`).
   - Kotatsu UI/API reachable at `http://localhost:8080`.
   - `GET /api/source/status` returns `connected: true` (may require at least one topic to exist).
   - MinIO reachable; `tansu` bucket exists.
   - Tansu broker reachable at `localhost:9092`, cluster `demo`.

2. **Test Data** (fictitious):
   - Topic name: `orders`
   - Partitions: 1
   - Records (key:value):
     - `key-1` → `{"id":1,"item":"widget"}`
     - `key-2` → `{"id":2,"item":"gadget"}`
     - `key-3` → `{"id":3,"item":"gizmo"}`

3. **Environment**:
   - Browser: Chrome/Firefox
   - Base URL: `http://localhost:8080`
   - Kafka client image available: `apache/kafka:latest`
   - Docker network: `kotatsu_default`

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Create the topic | `kafka-topics.sh --create --topic orders --partitions 1 --replication-factor 1` | `Created topic orders.` |
| 2 | Produce the 3 keyed records | see Test Data | Producer exits without error |
| 3 | Verify source is connected | `GET /api/source/status` | `connected: true` (cluster id comes from `GET /api/source`) |
| 4 | Verify cluster discovery | `GET /api/clusters` | Response `clusters` contains `"demo"` |
| 5 | Verify topic listing | `GET /api/clusters/demo/topics` | `items` contains `orders` with `messages: 3`, `partitions: 1`, `storage_bytes > 0` |
| 6 | Read messages via API | `GET /api/clusters/demo/topics/orders/messages` | `count: 3`, `records` array of 3, `watermark: {low:0, high:3}`, `exhausted: true` |
| 7 | Assert record fidelity | inspect `records` | Offsets 0,1,2; keys `key-1..3` (`kind:"utf8"` — a bare key is text, not JSON); values `kind:"json"` with `data` the parsed object (#103) |
| 8 | Open the UI | navigate to `http://localhost:8080` | Kotatsu loads; cluster `demo` visible |
| 9 | Open topic in UI | click cluster `demo` → topic `orders` | Topic detail shows 1 partition, 3 messages, low/high offsets 0/3 |
| 10 | Browse messages in UI | open the messages/events view for `orders` | The 3 records render with their keys, values and offsets matching step 7 |

## Expected Results

### Primary Verification Points

1. **Read-back fidelity**:
   - Exactly 3 records returned, offsets `0, 1, 2` (contiguous, ascending).
   - Each key decoded as UTF-8: `key-1`, `key-2`, `key-3`.
   - Each value equals the produced JSON string, unaltered.
   - Each record carries `partition: 0` and a non-zero `timestamp`.

2. **Watermarks & counts**:
   - `watermark.low = 0`, `watermark.high = 3`.
   - Topic listing reports `messages: 3`, `partitions: 1`, `storage_bytes > 0`.
   - `exhausted: true` (no further pages for this small topic).

3. **UI ↔ API consistency**:
   - Values shown in the event browser match the API response exactly.
   - No console errors; the view renders without a page refresh.

### Secondary Verification Points

4. **Source status**:
   - `GET /api/source` reports the correct `bucket` (`tansu`), `cluster` (`demo`) and `endpoint`; `GET /api/source/status` reports `connected: true`.

5. **No Kafka on read path**:
   - Data is served even if the Tansu broker is stopped after production (optional check) — reads come from S3 only.

## Test Data

```json
{
  "cluster": "demo",
  "topic": "orders",
  "partitions": 1,
  "records": [
    { "key": "key-1", "value": { "id": 1, "item": "widget" } },
    { "key": "key-2", "value": { "id": 2, "item": "gadget" } },
    { "key": "key-3", "value": { "id": 3, "item": "gizmo" } }
  ],
  "expected": {
    "count": 3,
    "offsets": [0, 1, 2],
    "watermark": { "low": 0, "high": 3 },
    "exhausted": true
  }
}
```

### Reference commands

```bash
# Step 1 — create topic
docker run --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 \
  --create --topic orders --partitions 1 --replication-factor 1

# Step 2 — produce records
printf 'key-1:{"id":1,"item":"widget"}\nkey-2:{"id":2,"item":"gadget"}\nkey-3:{"id":3,"item":"gizmo"}\n' | \
  docker run -i --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server tansu:9092 \
  --topic orders --property parse.key=true --property key.separator=:

# Steps 3-6 — verify via API
curl -s http://localhost:8080/api/source
curl -s http://localhost:8080/api/source/status
curl -s http://localhost:8080/api/clusters
curl -s http://localhost:8080/api/clusters/demo/topics
curl -s http://localhost:8080/api/clusters/demo/topics/orders/messages
```

## Post-conditions

1. Topic `orders` exists in cluster `demo` with 3 records at offsets 0-2.
2. Records remain readable via API and UI.
3. Objects are present in the `tansu` bucket under `clusters/demo/topics/orders/…`.

## Cleanup Steps

1. Delete the topic (optional): `kafka-topics.sh --delete --topic orders`.
2. For a full clean slate across cases: `docker compose down -v` (removes MinIO/Kora volumes).
3. No cleanup required if the topic is reused by subsequent MSG cases.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Source shows `connected: false` on empty bucket | Medium | Low | Produce at least one record first; re-check `/api/source/status` |
| Producer image pull slow/offline | Low | Medium | Pre-pull `apache/kafka:latest` before test |
| Timestamps differ run-to-run | High | None | Do not assert exact timestamp; assert non-zero only |
| S3 write lag after produce | Low | Medium | Retry the read after a few seconds |

## Dependencies

- Docker stack running (`app`, `tansu`, `minio`, `createbucket`, `kora`, `kora-db`).
- `apache/kafka:latest` image available for producing data.
- Network `kotatsu_default` created by compose.

## Notes

- This case is the canonical happy-path read-back and a prerequisite baseline for other MSG cases (pagination, null keys, binary values).
- All data is fictitious; do not substitute real topic or business names.
- The read path uses `object_store` + `tansu-sans-io` only — no Kafka client is involved when Kotatsu serves the records.
