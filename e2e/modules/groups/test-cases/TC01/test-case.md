# Test Case: GRP-001

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | GRP-001 |
| **Test Case Title** | List Consumer Groups With State and Member Count |
| **Test Type** | Functional, Integration |
| **Priority** | High |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that a consumer group that has committed offsets to Tansu is listed by
Kotatsu with its name, state and member count, read directly from S3.

## Requirements Traceability

- **User Story**: As a user, I want to see the consumer groups on a cluster so that I can understand who is consuming.
- **Requirement ID**: GRP-REQ-001 (Group enumeration)
- **Business Rule**: A group with committed offsets appears in the listing; when no members are actively connected its state is `Empty` and `members: 0`.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: a consumer group `qa-group` that consumed topic `orders` from the beginning and committed offsets, then disconnected.
3. **Environment**: Base URL `http://localhost:8080`; `curl`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Ensure `orders` has records | run MSG-001 | 3 records present |
| 2 | Consume with a group id | `kafka-console-consumer.sh --topic orders --from-beginning --group qa-group --max-messages 3 --timeout-ms 8000` | 3 messages consumed, offsets committed |
| 3 | List groups | `GET /api/clusters/demo/groups` | HTTP 200; `total: 1`; `items` contains `qa-group` |
| 4 | Verify group summary | inspect item | `name: "qa-group"`, `state: "Empty"` (consumer disconnected), `members: 0` |
| 5 | Cluster stats reflect group | `GET /api/clusters/demo` | Cluster summary returns without error |
| 6 | UI groups view | open the Consumer Groups view | `qa-group` listed with its state |

## Expected Results

### Primary Verification Points

1. `qa-group` appears in the listing with `members` and `state` fields.
2. After the consumer disconnects, `state: "Empty"`, `members: 0`.
3. HTTP 200 with a pagination envelope (`limit`, `offset`, `total`).

### Secondary Verification Points

4. UI Consumer Groups view lists the group consistently with the API.

## Test Data

```json
{
  "group": "qa-group",
  "consumed_topic": "orders",
  "expected": { "total": 1, "item": { "name": "qa-group", "state": "Empty", "members": 0 } }
}
```

### Reference commands

```bash
docker run --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server tansu:9092 \
  --topic orders --from-beginning --group qa-group --max-messages 3 --timeout-ms 8000

curl -s http://localhost:8080/api/clusters/demo/groups
```

## Post-conditions

1. Group `qa-group` exists with committed offsets on `orders`.

## Cleanup Steps

1. None, or `docker compose down -v` for a clean slate.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Consumer still connected → state not `Empty` | Medium | Low | Let the consumer exit (`--max-messages` / `--timeout-ms`) before listing |
| No offsets committed | Low | Medium | Ensure the consumer actually read and committed (auto-commit on clean exit) |

## Dependencies

- Topic `orders` populated; a Kafka consumer able to commit to group `qa-group`.

## Notes

- The demo console consumer disconnects after `--max-messages`, so the group settles to `Empty` with `members: 0` while retaining its committed offsets (see GRP-002).
