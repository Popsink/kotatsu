# Test Case: MSG-003

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-003 |
| **Test Case Title** | Keyless Records Are Read Back With a Null Key |
| **Test Type** | Functional, Integration, Boundary |
| **Priority** | Medium |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that records produced without a key are read back with `key: null` (not an
empty string or an error), while their values decode normally, and that the UI
renders a null key without breaking.

## Requirements Traceability

- **User Story**: As a user, I want keyless messages to display correctly so that topics without keys are still browsable.
- **Requirement ID**: MSG-REQ-003 (Null key handling)
- **Business Rule**: A record with no key serializes to `key: null`; the value is decoded independently of the key.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topic `events` — 6 keyless JSON records `{"n":1}`..`{"n":6}` in partition 0.
3. **Environment**: Base URL `http://localhost:8080`; `curl`; browser.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Create `events` (3 partitions) | `kafka-topics.sh --create --topic events --partitions 3 ...` | Topic created |
| 2 | Produce 6 keyless records | `printf '{"n":1}\n...{"n":6}\n' \| kafka-console-producer.sh --topic events` (no `parse.key`) | Producer exits OK |
| 3 | Read partition 0 | `?partition=0&offset=earliest` | `count: 6` |
| 4 | Verify keys | inspect each record | `key: null` for every record |
| 5 | Verify values | inspect each record | `value.kind: "utf8"`, data equals `{"n":k}` |
| 6 | UI render | open `events` in the event browser | Records show an empty/`null` key indicator, values visible, no error |

## Expected Results

### Primary Verification Points

1. Every keyless record returns `key: null` (JSON null), not `""` and not an error.
2. Values decode independently and correctly.
3. Offsets are contiguous 0..5 in partition 0.

### Secondary Verification Points

4. The event browser renders null keys without a crash or blank row.

## Test Data

```json
{
  "topic": "events",
  "partition": 0,
  "records": [{ "n": 1 }, { "n": 2 }, { "n": 3 }, { "n": 4 }, { "n": 5 }, { "n": 6 }],
  "expected": { "count": 6, "key": null, "value": { "kind": "utf8" } }
}
```

### Reference commands

```bash
docker run --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 \
  --create --topic events --partitions 3 --replication-factor 1

printf '{"n":1}\n{"n":2}\n{"n":3}\n{"n":4}\n{"n":5}\n{"n":6}\n' | \
  docker run -i --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server tansu:9092 --topic events
```

## Post-conditions

1. Topic `events` exists with 6 keyless records in partition 0.

## Cleanup Steps

1. None, or delete `events`.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Producer accidentally adds a key | Low | Medium | Do not pass `parse.key=true` for this case |
| Records spread across partitions | Medium | Low | Read the partition that holds them, or aggregate; assert `key: null` regardless |

## Dependencies

- Topic `events` produced without keys.

## Notes

- Observed behaviour: all 6 keyless records from one producer batch landed in partition 0; partitions 1 and 2 stayed empty.
