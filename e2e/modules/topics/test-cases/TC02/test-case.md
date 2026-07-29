# Test Case: TOP-002

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | TOP-002 |
| **Test Case Title** | Topic Detail Reports Per-Partition Watermarks and Storage |
| **Test Type** | Functional, Integration |
| **Priority** | High |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that the topic detail endpoint returns per-partition low/high watermarks,
per-partition message counts and storage sizes, plus topic-level aggregates
(total messages, storage, replication factor).

## Requirements Traceability

- **User Story**: As a user, I want per-partition detail for a topic so that I can see how data is distributed and where offsets stand.
- **Requirement ID**: TOP-REQ-002 (Topic detail)
- **Business Rule**: For each partition, `messages == high - low`; topic-level `messages`/`storage_bytes` equal the sum across partitions.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topic `events` — 3 partitions, 6 messages produced without keys (all landed in partition 0).
3. **Environment**: Base URL `http://localhost:8080`; `curl`; browser.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Produce `events` (3 partitions, 6 keyless records) | see MSG-003 / README | Topic exists |
| 2 | Get topic detail | `GET /api/clusters/demo/topics/events` | HTTP 200 |
| 3 | Verify topic aggregates | inspect body | `messages: 6`, `storage_bytes > 0`, `replication_factor: 1`, `partitions` array length 3 |
| 4 | Verify partition 0 | inspect `partitions[0]` | `partition: 0`, `low: 0`, `high: 6`, `messages: 6`, `storage_bytes > 0` |
| 5 | Verify partitions 1 and 2 | inspect entries | `low: 0`, `high: 0`, `messages: 0`, `storage_bytes: 0` |
| 6 | Cross-check aggregate | sum partition messages | equals topic-level `messages` (6) |
| 7 | Open UI topic detail | navigate to `events` | Per-partition offsets/counts match the API |

## Expected Results

### Primary Verification Points

1. `partitions` contains one entry per partition (3), each with `partition`, `low`, `high`, `messages`, `storage_bytes`.
2. For each partition, `messages == high - low`.
3. Topic-level `messages` and `storage_bytes` equal the sum across partitions.

### Secondary Verification Points

4. `replication_factor` reflects the topic (1 in the demo stack).
5. UI per-partition figures match the API.

## Test Data

```json
{
  "topic": "events",
  "expected": {
    "messages": 6,
    "replication_factor": 1,
    "partitions": [
      { "partition": 0, "low": 0, "high": 6, "messages": 6 },
      { "partition": 1, "low": 0, "high": 0, "messages": 0 },
      { "partition": 2, "low": 0, "high": 0, "messages": 0 }
    ]
  }
}
```

## Post-conditions

1. Topic unchanged (read-only).

## Cleanup Steps

1. None.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Records distributed differently across partitions | Medium | Low | Assert the invariant `messages == high - low` per partition rather than a fixed distribution |
| `configs` empty for Tansu topics | High | None | Do not assert on `configs`; it may be an empty array |

## Dependencies

- Topic `events` produced with 3 partitions.

## Notes

- With the console producer, keyless records in one batch all landed in partition 0; the reliable invariant to assert is `messages == high - low` per partition and the topic-level sum, not a specific spread.
