# Test Case: MSG-005

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-005 |
| **Test Case Title** | Empty Topic Returns No Records Gracefully |
| **Test Type** | Functional, Negative, Boundary |
| **Priority** | Medium |
| **Estimated Duration** | 2-3 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that reading messages from a topic that exists but has no records returns
an empty result set with zeroed watermarks and `exhausted: true` — never an error
or a hang.

## Requirements Traceability

- **User Story**: As a user, I want an empty topic to show cleanly as empty so that I know there is simply no data.
- **Requirement ID**: MSG-REQ-005 (Empty topic read)
- **Business Rule**: An existing topic with no records returns `count: 0`, `records: []`, `watermark: {low:0, high:0}`, `exhausted: true`.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topic `empty-topic` — created with 1 partition, no records produced.
3. **Environment**: Base URL `http://localhost:8080`; `curl`; browser.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Create `empty-topic` | `kafka-topics.sh --create --topic empty-topic --partitions 1 ...` | Topic created, no records |
| 2 | List topics | `GET /api/clusters/demo/topics` | `empty-topic` present with `messages: 0`, `storage_bytes: 0` |
| 3 | Read messages | `GET .../topics/empty-topic/messages` | `count: 0`, `records: []`, `watermark: {low:0, high:0}`, `exhausted: true`, `filtered: false` |
| 4 | Read with earliest | `?offset=earliest` | Same empty result |
| 5 | UI view | open `empty-topic` in the event browser | Empty-state message shown; no error, no spinner hang |

## Expected Results

### Primary Verification Points

1. `count: 0` and `records: []`.
2. `watermark.low == watermark.high == 0`.
3. `exhausted: true`, `filtered: false`.
4. HTTP 200 (no error, no timeout).

### Secondary Verification Points

5. UI shows a clean empty state rather than an error or infinite loader.

## Test Data

```json
{
  "topic": "empty-topic",
  "expected": {
    "count": 0,
    "records": [],
    "watermark": { "low": 0, "high": 0 },
    "exhausted": true,
    "filtered": false
  }
}
```

## Post-conditions

1. Topic remains empty.

## Cleanup Steps

1. None, or delete `empty-topic`.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Reading a nonexistent topic vs an empty one | Medium | Medium | Distinguish: nonexistent → `{"error":"topic '...' not found"}`; empty → `count: 0`. This case is the empty one |

## Dependencies

- Topic `empty-topic` created but not populated.

## Notes

- Contrast with a nonexistent topic, which returns `{"error":"topic '<name>' not found"}`. An existing-but-empty topic must return the zeroed, non-error payload above.
