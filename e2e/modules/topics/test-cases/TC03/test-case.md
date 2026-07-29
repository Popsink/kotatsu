# Test Case: TOP-003

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | TOP-003 |
| **Test Case Title** | Read Messages From a Specific Partition of a Multi-Partition Topic |
| **Test Type** | Functional, Integration |
| **Priority** | High |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that the `partition` query parameter on the messages endpoint scopes the
read to a single partition, returning that partition's records and watermarks,
and that an empty partition returns an empty result (not an error).

## Requirements Traceability

- **User Story**: As a user, I want to browse a single partition of a topic so that I can inspect its records independently.
- **Requirement ID**: TOP-REQ-003 (Per-partition read)
- **Business Rule**: `?partition=n` returns only partition `n`; each returned record carries `partition: n`; the `watermark` is that partition's low/high.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topic `events` — 3 partitions; 6 records in partition 0; partitions 1 and 2 empty.
3. **Environment**: Base URL `http://localhost:8080`; `curl`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Read partition 0 | `GET .../topics/events/messages?partition=0&offset=earliest` | `partition: 0`, `count: 6`, `watermark: {low:0, high:6}`, all records `partition: 0` |
| 2 | Read partition 1 | `GET .../topics/events/messages?partition=1&offset=earliest` | `partition: 1`, `count: 0`, `records: []`, `watermark: {low:0, high:0}` |
| 3 | Read partition 2 | `GET .../topics/events/messages?partition=2&offset=earliest` | `partition: 2`, `count: 0`, `records: []` |
| 4 | Default partition | `GET .../topics/events/messages?offset=earliest` (no `partition`) | Defaults to partition 0 |
| 5 | UI partition selector | open `events` in UI, switch partitions | Records shown match the API per partition |

## Expected Results

### Primary Verification Points

1. `?partition=n` returns only records for partition `n`, each with `partition: n`.
2. The `watermark` in the response is the selected partition's low/high.
3. An empty partition returns `count: 0`, `records: []`, `exhausted: true` — never an error.

### Secondary Verification Points

4. Omitting `partition` defaults to partition 0.
5. UI partition switcher matches API output.

## Test Data

```json
{
  "topic": "events",
  "reads": [
    { "partition": 0, "expect": { "count": 6, "watermark": { "low": 0, "high": 6 } } },
    { "partition": 1, "expect": { "count": 0, "watermark": { "low": 0, "high": 0 } } },
    { "partition": 2, "expect": { "count": 0, "watermark": { "low": 0, "high": 0 } } }
  ]
}
```

## Post-conditions

1. Topic unchanged (read-only).

## Cleanup Steps

1. None.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Out-of-range partition index | Low | Low | Optionally test `partition=99`; record actual behavior (empty vs. error) |
| Record spread differs | Medium | Low | Assert each record's `partition` field equals the requested one |

## Dependencies

- Topic `events` with 3 partitions produced.

## Notes

- Default `partition` is `0` when the parameter is omitted (server default). Default `offset` is `latest`; use `earliest` to read from the start.
