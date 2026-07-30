# Test Case: GRP-002

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | GRP-002 |
| **Test Case Title** | Group Detail Reports Committed Offsets, High Watermark and Lag |
| **Test Type** | Functional, Integration |
| **Priority** | High |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that the group detail endpoint returns per-topic-partition committed
offset, high watermark and lag, and a correct `total_lag`, computed from data
read out of S3.

## Requirements Traceability

- **User Story**: As a user, I want to see a group's committed offsets and lag per partition so that I can tell how far behind it is.
- **Requirement ID**: GRP-REQ-002 (Group offsets & lag)
- **Business Rule**: `lag == high_watermark - committed_offset` per partition; `total_lag` is the sum of per-partition lags; a fully caught-up group has `total_lag: 0`.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: group `qa-group` fully consumed `orders` (3 records) → committed offset 3, high watermark 3, lag 0 (from GRP-001).
3. **Environment**: Base URL `http://localhost:8080`; `curl`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Get group detail | `GET /api/clusters/demo/groups/qa-group` | HTTP 200 |
| 2 | Verify identity | inspect body | `name: "qa-group"`, `state: "Empty"`, `protocol_type: "consumer"` |
| 3 | Verify offsets entry | inspect `offsets[]` | entry for `topic: "orders"`, `partition: 0`, `committed_offset: 3`, `high_watermark: 3`, `lag: 0` |
| 4 | Verify total lag | inspect body | `total_lag: 0` |
| 5 | Produce lag (optional) | produce 2 more records to `orders`, re-GET detail | `high_watermark: 5`, `committed_offset: 3`, `lag: 2`, `total_lag: 2` |
| 6 | UI group detail | open `qa-group` in the UI | Per-partition offsets and lag match the API |

## Expected Results

### Primary Verification Points

1. `offsets[]` contains a per-topic-partition entry with `committed_offset`, `high_watermark`, `lag`.
2. `lag == high_watermark - committed_offset` for each entry.
3. `total_lag` equals the sum of per-partition lags (0 when caught up).

### Secondary Verification Points

4. Producing more records without consuming increases `lag` and `high_watermark` accordingly (optional step 5).
5. UI figures match the API.

## Test Data

```json
{
  "group": "qa-group",
  "expected_caught_up": {
    "name": "qa-group",
    "state": "Empty",
    "protocol_type": "consumer",
    "offsets": [ { "topic": "orders", "partition": 0, "committed_offset": 3, "high_watermark": 3, "lag": 0 } ],
    "total_lag": 0
  },
  "expected_after_lag": {
    "offsets": [ { "topic": "orders", "partition": 0, "committed_offset": 3, "high_watermark": 5, "lag": 2 } ],
    "total_lag": 2
  }
}
```

## Post-conditions

1. If step 5 was run, `orders` now has 5 records and `qa-group` shows lag 2 until it consumes again.

## Cleanup Steps

1. If lag was introduced and a caught-up state is needed for other cases, re-consume with `qa-group`, or `docker compose down -v`.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Group state changes if a consumer reconnects | Medium | Low | Do not run a live consumer during the read |
| Optional lag step left uncleaned | Medium | Medium | Note the changed record count; reset via cleanup if needed |

## Dependencies

- Group `qa-group` with committed offsets on `orders` (GRP-001).

## Notes

- Observed caught-up detail: `committed_offset: 3`, `high_watermark: 3`, `lag: 0`, `total_lag: 0`, `generation_id: 1`, `protocol_name: "range"`. Lag is the primary QA signal for consumer health.
