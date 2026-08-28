# Test Case: MSG-008

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-008 |
| **Test Case Title** | Seek to Records by Timestamp (`offset=timestamp:<ms>`) |
| **Test Type** | Functional, Integration, Boundary |
| **Priority** | Medium |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-29 |
| **Last Modified** | 2026-07-29 |

## Test Objective

Verify that `offset=timestamp:<ms>` starts the read at the record batch whose
timestamp range covers the requested time, returning records from that batch's
base offset onward.

## Requirements Traceability

- **User Story**: As a user, I want to jump to messages around a point in time so that I can investigate events near an incident.
- **Requirement ID**: MSG-REQ-008 (Timestamp seek)
- **Business Rule**: `timestamp:<ms>` resolves to the first record batch whose timestamp range includes/exceeds `<ms>`; the read returns records from that batch's base offset. Seeking is **batch-granular**, not record-granular.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topic `orders` — 3 records; note their timestamps from an `earliest` read.
3. **Environment**: Base URL `http://localhost:8080`; `curl`; `python3` (to extract a timestamp).

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Read earliest, capture timestamps | `?offset=earliest` | Record timestamps `T0 <= T1 <= T2` captured |
| 2 | Seek to `T0` | `?offset=timestamp:T0` | Returns records from offset 0 |
| 3 | Seek to a time after all records | `?offset=timestamp:<T2+100000>` | Empty or tail result (no records after the last) |
| 4 | Seek to `T1` (mid-batch) | `?offset=timestamp:T1` | Returns from the **batch base offset** (offset 0 when all records share one batch), NOT necessarily offset 1 |
| 5 | UI time seek | if the UI exposes a time picker, seek to `T1` | Behaviour consistent with the API |

## Expected Results

### Primary Verification Points

1. Seeking to the earliest timestamp returns records from offset 0.
2. Seeking beyond the last timestamp returns no newer records.
3. Seeking to a timestamp inside a batch returns from the batch base offset (batch-granular), which may include records slightly earlier than the exact timestamp.

### Secondary Verification Points

4. HTTP 200 for all valid timestamp seeks.
5. UI time-based navigation matches the API.

## Test Data

```json
{
  "topic": "orders",
  "note": "In the demo, all 3 records were produced in a single batch, so timestamp:<any covered ms> returns from offset 0.",
  "cases": [
    { "seek": "timestamp:T0", "expect_from_offset": 0 },
    { "seek": "timestamp:T1", "expect_from_offset": 0, "reason": "batch-granular seek" },
    { "seek": "timestamp:(T2+100000)", "expect": "empty / tail" }
  ]
}
```

### Reference commands

```bash
# capture the 2nd record timestamp
# `partition=0` keeps the records in storage order: with the default `partition=all`
# they come back newest-first, so "the 2nd record" would mean the other end (#102).
TS=$(curl -s "http://localhost:8080/api/clusters/demo/topics/orders/messages?partition=0&offset=earliest" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['records'][1]['timestamp'])")
curl -s "http://localhost:8080/api/clusters/demo/topics/orders/messages?partition=0&offset=timestamp:$TS"
```

## Post-conditions

1. No change (read-only).

## Cleanup Steps

1. None.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Expecting record-level seek precision | High | Medium | Assert batch-granular behaviour; to test record-level boundaries, produce records in separate batches (add delay between produces) |
| Clock/timestamp confusion (ms vs s) | Medium | Low | Timestamps are epoch milliseconds |

## Dependencies

- Topic `orders` populated.

## Notes

- **Key learning:** timestamp seek is batch-granular. In the demo the 3 `orders` records were produced in one batch (offsets 0-2 share/adjoin timestamps), so seeking to the 2nd record's timestamp returned all 3 from offset 0. To exercise record-level boundaries, produce records in distinct batches (e.g. a short sleep between produces) and re-verify.
