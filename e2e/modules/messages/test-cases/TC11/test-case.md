# Test Case: MSG-011

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-011 |
| **Test Case Title** | Cross-Partition Search Returns Matches From Every Partition Within One Scan Budget |
| **Test Type** | Functional, Positive, Negative, Performance-boundary |
| **Priority** | High |
| **Estimated Duration** | 5-8 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-08-28 |
| **Last Modified** | 2026-08-28 |

## Test Objective

Verify that `partition=all` reads every partition of a topic in one request,
merges the results newest-first, and does so within a **topic-wide** scan budget —
not one budget per partition. Verify that the parameter combinations that cannot
mean anything are rejected with a 400 rather than silently reinterpreted.

## Requirements Traceability

- **User Story**: As a user hunting one event in a partitioned topic, I want to search the whole topic at once so that I do not have to repeat the search once per partition and remember which ones I have tried.
- **Requirement ID**: MSG-REQ-011 (Cross-partition search, #102)
- **Business Rules**:
  - `partition` defaults to `all`; a number narrows to one partition and keeps the historical response shape.
  - With `all`, records are ordered by timestamp descending, tie-broken by `(partition, offset)`. The order is **best effort**: Kafka does not order timestamps across partitions, and the response says so via `order_best_effort: true`.
  - The scan budget (`max_scan`) belongs to the topic. A search over N partitions must not read N × the budget.
  - `offset=<n>` names a different record in each partition, so it is only valid against a single partition.

## Preconditions

1. **System State**: Stack up (`docker compose up -d`), source connected, seed applied.
2. **Test Data**: topic `spread` — 3 partitions, 12 keyed records (`k-1` … `k-12`) distributed across all three by the key hash. `orders` — 1 partition, 3 records.
3. **Environment**: Base URL `http://localhost:8080`; `curl`; browser.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Confirm the data really spans partitions | `GET /api/clusters/demo/topics/spread` | More than one entry in `partitions[]` has `messages > 0` |
| 2 | Search the whole topic | `GET .../topics/spread/messages?partition=all&offset=earliest&limit=100` | `count: 12`; `records[].partition` covers every populated partition |
| 3 | Check the ordering | same response | `timestamp` values are non-increasing; `order: "timestamp_desc"`; `order_best_effort: true` |
| 4 | Check the provenance summary | same response | `partitions[]` has one entry per partition with `watermark`, `scanned`, `exhausted`; no top-level `watermark` |
| 5 | Filter across partitions | `&key_contains=k-12` | `count: 1`, and the match is returned regardless of which partition holds it |
| 6 | Topic-wide budget | `&value_contains=zzz-no-match&max_scan=6` | `scanned` ≤ 6 + partition count — **not** 6 × 3; `exhausted: false` |
| 7 | Narrow to one partition | `?partition=0&offset=earliest` | Historical shape: top-level `partition` and `watermark`, no `partitions[]`, storage order preserved |
| 8 | Reject a concrete offset | `?partition=all&offset=42` | HTTP 400, error naming the constraint ("needs a single partition") |
| 9 | Reject an unparseable partition | `?partition=nope` | HTTP 400, error naming `'all'` as the alternative |
| 10 | Out-of-range partition | `?partition=99` | HTTP 400, `partition 99 out of range (topic has 3 partitions)` |
| 11 | UI default | open `spread` in the event browser | Partition control reads **All partitions**; searching shows a `partition` column and a per-partition summary |
| 12 | UI narrowing | select partition `0`, search | `partition` column disappears; the single-partition watermark line returns |

## Expected Results

### Primary Verification Points

1. One request returns matches from every populated partition.
2. Ordering is newest-first and declared best-effort in the payload.
3. `scanned` for `partition=all` stays within one topic-wide budget.
4. Steps 8-10 are 400s with messages that name the constraint, not 500s.

### Secondary Verification Points

5. The single-partition response shape is byte-for-byte what it was before #102, so existing callers and the Python bindings are unaffected when they pass a partition.
6. Every partition receives at least one record of budget, so no partition is silently unsearchable on a wide topic with a small limit.

## Test Data

```json
{
  "topic": "spread",
  "partitions": 3,
  "records": 12,
  "keys": ["k-1", "k-2", "k-3", "k-4", "k-5", "k-6", "k-7", "k-8", "k-9", "k-10", "k-11", "k-12"],
  "expected": {
    "order": "timestamp_desc",
    "order_best_effort": true,
    "count": 12
  }
}
```

## Post-conditions

1. No data is modified — every step is a read.

## Cleanup Steps

1. None, or delete `spread`.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| A naive fan-out multiplies S3 reads by the partition count | Medium | **High** — breaks the on-demand contract on a wide topic | Step 6 asserts the budget; the split is unit-tested in `backend/src/query.rs` |
| The key hash puts all 12 records in one partition, making step 2 vacuous | Low | Medium | Step 1 asserts the spread before step 2 relies on it |
| Timestamps identical across records make the ordering assertion vacuous | Medium | Low | Ordering is asserted as non-increasing, which holds either way; the tie-break is unit-tested separately |
| A 200-partition topic opens 200 simultaneous ranged GETs | Low | High | Concurrency is bounded by `FANOUT` in `backend/src/query.rs` |

## Dependencies

- `e2e/scripts/seed.sh` creates and populates `spread`.
- Automated equivalent: the `partition=all` cases in `e2e/ci/smoke.spec.ts`.

## Notes

- The merge is **not** a total order and must never be presented as one: two records in different partitions can carry the same timestamp, and a producer can write out of order. `order_best_effort: true` exists so a consumer of the API cannot mistake the merge for a global sort.
- Narrowing to a single partition intentionally returns records in storage order, not newest-first. Making both directions consistent is #108.
