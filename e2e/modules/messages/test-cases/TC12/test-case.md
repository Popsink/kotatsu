# Test Case: MSG-012

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-012 |
| **Test Case Title** | Paging Through A Topic Resumes The Read, And A Query Survives As A URL |
| **Test Type** | Functional, Positive, Negative |
| **Priority** | High |
| **Estimated Duration** | 6-10 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-08-31 |
| **Last Modified** | 2026-08-31 |

## Test Objective

Verify that a read can be continued instead of restarted: every response names a
resume point per partition, and handing those back as `cursor` returns the next
window with no record repeated and none stepped over. Verify that the query lives
in the URL, so an investigation can be bookmarked, pasted into a ticket, and
reopened on exactly the result set it captured.

## Requirements Traceability

- **User Story**: As a user who has found the window my event is in, I want to walk past the end of it, and to hand someone the exact search I ran, so that an investigation is not trapped in one screenful and one browser tab.
- **Requirement ID**: MSG-REQ-012 (Pagination and permalinks, #104)
- **Business Rules**:
  - Every response carries a `resume` point per partition — the offset the next page starts from — and `null` once that partition has nothing left in the read direction.
  - `cursor=0:412,3:998` continues the read. `offset` then names only the **direction** of travel; the cursor supplies each partition's start. A partition absent from the cursor is one the previous page exhausted, and is not read again.
  - The resume point sits past the last record the page **returned**, not past everything it scanned: a record dropped by the merge's truncation was never shown and must come back. Only a partition that returned nothing resumes at its scan frontier, so an unproductive filtered region is not walked twice.
  - The URL carries every control except the cursor — a permalink reproduces the **first** page, and a resume point pasted without the page it continues means nothing.
  - Opening such a URL runs the query on arrival. The user clicked the link, so this is still a user action and does not break the on-demand contract (#7).

## Preconditions

1. **System State**: Stack up (`docker compose up -d`), source connected, seed applied.
2. **Data**: `spread` — 12 keyed records across 3 partitions.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Read the first window | `GET .../topics/spread/messages?partition=all&offset=earliest&limit=5` | `count: 5`; `exhausted: false`; at least one `partitions[].resume` is not `null` |
| 2 | Continue it | same URL `+ &cursor=` built from `partitions[].resume` | `count > 0`; no `(partition, offset)` pair from step 1 appears again |
| 3 | Page to the end | repeat step 2 until `exhausted: true` | every record seen exactly once; 12 in total |
| 4 | Page backwards | `?partition=all&offset=latest&limit=5`, then follow the cursor | the same 12 records, reached from the newest end |
| 5 | Continue a capped filtered scan | `&value_contains=zzz-no-match&max_scan=6`, then follow the cursor | `scanned` advances; the second page does not re-read the offsets the first one scanned |
| 6 | Exhausted cursor | `&cursor=` (empty) | `count: 0`, `exhausted: true`, HTTP 200 — paging past the end is not an error |
| 7 | Reject a foreign partition | `?partition=0&cursor=2:1` | HTTP 400, error saying the cursor names a partition this query does not read |
| 8 | Reject a malformed cursor | `?partition=all&cursor=nope` | HTTP 400, error naming the `partition:offset` shape |
| 9 | UI — the URL follows the search | run a search in the event browser | the address bar gains `?from=…` and the filters that were set; defaults are **not** written |
| 10 | UI — the URL replays | open that URL in a fresh tab | the controls come back set, the query runs on arrival, the same rows show |
| 11 | UI — Load more | `?from=earliest&limit=5` on `spread`, click **Load more** | the table grows to 10 rows; the earlier rows stay where they were |
| 12 | UI — Back | click **Back** | the table returns to 5 rows with no network request |
| 13 | UI — end of the read | keep clicking **Load more** | the button disables and the line reads *end of the topic* |
| 14 | UI — Copy link | click **Copy link** | the clipboard holds the permalink; the button confirms, and says so if the clipboard is refused (#65) |

## Expected Results

### Primary Verification Points

1. Paging covers a topic exactly once — no gap, no repeat.
2. A filtered `Load more` continues the scan rather than restarting it.
3. A pasted URL reproduces the result set it was copied from.
4. `Back` is free: it walks windows already in hand.

## Test Data

```json
{
  "topic": "spread",
  "partitions": 3,
  "records": 12,
  "page": { "limit": 5, "pages_to_exhaust": 3 },
  "cursor_example": "0:2,1:2,2:1"
}
```

## Post-conditions

1. No data is modified — every step is a read.

## Cleanup Steps

1. None.

## Risk Assessment

- `e2e/scripts/seed.sh` creates and populates `spread`.
- Automated equivalent: the cursor and permalink cases in `e2e/ci/smoke.spec.ts`.

## Notes

- Paging is exact as long as a partition's own timestamps do not go backwards. A producer sets them, so they can; a record whose timestamp regressed far enough to be truncated out of a page below one that was kept is stepped over. Resuming from the lowest gap instead would return records already shown, which reads worse. This shares the `order_best_effort` caveat of MSG-011 rather than pretending a total order the log does not have.
- `MAX_LIMIT` stays 500. Pagination is the answer to "I need more", not a bigger cap.
