# Test Case: MSG-004

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-004 |
| **Test Case Title** | Filter Messages by Value Substring (`value_contains`) |
| **Test Type** | Functional, Integration |
| **Priority** | Medium |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that the `value_contains` filter returns only records whose decoded value
matches the substring, sets `filtered: true`, and reports `scanned` (records
examined) distinctly from `count` (records matched).

## Requirements Traceability

- **User Story**: As a user, I want to filter a topic's messages by content so that I can find specific records quickly.
- **Requirement ID**: MSG-REQ-004 (Content filtering)
- **Business Rule**: With a filter active, `filtered: true`; `count` = matches returned; `scanned` = records examined (up to `max_scan`); non-matching records are excluded.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topic `orders` — 3 records; values contain `widget`, `gadget`, `gizmo`.
3. **Environment**: Base URL `http://localhost:8080`; `curl`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Filter by `widget` | `?offset=earliest&value_contains=widget` | `filtered: true`, `count: 1`, `scanned: 3`; the single record is offset 0 (`item: widget`) |
| 2 | Filter with no match | `?offset=earliest&value_contains=zzzznotfound` | `filtered: true`, `count: 0`, `records: []`, `scanned: 3` |
| 3 | Filter matching multiple | `?offset=earliest&value_contains=item` | `count: 3` (all values contain `item`) |
| 4 | Regex filter | `?offset=earliest&value_contains=wi.get&regex=true` | Matches `widget` via regex |
| 5 | UI filter | use the search/filter box in the event browser | Same matches as the API |

## Expected Results

### Primary Verification Points

1. Only records whose value contains the substring are returned.
2. `filtered: true` whenever a filter is supplied.
3. `count` = number of matches; `scanned` = number of records examined (3 here).
4. A no-match filter returns `count: 0` with an empty `records` array (not an error).

### Secondary Verification Points

5. `regex=true` interprets `value_contains` as a regular expression.
6. UI filtering matches API results.

## Test Data

```json
{
  "topic": "orders",
  "filters": [
    { "query": "value_contains=widget",        "expect": { "count": 1, "filtered": true, "scanned": 3 } },
    { "query": "value_contains=zzzznotfound",   "expect": { "count": 0, "filtered": true, "scanned": 3 } },
    { "query": "value_contains=item",           "expect": { "count": 3, "filtered": true } },
    { "query": "value_contains=wi.get&regex=true", "expect": { "count": 1 } }
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
| `max_scan` truncates scanning on large topics | Medium | Medium | For big topics, raise `max_scan`; note `scanned` vs `count` |
| Invalid regex string | Low | Low | Validate the app returns a clear error, not a 5xx |

## Dependencies

- Topic `orders` with the three named values.

## Notes

- Related filters accept `key_contains`, `header_key`, `header_value`, and `regex=true`. `max_scan` bounds how far forward the filter scans; `scanned` reports how many records were examined.
