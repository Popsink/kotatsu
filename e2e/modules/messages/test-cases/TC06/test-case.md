# Test Case: MSG-006

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-006 |
| **Test Case Title** | Filter Messages by Key Substring (`key_contains`) |
| **Test Type** | Functional, Integration |
| **Priority** | Medium |
| **Estimated Duration** | 2-3 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-29 |
| **Last Modified** | 2026-07-29 |

## Test Objective

Verify that the `key_contains` filter returns only records whose decoded key
matches the substring, independently of the value, and reports `filtered: true`
with accurate `count`/`scanned`.

## Requirements Traceability

- **User Story**: As a user, I want to filter messages by key so that I can find the records for a specific key quickly.
- **Requirement ID**: MSG-REQ-006 (Key filtering)
- **Business Rule**: `key_contains` matches on the decoded key; non-matching records are excluded; `filtered: true` when the filter is present.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topic `orders` — keys `key-1`, `key-2`, `key-3`.
3. **Environment**: Base URL `http://localhost:8080`; `curl`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Filter exact key | `?offset=earliest&key_contains=key-2` | `filtered: true`, `count: 1`; the record is offset 1 with key `key-2` |
| 2 | Filter common prefix | `?offset=earliest&key_contains=key-` | `count: 3` (all keys share the prefix) |
| 3 | Filter with no match | `?offset=earliest&key_contains=zzz` | `count: 0`, `records: []`, `filtered: true` |
| 4 | UI key filter | use the key filter in the event browser | Same matches as the API |

## Expected Results

### Primary Verification Points

1. Only records whose key contains the substring are returned.
2. `filtered: true`; `count` = matches; `scanned` reflects records examined.
3. A no-match filter returns an empty `records` array, not an error.

### Secondary Verification Points

4. UI key filtering matches API results.

## Test Data

```json
{
  "topic": "orders",
  "filters": [
    { "query": "key_contains=key-2", "expect": { "count": 1, "offset": 1, "filtered": true } },
    { "query": "key_contains=key-",  "expect": { "count": 3, "filtered": true } },
    { "query": "key_contains=zzz",   "expect": { "count": 0, "filtered": true } }
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
| Keyless topic → filter matches nothing | Medium | Low | Run against a keyed topic like `orders` |

## Dependencies

- Topic `orders` with keyed records.

## Notes

- Combine with `value_contains` to filter on both; `regex=true` applies to `key_contains` as well.
