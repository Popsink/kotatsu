# Test Case: MSG-002

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-002 |
| **Test Case Title** | Paginate Messages With `offset` and `limit` |
| **Test Type** | Functional, Integration, Boundary |
| **Priority** | High |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that the messages endpoint honours `offset` (`earliest` | `latest` |
`<number>` | `timestamp:<ms>`) and `limit`, returns the correct slice of records,
and reports `exhausted` accurately (false when more records remain, true when the
end is reached).

## Requirements Traceability

- **User Story**: As a user, I want to page through a topic's messages so that I can browse large topics without loading everything at once.
- **Requirement ID**: MSG-REQ-002 (Pagination)
- **Business Rule**: `limit` bounds the number of records; `exhausted` is `false` when `high` watermark is beyond the last returned offset; a numeric `offset` starts the read at that offset.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topic `orders` — 3 records at offsets 0,1,2 (MSG-001).
3. **Environment**: Base URL `http://localhost:8080`; `curl`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | First page | `?offset=earliest&limit=2` | `count: 2`, offsets 0 and 1, `exhausted: false`, `watermark.high: 3` |
| 2 | Next page from offset 2 | `?offset=2&limit=2` | `count: 1`, offset 2, `exhausted: true` |
| 3 | Read from latest | `?offset=latest&limit=50` | Returns up to the last records ending at high watermark |
| 4 | Limit larger than data | `?offset=earliest&limit=50` | `count: 3`, `exhausted: true` |
| 5 | UI pagination | open `orders`, use next/prev controls | Pages match the API slices |

## Expected Results

### Primary Verification Points

1. `limit` caps the number of returned records.
2. `exhausted: false` when more records exist beyond the page; `true` at the end.
3. A numeric `offset` begins the read at that offset (inclusive).
4. `watermark.high` stays 3 regardless of the page.

### Secondary Verification Points

5. `offset=latest` returns the tail of the topic.
6. UI next/prev navigation matches API paging.

## Test Data

```json
{
  "topic": "orders",
  "pages": [
    { "query": "offset=earliest&limit=2", "expect": { "count": 2, "offsets": [0, 1], "exhausted": false } },
    { "query": "offset=2&limit=2",        "expect": { "count": 1, "offsets": [2],    "exhausted": true } },
    { "query": "offset=earliest&limit=50","expect": { "count": 3, "exhausted": true } }
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
| Misreading `exhausted` semantics | Medium | Low | `exhausted` reflects end-of-partition, not end-of-page |
| Default `offset` is `latest` | High | Medium | Always pass `offset=earliest` when reading from the start |

## Dependencies

- Topic `orders` with 3 records.

## Notes

- Server defaults: `offset=latest`, `limit=50`. `offset` also accepts `timestamp:<ms>` — an optional extra step can validate time-based seeking.
