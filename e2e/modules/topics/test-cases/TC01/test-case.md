# Test Case: TOP-001

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | TOP-001 |
| **Test Case Title** | List Topics With Message Counts and Storage Size |
| **Test Type** | Functional, Integration |
| **Priority** | Critical |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that the topic listing endpoint (and the UI topics view) enumerates every
topic in the cluster with the correct message count, partition count and
non-negative storage size, including topics with zero messages.

## Requirements Traceability

- **User Story**: As a user, I want to see all topics with their size and message counts so that I can pick one to inspect.
- **Requirement ID**: TOP-REQ-001 (Topic enumeration)
- **Business Rule**: The listing reflects exactly the topics present in S3; counts are derived from watermarks; an empty topic shows `messages: 0`, `storage_bytes: 0`.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data** (produce beforehand):
   - `orders` — 1 partition, 3 messages (MSG-001)
   - `events` — 3 partitions, 6 messages
   - `empty-topic` — 1 partition, 0 messages
3. **Environment**: Base URL `http://localhost:8080`; `curl`; browser.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Produce the three topics above | see Test Data | Topics exist in cluster `demo` |
| 2 | List topics via API | `GET /api/clusters/demo/topics` | HTTP 200; `total: 3`; `items` length 3 |
| 3 | Verify `orders` entry | inspect item | `messages: 3`, `partitions: 1`, `storage_bytes > 0` |
| 4 | Verify `events` entry | inspect item | `messages: 6`, `partitions: 3`, `storage_bytes > 0` |
| 5 | Verify `empty-topic` entry | inspect item | `messages: 0`, `partitions: 1`, `storage_bytes: 0` |
| 6 | Verify pagination envelope | inspect body | `limit: 50`, `offset: 0` present |
| 7 | Open UI topics view | navigate to cluster `demo` | The three topics render with matching counts |
| 8 | Search filter | `GET /api/clusters/demo/topics?search=ord` | Only `orders` returned |

## Expected Results

### Primary Verification Points

1. All produced topics are listed; `total` equals the number of topics.
2. Per-topic `messages`, `partitions`, `storage_bytes` match production.
3. An empty topic appears with zeros (not omitted, not erroring).

### Secondary Verification Points

4. `search` narrows the list by substring on the topic name.
5. UI counts match the API response.

## Test Data

```json
{
  "cluster": "demo",
  "expected_items": [
    { "name": "empty-topic", "messages": 0, "partitions": 1, "storage_bytes": 0 },
    { "name": "events",      "messages": 6, "partitions": 3 },
    { "name": "orders",      "messages": 3, "partitions": 1 }
  ],
  "envelope": { "total": 3, "limit": 50, "offset": 0 }
}
```

## Post-conditions

1. Topics unchanged (read-only).

## Cleanup Steps

1. None, or `docker compose down -v` for a clean slate.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| S3 write lag right after producing | Low | Medium | Retry the listing after a few seconds |
| Ordering assumptions | Medium | Low | Assert set membership, not array order |

## Dependencies

- Topics produced as per Test Data.

## Notes

- Do not assume a specific ordering of `items`; assert on set membership and per-topic fields.
