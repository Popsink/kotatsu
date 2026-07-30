# Test Case: MSG-009

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-009 |
| **Test Case Title** | Invalid Read Requests Return Clear 4xx Errors |
| **Test Type** | Functional, Negative, Error Handling |
| **Priority** | High |
| **Estimated Duration** | 2-3 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-29 |
| **Last Modified** | 2026-07-29 |

## Test Objective

Verify that malformed or unsatisfiable read requests return appropriate 4xx
status codes with a descriptive JSON `error` body — never a 5xx or a stack
trace.

## Requirements Traceability

- **User Story**: As a user, I want clear errors on bad requests so that I can correct my query.
- **Requirement ID**: MSG-REQ-009 (Read error handling)
- **Business Rule**: An unparseable `offset` yields HTTP 400; a nonexistent topic yields HTTP 404; the body is `{"error": "<message>"}`.

## Preconditions

1. **System State**: Stack up; source connected; topic `orders` exists.
2. **Test Data**: invalid offset string `abc`; nonexistent topic `nope`.
3. **Environment**: Base URL `http://localhost:8080`; `curl -w "%{http_code}"`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Invalid offset | `?offset=abc` on `orders` | HTTP 400; body `{"error":"invalid offset: abc"}` |
| 2 | Nonexistent topic | `GET .../topics/nope/messages` | HTTP 404; body `{"error":"topic 'nope' not found"}` |
| 3 | Nonexistent cluster | `GET /api/clusters/ghost/topics` | 4xx or empty result (no 5xx) |
| 4 | Nonexistent group | `GET /api/clusters/demo/groups/ghost` | 4xx / not-found (no 5xx) |
| 5 | Health unaffected | `GET /api/health` after the above | HTTP 200, `status: "ok"` |

## Expected Results

### Primary Verification Points

1. Invalid `offset` → HTTP 400 with `{"error":"invalid offset: abc"}`.
2. Nonexistent topic → HTTP 404 with `{"error":"topic 'nope' not found"}`.
3. No request returns a 5xx or a raw stack trace.

### Secondary Verification Points

4. `/api/health` stays `ok` throughout — bad requests do not destabilise the service.

## Test Data

```json
{
  "cases": [
    { "request": "orders/messages?offset=abc", "status": 400, "error": "invalid offset: abc" },
    { "request": "nope/messages",              "status": 404, "error": "topic 'nope' not found" }
  ]
}
```

### Reference commands

```bash
curl -s -w " [HTTP %{http_code}]" "http://localhost:8080/api/clusters/demo/topics/orders/messages?offset=abc"
curl -s -w " [HTTP %{http_code}]" "http://localhost:8080/api/clusters/demo/topics/nope/messages"
```

## Post-conditions

1. No change (read-only).

## Cleanup Steps

1. None.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Exact status for cluster/group not-found unverified | Medium | Low | Record the actual status/body for steps 3-4 on first run and pin them |
| Error message wording changes | Low | Low | Assert status code primarily; treat exact text as informative |

## Dependencies

- Topic `orders` exists (for the invalid-offset case).

## Notes

- Confirmed responses: invalid offset → 400 `{"error":"invalid offset: abc"}`; unknown topic → 404 `{"error":"topic 'nope' not found"}`. Steps 3-4 should be run once to pin the exact status codes for cluster/group not-found.
