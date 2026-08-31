# Test Case: SRC-002

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | SRC-002 |
| **Test Case Title** | Empty Bucket Reports Not Connected With a Clear Error |
| **Test Type** | Functional, Negative, Configuration |
| **Priority** | High |
| **Estimated Duration** | 2-3 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that when the configured bucket contains no data for the configured
cluster, `GET /api/source/status` still returns HTTP 200 with `configured: true` but
`connected: false` and a human-readable error explaining the cluster was
not found.

## Requirements Traceability

- **User Story**: As a user, I want a clear message when Kotatsu cannot find its cluster so that I can fix my configuration.
- **Requirement ID**: SRC-REQ-002 (Source error reporting)
- **Business Rule**: A missing cluster prefix must produce `connected: false` with a descriptive error, never a crash or a 5xx.

## Preconditions

1. **System State**: Fresh stack with an empty `tansu` bucket (no topics produced yet), OR the bucket cleared via `docker compose down -v && docker compose up -d`.
2. **Test Data**: bucket `tansu`, cluster `demo`.
3. **Environment**: Base URL `http://localhost:8080`; `curl` available.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Ensure the bucket has no `demo` cluster data | `docker compose down -v` then `up -d`, do NOT produce anything | Bucket empty |
| 2 | Query source status | `GET /api/source/status` | HTTP 200 |
| 3 | Inspect status | response body | `configured: true`, `connected: false` |
| 4 | Inspect error | response body | `error` = `"cluster 'demo' not found in the bucket"` (or equivalent descriptive text) |
| 5 | Query clusters | `GET /api/clusters` | `clusters: []` |

## Expected Results

### Primary Verification Points

1. HTTP 200 (not a 4xx/5xx) despite no data.
2. `configured: true` (config is valid), `connected: false` (no data found).
3. A descriptive `error` string naming the missing cluster.
4. `GET /api/clusters` returns an empty list.

### Secondary Verification Points

5. UI surfaces the "not connected" / empty state gracefully (no blank crash).

## Test Data

```json
{
  "bucket": "tansu",
  "cluster": "demo",
  "expected": {
    "configured": true,
    "status": { "connected": false, "error": "cluster 'demo' not found in the bucket" }
  }
}
```

## Post-conditions

1. No data created; bucket remains empty.

## Cleanup Steps

1. None, or proceed to produce data for subsequent cases (SRC-001, MSG-001).

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Residual data from prior cases | Medium | Medium | Use `docker compose down -v` to guarantee an empty bucket |
| Error wording changes | Low | Low | Assert on `connected: false` primarily; treat exact string as informative |

## Dependencies

- MinIO up with an empty `tansu` bucket.

## Notes

- This is the empty-state counterpart to SRC-001. The exact error string is informative; the contract is `connected: false` + a descriptive message + HTTP 200.
