# Test Case: SRC-001

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | SRC-001 |
| **Test Case Title** | S3 Source Reports Connected With a Valid Configuration |
| **Test Type** | Functional, Integration, Configuration |
| **Priority** | Critical |
| **Estimated Duration** | 2-3 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that, when Kotatsu is pointed at a valid S3 bucket that contains at least
one Tansu cluster, `GET /api/source` reports the configured bucket/cluster/endpoint,
`GET /api/source/status` reports `connected: true`, and the cluster is discoverable.

## Requirements Traceability

- **User Story**: As a user, I want to confirm Kotatsu is correctly connected to its S3 source so that I trust the data it displays.
- **Requirement ID**: SRC-REQ-001 (Source connectivity)
- **Business Rule**: `/api/source/status` reports `connected: true` only when the configured bucket is reachable and the configured cluster prefix exists. `/api/source` itself performs no object-store call (#109).

## Preconditions

1. **System State**:
   - Docker stack up; Kotatsu reachable at `http://localhost:8080`.
   - At least one topic produced in cluster `demo` (see MSG-001) so the cluster prefix exists in the bucket.
2. **Test Data** (from `docker-compose.yml`): bucket `tansu`, cluster `demo`, endpoint `http://minio:9000`, region `us-east-1`.
3. **Environment**: Base URL `http://localhost:8080`; `curl` available.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Ensure at least one topic exists | run MSG-001 steps 1-2 | Topic `orders` created and populated |
| 2 | Query source | `GET /api/source` | HTTP 200, no `status` key |
| 3 | Inspect configuration fields | response body | `bucket: "tansu"`, `cluster: "demo"`, `endpoint: "http://minio:9000"`, `region: "us-east-1"`, `configured: true` |
| 4 | Inspect connection status | `GET /api/source/status` | `connected: true`, no `error` field |
| 5 | Confirm cluster discovery | `GET /api/clusters` | `clusters` contains `"demo"` |

## Expected Results

### Primary Verification Points

1. `GET /api/source` returns `configured: true`; `GET /api/source/status` returns `connected: true`.
2. The configuration echoes the compose environment exactly.
3. `GET /api/clusters` lists `demo`.

### Secondary Verification Points

4. No error string is present under `status`.
5. UI header/source indicator (if present) shows the source as connected.

## Test Data

```json
{
  "bucket": "tansu",
  "cluster": "demo",
  "endpoint": "http://minio:9000",
  "region": "us-east-1",
  "expected": { "configured": true, "status": { "connected": true } }
}
```

## Post-conditions

1. Source remains connected; no state change caused by the read.

## Cleanup Steps

1. None (read-only test).

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Empty bucket → `connected: false` | Medium | Low | Produce a topic first (precondition); covered by SRC-002 |
| Wrong endpoint in compose | Low | High | Verify `docker-compose.yml` env before test |

## Dependencies

- MinIO up and `tansu` bucket created (`createbucket` service).
- At least one cluster prefix present in the bucket.

## Notes

- On a completely empty bucket the source can report `connected: false` with `error: "cluster 'demo' not found in the bucket"` — that is expected and is covered by SRC-002.
