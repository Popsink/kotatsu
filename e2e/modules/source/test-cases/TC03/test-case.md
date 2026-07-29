# Test Case: SRC-003

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | SRC-003 |
| **Test Case Title** | Misconfigured Bucket Name Fails to Connect Without Crashing |
| **Test Type** | Functional, Negative, Configuration |
| **Priority** | Medium |
| **Estimated Duration** | 3-5 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-28 |
| **Last Modified** | 2026-07-28 |

## Test Objective

Verify that when Kotatsu is configured with a bucket that does not exist (or is
unreachable), the source endpoint reports `connected: false` with an error and
the app stays responsive — no crash, no 5xx on the health endpoint.

## Requirements Traceability

- **User Story**: As an operator, I want Kotatsu to fail gracefully on a bad S3 configuration so that I can diagnose it from the UI/API.
- **Requirement ID**: SRC-REQ-003 (Resilience to bad config)
- **Business Rule**: An unreachable/nonexistent bucket must not take the service down; `/api/health` stays `ok`.

## Preconditions

1. **System State**: Stack up. Ability to edit `docker-compose.yml` (or override env) and restart the `app` service.
2. **Test Data**: set `KOTATSU_S3_BUCKET` to a nonexistent bucket, e.g. `does-not-exist`.
3. **Environment**: Base URL `http://localhost:8080`; `curl`; docker compose.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Set an invalid bucket for the app | `KOTATSU_S3_BUCKET=does-not-exist` in compose env | Config change staged |
| 2 | Recreate the app service | `docker compose up -d app` | App restarts |
| 3 | Check health | `GET /api/health` | HTTP 200, `{"service":"kotatsu","status":"ok"}` |
| 4 | Query source | `GET /api/source` | HTTP 200, `bucket: "does-not-exist"`, `status.connected: false`, `status.error` present |
| 5 | Query clusters | `GET /api/clusters` | Empty list or graceful error (no 5xx) |
| 6 | Restore config | reset `KOTATSU_S3_BUCKET=tansu`, `docker compose up -d app` | App reconnects; SRC-001 passes again |

## Expected Results

### Primary Verification Points

1. `/api/health` remains `ok` throughout.
2. `/api/source` reports the misconfigured bucket and `connected: false` with an error.
3. No endpoint returns a 5xx / stack trace to the client.

### Secondary Verification Points

4. After restoring the correct bucket, SRC-001 passes again (no persistent bad state).

## Test Data

```json
{
  "misconfigured": { "KOTATSU_S3_BUCKET": "does-not-exist" },
  "expected": { "health": "ok", "source": { "connected": false } },
  "restore": { "KOTATSU_S3_BUCKET": "tansu" }
}
```

## Post-conditions

1. Configuration restored to `tansu`; source connected again.

## Cleanup Steps

1. Ensure `KOTATSU_S3_BUCKET=tansu` is restored and the app recreated.
2. Re-run SRC-001 to confirm recovery.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Forgetting to restore config | Medium | High | Cleanup step is mandatory; verify with SRC-001 |
| App refuses to start on bad config | Low | Medium | If startup fails, that is itself a finding — record actual behavior |

## Dependencies

- Ability to edit compose env and restart the `app` service.

## Notes

- Exact error text is environment-dependent (bucket-not-found vs. access-denied). The contract under test is graceful failure + health stays `ok`.
