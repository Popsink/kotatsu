# Test Case: HLT-001

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | HLT-001 |
| **Test Case Title** | Health, Source Metadata and Cluster Stats Endpoints |
| **Test Type** | Functional, Integration, Smoke |
| **Priority** | Critical |
| **Estimated Duration** | 2-3 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-29 |
| **Last Modified** | 2026-07-29 |

## Test Objective

Verify the service exposes a working health probe (on both `/health` and
`/api/health`), reports its source metadata, and returns cluster-level stats
(topics, producers, transactions) consistent with the topic listing.

## Requirements Traceability

- **User Story**: As an operator, I want health and stats endpoints so that I can monitor Kotatsu and see cluster totals at a glance.
- **Requirement ID**: HLT-REQ-001 (Health & stats)
- **Business Rule**: `/health` and `/api/health` both return `{"service":"kotatsu","status":"ok"}`; `/api/clusters/{cluster}` returns `topics` equal to the topic-listing `total`.

## Preconditions

1. **System State**: Stack up; source connected; some topics produced.
2. **Test Data**: cluster `demo` with the topics seeded by prior cases.
3. **Environment**: Base URL `http://localhost:8080`; `curl -w "%{http_code}"`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Health (root) | `GET /health` | HTTP 200; `{"service":"kotatsu","status":"ok"}` |
| 2 | Health (api) | `GET /api/health` | HTTP 200; identical body |
| 3 | Source metadata | `GET /api/source` | `configured: true`; `bucket`, `cluster`, `endpoint`, `region` present (no `status` — the probe moved to `/api/source/status`) |
| 3b | Source reachable | `GET /api/source/status` | `connected: true` |
| 4 | Cluster stats | `GET /api/clusters/demo` | `cluster: "demo"`; integer `topics`, `producers`, `transactions` (>= 0) |
| 5 | Consistency check | compare step 4 `topics` with `GET /api/clusters/demo/topics` `total` | equal |
| 6 | UI header/status | open the UI | Source/cluster indicators reflect the same figures |

## Expected Results

### Primary Verification Points

1. Both health paths return HTTP 200 and the same `{service, status}` body.
2. `/api/source` reports the configured source, and `/api/source/status` reports `connected: true`.
3. `/api/clusters/demo` returns numeric `topics`, `producers`, `transactions`.
4. Cluster `topics` equals the topic-listing `total`.

### Secondary Verification Points

5. UI indicators match the API figures.

## Test Data

```json
{
  "health": { "service": "kotatsu", "status": "ok" },
  "cluster_stats_shape": { "cluster": "demo", "topics": "int>=0", "producers": "int>=0", "transactions": "int>=0" },
  "invariant": "clusters/demo.topics == clusters/demo/topics.total"
}
```

### Reference commands

```bash
curl -s -w " [HTTP %{http_code}]" http://localhost:8080/health
curl -s -w " [HTTP %{http_code}]" http://localhost:8080/api/health
curl -s http://localhost:8080/api/source
curl -s http://localhost:8080/api/source/status
curl -s http://localhost:8080/api/clusters/demo
```

## Post-conditions

1. No change (read-only).

## Cleanup Steps

1. None.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Stats depend on how much was produced | High | Low | Assert types/invariants (topics == listing total), not fixed counts |
| `transactions`/`producers` semantics unclear | Medium | Low | Assert `>= 0` integers; note observed values on first run |

## Dependencies

- Some topics produced in cluster `demo`.

## Notes

- Observed shape: `/api/clusters/demo` → `{"cluster":"demo","producers":<int>,"topics":<int>,"transactions":<int>}`. Counts scale with produced data, so assert the topics-equals-listing invariant rather than a fixed number.
