# Test Case: GRP-003

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | GRP-003 |
| **Test Case Title** | Groups List Reports Lag On Request, Ranks By It, And Stays Cheap Without It |
| **Test Type** | Functional, Integration |
| **Priority** | High |
| **Estimated Duration** | 5-6 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-09-02 |
| **Last Modified** | 2026-09-02 |

## Test Objective

Verify that the consumer-group **listing** carries each group's lag when
`?lag=true` is passed, that the ranking it produces covers the whole result set
rather than the current page, and that omitting the flag returns the pre-#107
payload — the opt-in that keeps the plain listing cheap.

## Requirements Traceability

- **User Story**: As a user, I want to see which consumer group is furthest behind without opening each one so that I can find the unhealthy one at a glance.
- **Requirement ID**: GRP-REQ-003 (Lag in the listing)
- **Business Rule**: `lag` is absent unless requested; `lag.total` is the sum of per-partition lags; a group with **no committed offsets** reports `total: null` (rendered `—`), which is not the same as a caught-up group's `0`; ranking is by `lag.total` descending over every match, tie-broken by name.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: group `qa-group` fully consumed `orders` (3 records) → lag 0 (from GRP-001). A second group is created in step 4.
3. **Environment**: Base URL `http://localhost:8080`; `curl`; a Kafka client on the compose network for step 4.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | List groups, no flag | `GET /api/clusters/demo/groups` | HTTP 200; each item has `name`, `state`, `members` and **no `lag` key at all** |
| 2 | List groups with lag | `GET /api/clusters/demo/groups?lag=true` | HTTP 200; `qa-group` carries `lag: { total: 0, topics: 1, max_partition: 0 }` |
| 3 | Cross-check against detail | `GET /api/clusters/demo/groups/qa-group` | The detail's `total_lag` equals the listing's `lag.total` |
| 4 | Create a group that is behind | consume 1 of the 3 `orders` records with a new group `lagging-group`, then produce 5 more records to `orders` | `lagging-group` exists with a committed offset below the high watermark |
| 5 | Verify the ranking | `GET /api/clusters/demo/groups?lag=true` | `lagging-group` is **first**, ahead of `qa-group`, even though `q` sorts before `l` |
| 6 | Verify ranking beats paging | `GET /api/clusters/demo/groups?lag=true&limit=1` | `items` holds `lagging-group` alone; `total` still counts every group |
| 7 | Verify name order is reachable | `GET /api/clusters/demo/groups?lag=true&sort=name` | Groups come back alphabetically, each still carrying its `lag` |
| 8 | Verify the uncommitted case | create group `idle-group` with no committed offset (e.g. a consumer that joins and reads nothing), re-run step 2 | `idle-group` reports `lag: { total: null, topics: 0, max_partition: null }` and ranks **last** |
| 9 | UI listing | open `/groups` in the UI | `topics` and `lag` columns present; `lagging-group` on top; `lag` header shows a `▼`; `idle-group` renders `—` in both columns, not `0` |
| 10 | UI sort toggle | click the `lag` column header | Order becomes alphabetical, the `▼` disappears, and the list returns to page 1 |

## Expected Results

### Primary Verification Points

1. `lag` is absent from every row when the flag is omitted (opt-in cost).
2. With `lag=true`, `lag.total` matches the group detail's `total_lag`.
3. The most-behind group in the **cluster** reaches the first page, not just the top of whatever page name order produced.
4. `total` reports every match, not the size of the returned page.
5. A group with no committed offsets reports `total: null` and renders `—`.

### Secondary Verification Points

6. `sort=name` keeps lag figures while restoring alphabetical order.
7. `lag.max_partition` names the worst single partition, which the total hides.
8. Repeating step 2 within ~45 s is served from the catalog cache and returns the same figures.

## Test Data

```json
{
  "no_flag_item": { "name": "qa-group", "state": "Empty", "members": 0 },
  "with_lag_item": {
    "name": "qa-group",
    "state": "Empty",
    "members": 0,
    "lag": { "total": 0, "topics": 1, "max_partition": 0 }
  },
  "uncommitted_item": {
    "name": "idle-group",
    "lag": { "total": null, "topics": 0, "max_partition": null }
  }
}
```

## Post-conditions

1. `orders` holds 8 records (3 seeded + 5 from step 4) and `lagging-group` remains behind.
2. `idle-group` exists with no committed offsets.

## Cleanup Steps

1. `docker compose down -v` and re-seed, if a later case needs `orders` back at 3 records and only `qa-group` present.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Catalog cache serves a stale figure right after step 4 | Medium | Low | Wait out the 45 s TTL, or accept that the previous figure is bounded-stale by design (#84) |
| A live consumer keeps committing during the read | Medium | Medium | Stop the step-4 consumer before asserting |
| Step 8's consumer commits an offset despite reading nothing | Medium | Medium | Verify no `offsets/` objects exist under the group before asserting `total: null` |

## Dependencies

- Group `qa-group` with committed offsets on `orders` (GRP-001, GRP-002).
- Topic `orders` from the seed (`e2e/scripts/seed.sh`).

## Notes

- Steps 1, 2 and 9 are automated in `e2e/ci/smoke.spec.ts`; steps 4-8 are manual because they need a second consumer group and a deliberately un-consumed backlog, which the seed does not create.
- The `0` vs `—` distinction in step 8 is the point of the whole case: a group that has never committed is not a healthy one, and a listing that shows it as `0` says the opposite of the truth.
