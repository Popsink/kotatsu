# Test Case: NAV-002

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | NAV-002 |
| **Test Case Title** | Find a Topic From the Root With Flat Search |
| **Test Type** | Functional, Integration |
| **Priority** | Medium |
| **Estimated Duration** | 2-3 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-09-01 |
| **Last Modified** | 2026-09-01 |

## Test Objective

Verify that a topic can be found by name from any tree level, without knowing
its `org.env.conn` path — that the tree offers the flat mode when a search at a
group level cannot answer, that flat mode matches the full topic name across the
cluster, and that the mode lives in the URL.

## Requirements Traceability

- **User Story**: As a user, I want to search every topic in the cluster by name so that I can find one without already knowing its organization and environment.
- **Requirement ID**: NAV-REQ-002 (Flat topic search)
- **Business Rule**: The hierarchy stays the default. A search at depth < 3 is matched against that level's segment names, and offers flat mode as the escape; flat mode reads `list_topics` (`GET .../topics?search=`), which matches the whole dotted name. `?all=1` selects the mode, `?q=` carries the term, and `?p=` is left untouched so leaving flat mode returns to the branch the user was standing on.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topic `acme.prod.db2.dbz_config` present in cluster `demo` — the only seeded topic with a connector path above it.
3. **Environment**: Base URL `http://localhost:8080`; `curl`; browser.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Confirm the dead end | `GET .../topic-tree?search=dbz_config` | HTTP 200; `total: 0` — the term is matched against org names |
| 2 | Confirm the flat endpoint answers | `GET .../topics?search=dbz_config` | HTTP 200; `total: 1`; `items[0].name` is `acme.prod.db2.dbz_config` |
| 3 | Search at the root in the UI | open `/topics`, type `dbz_config` | "No organizations match." plus an offer to search all topics |
| 4 | Take the offer | click **Search all topics instead** | URL gains `all=1`; the row appears |
| 5 | Verify the row keeps its path | inspect the row | Rendered `acme.prod.db2 / dbz_config`, linking to the topic |
| 6 | Verify the mode is shareable | open `/topics?all=1&q=dbz_config` in a fresh tab | The same single row, no further interaction needed |
| 7 | Leave flat mode | click **back to the tree** | The hierarchical view returns |

## Expected Results

### Primary Verification Points

1. A search at a group level that cannot match offers flat mode rather than dead-ending.
2. Flat mode finds a topic by any part of its full dotted name.
3. `?all=1&q=` reproduces the flat result on a fresh load.

### Secondary Verification Points

4. Flat rows carry their connector path, since no breadcrumb stands above them.
5. Leaving flat mode returns to the tree branch the user came from.
6. The offer is not shown at the connector level (depth 3), where the tree already matches topic names.

## Test Data

```json
{
  "cluster": "demo",
  "tree_at_root": { "prefix": "", "depth": 0, "level": "group", "total": 0 },
  "flat": {
    "total": 1,
    "items": [{ "name": "acme.prod.db2.dbz_config", "partitions": 1 }]
  }
}
```

## Post-conditions

1. No change (read-only).

## Cleanup Steps

1. None.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Flat mode replaces the tree as the landing view | Low | Medium | `?all=1` is absent by default; step 7 asserts the tree returns |
| The term is lost when switching mode | Medium | Low | Step 4 asserts the row appears without retyping |
| A cluster with many topics makes flat search slow | Medium | Medium | Server-paged with the same 50-row window as every other list; `total` reports the rest |

## Dependencies

- `acme.prod.db2.dbz_config` seeded (`e2e/scripts/seed.sh`).
- `list_topics` search (#29–#31), unchanged by this case.

## Notes

- The other seeded topics are flat names, so only `acme.prod.db2.dbz_config` exercises the `org.env.conn / topic` rendering. A second dotted topic would also cover ordering across paths.
