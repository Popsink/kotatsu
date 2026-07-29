# Test Case: NAV-001

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | NAV-001 |
| **Test Case Title** | Browse Topics via the Hierarchical Topic Tree |
| **Test Type** | Functional, Integration |
| **Priority** | Medium |
| **Estimated Duration** | 3-4 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-29 |
| **Last Modified** | 2026-07-29 |

## Test Objective

Verify that the `topic-tree` endpoint returns the cluster's topics as a
navigable tree — with `path`, `segment`, `topic`/`group` flags and a per-node
topic count — and that a `prefix` narrows the view to a subtree.

## Requirements Traceability

- **User Story**: As a user, I want to browse topics in a tree so that I can navigate large, dot-namespaced topic sets.
- **Requirement ID**: NAV-REQ-001 (Topic tree navigation)
- **Business Rule**: At the root (`prefix=""`, `depth=0`) every top-level segment is returned; each node reports whether it is a leaf topic or a group and how many topics it contains.

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topics `orders`, `events`, `empty-topic`, `avro-orders` present in cluster `demo`.
3. **Environment**: Base URL `http://localhost:8080`; `curl`; browser.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Get the root tree | `GET /api/clusters/demo/topic-tree` | HTTP 200; `prefix: ""`, `depth: 0`, `total: 4` |
| 2 | Verify nodes | inspect `items` | one node per topic; each has `path`, `segment`, `topic`, `group: false`, `topics: 1` |
| 3 | Verify a leaf topic | inspect `orders` node | `topic: "orders"`, `segment: "orders"`, `path: "orders"`, `group: false` |
| 4 | Narrow with a prefix | `GET .../topic-tree?prefix=ord` | Only the `orders` subtree returned |
| 5 | Pagination envelope | inspect body | `limit: 50`, `offset: 0` present |
| 6 | UI tree | open the sidebar / tree navigation | Tree matches the API; clicking a leaf opens the topic |

## Expected Results

### Primary Verification Points

1. Root call returns one node per top-level segment with `total` equal to the number of top-level segments.
2. Each node exposes `path`, `segment`, `topic`/`group` and a `topics` count.
3. A `prefix` restricts the result to the matching subtree.

### Secondary Verification Points

4. Leaf topics carry `group: false` and a resolvable `topic` name.
5. UI tree navigation matches the API structure.

## Test Data

```json
{
  "cluster": "demo",
  "root": {
    "prefix": "",
    "depth": 0,
    "total": 4,
    "items": [
      { "segment": "avro-orders", "path": "avro-orders", "topic": "avro-orders", "group": false, "topics": 1 },
      { "segment": "orders",      "path": "orders",      "topic": "orders",      "group": false, "topics": 1 }
    ]
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
| Flat names → no real hierarchy to browse | High | Low | With dot-namespaced topics (e.g. `a.b.c`) the tree nests; the demo topics are flat so nodes are leaves at depth 0 |
| Node ordering assumptions | Medium | Low | Assert set membership, not order |

## Dependencies

- Topics present in cluster `demo`.

## Notes

- Demo topics are flat (no dots), so every root node is a leaf with `topics: 1` and `group: false`. To exercise real nesting, produce dot-namespaced topics such as `sales.orders.v1` and re-check `depth`/`prefix` traversal.
