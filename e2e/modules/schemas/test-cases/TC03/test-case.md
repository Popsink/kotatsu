# Test Case: SCH-003

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | SCH-003 |
| **Test Case Title** | Compare Two Versions Of A Subject, And Reach That Diff From A Decoded Record |
| **Test Type** | Functional, Integration |
| **Priority** | Medium |
| **Estimated Duration** | 5-6 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-09-02 |
| **Last Modified** | 2026-09-02 |

## Test Objective

Verify that two versions of a subject can be diffed on one screen, that a change
confined to **key order** renders as no change at all, that added / removed /
type-changed / default-changed fields are distinguished, and that the `↗ schema`
link on a decoded record lands on the version that record was written with.

## Requirements Traceability

- **User Story**: As a user, I want to see what changed between two versions of a schema so that I can tell why a record fails to decode.
- **Requirement ID**: SCH-REQ-003 (Version diff)
- **Business Rule**: The diff is a **text diff over canonicalised JSON** — object keys are sorted before comparing, so key order is not a change; a record's `fields` **array order is preserved**, because in Avro it is the wire layout and reordering it is a real change. Field annotations cover top-level record fields only. Kotatsu never registers, evolves or deletes a schema.

## Preconditions

1. **System State**: Stack up; source connected; Kora reachable.
2. **Test Data**: subject `diff-demo-value` with two versions, seeded by `e2e/scripts/seed.sh`:
   - **v1** — `{id: int, item: string}`
   - **v2** — `{id: int, item: ["null","string"] default null, note: string default ""}`, with its top-level keys written in a **different order** from v1.
   Subject `avro-orders-value` with records in `avro-orders` (from SCH-001).
3. **Environment**: Base URL `http://localhost:8080`; `curl`; a browser.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Both versions exist | `GET /api/schemas/diff-demo-value` | `versions: [1, 2]`, `latest.version: 2` |
| 2 | Open the subject | browse `/schemas/diff-demo-value` | v2 shown pretty-printed; **Compare with** unchecked; the selector offers 2 and 1 |
| 3 | Enter compare mode | tick **Compare with**, leave the selector on `1` | Header reads `v1 → v2`, `changed`, and the subject's compatibility level |
| 4 | Field annotations | inspect the change list | `item` — **type changed**, `string → null \| string`; `note` — **added** |
| 5 | The diff itself | inspect the diff block | Added lines green with `+`, removed lines red with `-`, unchanged lines plain; two line-number gutters, one per side |
| 6 | Key order is not a change | note that v2 declares `name` before `type` while v1 does the opposite | Neither `name` nor `type` appears as a changed line — canonicalisation sorted them before comparing |
| 7 | Compare a version with itself | set the selector to `2` | Reads `identical`, and the diff block is replaced by a plain statement |
| 8 | Single-version subject | browse `/schemas/avro-orders-value` | **Compare with** is disabled, with `— only one version` beside it |
| 9 | Resolve a record's schema id | `GET /api/schemas/ids/{id}/versions` with `id` from `GET /api/schemas/avro-orders-value` | `versions` contains `{subject: "avro-orders-value", version: <latest>}` |
| 10 | Unknown id | `GET /api/schemas/ids/999999/versions` | HTTP 404 with a message naming the id, not an empty list |
| 11 | Link from a decoded record | open `/topics/avro-orders`, Search, expand a record, hover `↗ schema` | The href is `/schemas/avro-orders-value?id=<the record's schema id>` |
| 12 | Follow the link | click it | Lands on the subject page; because that record used the version in force, compare stays **off** |
| 13 | Follow it when the record is behind | register a new version of `avro-orders-value`, reload, click `↗ schema` again | Compare is **on**, `from` is the record's version and `to` is the new latest |

## Expected Results

### Primary Verification Points

1. Two versions of a subject are diffed on one screen.
2. A pure key-reordering renders as no change (step 6).
3. Added, removed, type-changed and default-changed fields are visually distinguished.
4. `↗ schema` carries the record's schema id and the page resolves it to a version.

### Secondary Verification Points

5. A subject with one version disables compare rather than offering a broken control.
6. Comparing a version with itself says so instead of rendering an empty diff.
7. An unknown schema id is a 404.

## Test Data

```json
{
  "subject": "diff-demo-value",
  "v1": { "type": "record", "name": "Demo", "fields": [ {"name":"id","type":"int"}, {"name":"item","type":"string"} ] },
  "v2": { "name": "Demo", "type": "record", "fields": [ {"name":"id","type":"int"}, {"name":"item","type":["null","string"],"default":null}, {"name":"note","type":"string","default":""} ] },
  "expected_field_changes": [
    { "name": "item", "kind": "type", "from": "string", "to": "null | string" },
    { "name": "item", "kind": "default", "from": "∅", "to": "null" },
    { "name": "note", "kind": "added" }
  ]
}
```

## Post-conditions

1. If step 13 was run, `avro-orders-value` has an extra version and its `latest` has moved.

## Cleanup Steps

1. `docker compose down -v` and re-seed if a later case needs `avro-orders-value` back at one version.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Step 13's registration is rejected by the compatibility level | Medium | Low | Evolve compatibly (add a field with a default), or set the subject to `NONE` first |
| Kora unreachable → the page shows a registry error rather than a diff | Low | Medium | Check `GET /api/schemas` returns 200 before starting |
| A very large schema makes the diff slow | Low | Low | The diff is O(n × m) in lines; schemas of a few hundred lines are unnoticeable |

## Dependencies

- `diff-demo-value` seeded with two versions (`e2e/scripts/seed.sh`).
- Avro records in `avro-orders` and their subject (SCH-001).

## Notes

- Steps 1-8 and 11 are automated in `e2e/ci/smoke.spec.ts`; steps 12-13 are manual because 13 mutates the registry, which the seed deliberately leaves in a fixed state.
- The point of step 6 is easy to lose: the two seeded versions differ in key order **on purpose**. If that ever shows up as a changed line, the canonicalisation has regressed and every diff on the page becomes noise.
