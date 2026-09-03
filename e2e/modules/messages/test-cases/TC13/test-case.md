# Test Case: MSG-013

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-013 |
| **Test Case Title** | A Decoded Payload Reads As A Tree, And Headers As Rows |
| **Test Type** | Functional, Positive, Boundary |
| **Priority** | High |
| **Estimated Duration** | 6-9 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-08-31 |
| **Last Modified** | 2026-08-31 |

## Test Objective

Verify that an expanded record shows its key and value as a collapsible,
type-coloured tree that can be searched from the inside, that every header is one
row of a table, and that an oversized payload does not lock the tab.

## Requirements Traceability

- **User Story**: As a user who has found the record I was looking for, I want to read its payload and its headers, so that the last step of an investigation is not scrolling a wall of text.
- **Requirement ID**: MSG-REQ-013 (Payload tree and headers table, #103)
- **Business Rules**:
  - The tree engages only on a structured payload. A `hex` or `utf8` field is one scalar and keeps the flat rendering — a tree around it is a worse `<pre>`.
  - Objects and arrays are collapsed past depth 2, and a closed node states what is behind the fold: `{…} 12 keys` / `[…] 340 items`.
  - Search inside the payload matches by **key or scalar value**, case-insensitively, and opens every node on the way to a match. A node the reader had collapsed by hand reopens — the new needle is the more specific intent. It composes with the server-side `value_contains`: the server finds the record, the tree finds the field.
  - A container that matched by its own **key** is highlighted but not opened: the match is the node, and its contents did not match.
  - Copy path yields a JSONPath that parses back — bracket notation whenever dot notation would name a different node (`$["user.id"]`, not `$.user.id`).
  - A decode error (`FieldValue.error`) is rendered independently of whatever decoded. The tree must never swallow it.
  - Above 256 KB of JSON the field renders collapsed-only with an **Expand anyway** escape. The guard is a default, not a refusal.
  - Raw / pretty sits with the two serializer controls in the search form, not in each row — it is a per-topic rendering choice, not a per-record one — and is remembered with them in `localStorage` under `kotatsu:fmt:{topic}` (#32).
  - The first match, in document order, is scrolled into view. Opening the way to a match is not enough on a payload taller than the viewport; later matches do not scroll, or the view would jump between them.
  - Every header is one table row, with the same decode badge treatment as key/value.

## Preconditions

1. **System State**: Stack up (`docker compose up -d`), source connected, seed applied.
2. **Data**: `nested` — 2 records with a CDC-shaped envelope (`op` / `source` / `before` / `after`). `avro-nested` — 1 Avro record three levels deep. `headers` — 2 records: one with two headers, one with none.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Open a nested payload | `/topics/nested`, From `earliest`, Search, click the first row | `value` renders as a tree, not a `<pre>`, and the badge reads `json` |
| 2 | Check the fold | same row | depth 0 and 1 are open, so `op` / `source` / `before` / `after` are all visible. The folds are at depth 2: `after.tags` reads `[…] 3 items`, `after.meta` reads `{…} 2 keys` |
| 2b | The same for Avro | `/topics/avro-nested`, Search, open the row | badge `avro #n`; `customer.address` is folded at depth 2. `avro_to_json` flattens a union and nests a record, so the tree must fold what that produces, not only hand-written JSON |
| 3 | Check the colours | same row | strings, numbers, booleans and `null` are visually distinct, using the palette already in `layouts/default.vue` — no new colours |
| 4 | Open a node | click a `{…}` summary | it expands; clicking the caret closes it again |
| 4b | A search outranks a manual collapse | close `after`, then search a value inside it | it reopens. A counted match with nowhere to be seen would make the count a lie |
| 5 | Search inside | type `ops` in `find in payload` | the match count appears, the fold on the way to `$.after.meta.by` opens, the match is highlighted, **and the view scrolls to it** |
| 5b | Only the first match scrolls | search a value that occurs several times | the view lands on the first in document order and does not jump between them |
| 6 | Search by key | type `tags` | the key matches, not only values |
| 7 | No match | type `zzz` | `0 matches`, and nothing is force-opened |
| 8 | Copy path | hover a leaf, click **path** | the clipboard holds a JSONPath naming that leaf; a key containing a dot comes back bracketed |
| 9 | Copy subtree | hover a container, click **subtree** | the clipboard holds that node's pretty JSON |
| 10 | Clipboard refused | deny clipboard permission, retry | `copy failed` is shown rather than silence (#65) |
| 11 | Scalar field | `/topics/orders`, open a row | the `key` field is flat text, no tree, no `{…}` |
| 12 | Decode error | a record whose value fails to decode | the `⚠` line is visible **and** whatever decoded is still shown |
| 13 | Raw toggle | tick **raw JSON** in the search form | both fields become pretty-printed JSON; the search box disappears, where it would do nothing |
| 14 | Raw persists | reload the page | **raw JSON** is still ticked, before any search |
| 15 | Large payload | a record over 256 KB | collapsed only, with the size stated and an **Expand anyway** button; the tab stays responsive |
| 16 | Expand anyway | click it | the tree renders and the search box returns |
| 17 | Headers table | `/topics/headers`, open the first record | a real table, **two** rows, keys `trace` and `span` |
| 18 | Line break / binary value | covered by `HeadersTable.spec.ts` — unreachable from the seed, see Risk Assessment | a value holding `\n` is one row; a binary value shows a `hex` badge, not mojibake |
| 19 | No headers | open the second record | no headers table at all |

## Expected Results

### Primary Verification Points

1. A CDC envelope is legible without scrolling a wall of text.
2. A field can be located inside a payload the server has already narrowed to a record.
3. A copied path is a valid JSONPath for the node it came from.
4. A header containing a newline is one header.
5. An oversized record does not freeze the tab.

## Test Data

```json
{
  "nested": { "records": 2, "shape": "op / source / before / after with a nested meta" },
  "headers": { "records": 2, "with_headers": 1, "header_count": 2, "line_break_in_value": 1 },
  "guard_threshold_bytes": 262144
}
```

## Post-conditions

1. No data is modified — every step is a read.
2. `localStorage` holds `kotatsu:fmt:nested` with a `raw` field.

## Cleanup Steps

1. Clear `kotatsu:fmt:*` if the raw preference is in the way of a later case.

## Risk Assessment

- `e2e/scripts/seed.sh` creates `nested` and `headers`, the latter with `--property parse.headers=true` (Kafka 3.1+). kcat is the obvious tool and cannot be used: librdkafka's `ApiVersionRequest` is not answered by the Tansu fork, so it never reaches the broker.
- That producer reads one record per line and is text-only, which puts **both** of the interesting header values out of reach: `readLine` eats a carriage return as readily as a newline (it fails outright with *No headers delimiter found*), and a non-UTF-8 byte cannot be typed into it at all. Both cases are therefore covered where they can be: `frontend/test/components/HeadersTable.spec.ts`. What the seed proves is that the table renders against a real broker, where a joined `<pre>` used to be.
- Automated equivalent: `JsonTree` is unit-tested in `frontend/test/components/JsonTree.spec.ts`, the pure helpers in `frontend/test/field.spec.ts`, and the four browser cases in `e2e/ci/smoke.spec.ts`.

## Notes

- Step 12 is the one worth being strict about. Wrapping the tree render in a `try/catch` is the natural way to make it robust, and it silently swallows the decode error — the single thing the reader most needs to see.
- Step 4b is the one that is easy to get backwards. Honouring a manual collapse over a later search feels respectful and is wrong: the match count would report matches the reader cannot reach.
- Search-in-payload and `value_contains` are different tools and both are needed: the server cannot tell you *which field* matched, and the browser cannot search records it has not fetched.
