# Test Case: MSG-010

## Test Case Information

| Field | Value |
|-------|-------|
| **Test Case ID** | MSG-010 |
| **Test Case Title** | Filter Messages by Record Headers (`header_key`, `header_value`) |
| **Test Type** | Functional, Integration |
| **Priority** | Medium |
| **Estimated Duration** | 4-5 minutes |
| **Created By** | QA Specialist |
| **Created Date** | 2026-07-29 |
| **Last Modified** | 2026-07-29 |

## Test Objective

Verify that records carrying Kafka headers are decoded with their headers, and
that `header_key` (optionally paired with `header_value`) filters records by
header. Confirm the coupling rule: `header_value` has no effect unless
`header_key` is also supplied.

## Requirements Traceability

- **User Story**: As a user, I want to filter messages by header so that I can isolate records tagged with a given attribute.
- **Requirement ID**: MSG-REQ-010 (Header decoding & filtering)
- **Business Rule**: Each record exposes `headers[]` as `{key, value}` pairs; `header_key=X` matches records having a header keyed `X`; adding `header_value=Y` requires the value to equal `Y`; `header_value` alone is ignored (`filtered: false`).

## Preconditions

1. **System State**: Stack up; source connected.
2. **Test Data**: topic `orders-hdr` — 2 records with headers:
   - offset 0: headers `source=web`, `region=eu`; key `hk-1`; value `{"id":10,"item":"boxed"}`
   - offset 1: headers `source=mobile`, `region=us`; key `hk-2`; value `{"id":11,"item":"crate"}`
3. **Environment**: Base URL `http://localhost:8080`; `curl`.

## Test Steps

| Step | Action | Input Data | Expected Result |
|------|--------|------------|-----------------|
| 1 | Create `orders-hdr` and produce 2 records with headers | see Reference commands | Topic populated |
| 2 | Read and verify headers decode | `?offset=earliest` | Each record has `headers: [{key:{data,kind}, value:{data,kind}}]` matching the produced headers |
| 3 | Filter by header key | `?offset=earliest&header_key=source` | `filtered: true`, `count: 2` (both have `source`) |
| 4 | Filter by key+value | `?offset=earliest&header_key=source&header_value=mobile` | `filtered: true`, `count: 1`, offset 1 |
| 5 | Filter by key+value (other) | `?offset=earliest&header_key=region&header_value=us` | `count: 1`, offset 1 |
| 6 | Nonexistent header key | `?offset=earliest&header_key=nokey` | `filtered: true`, `count: 0` |
| 7 | **Value without key (gotcha)** | `?offset=earliest&header_value=web` | `filtered: false`, `count: 2` (no filtering applied) |
| 8 | UI header filter | use the header filter in the event browser | Matches the API, including the value-needs-key rule |

## Expected Results

### Primary Verification Points

1. Headers decode as `{key, value}` pairs with `kind` per part.
2. `header_key=X` returns only records having header key `X`; `filtered: true`.
3. `header_key=X&header_value=Y` requires the exact value; a non-match returns `count: 0`.
4. **`header_value` supplied alone does nothing** — `filtered: false`, all records returned.

### Secondary Verification Points

5. UI header filtering matches the API, including the coupling rule.

## Test Data

```json
{
  "topic": "orders-hdr",
  "records": [
    { "offset": 0, "headers": { "source": "web",    "region": "eu" }, "key": "hk-1" },
    { "offset": 1, "headers": { "source": "mobile", "region": "us" }, "key": "hk-2" }
  ],
  "filters": [
    { "query": "header_key=source",                 "expect": { "count": 2, "filtered": true } },
    { "query": "header_key=source&header_value=mobile", "expect": { "count": 1, "offsets": [1], "filtered": true } },
    { "query": "header_key=region&header_value=us",  "expect": { "count": 1, "offsets": [1] } },
    { "query": "header_key=nokey",                   "expect": { "count": 0, "filtered": true } },
    { "query": "header_value=web",                   "expect": { "count": 2, "filtered": false } }
  ]
}
```

### Reference commands

```bash
docker run --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 \
  --create --topic orders-hdr --partitions 1 --replication-factor 1

printf 'source:web,region:eu|hk-1#{"id":10,"item":"boxed"}\nsource:mobile,region:us|hk-2#{"id":11,"item":"crate"}\n' | \
  docker run -i --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server tansu:9092 --topic orders-hdr \
  --property parse.headers=true --property parse.key=true \
  --property headers.delimiter='|' --property headers.separator=',' \
  --property headers.key.separator=':' --property key.separator='#'
```

## Post-conditions

1. Topic `orders-hdr` holds 2 records with headers.

## Cleanup Steps

1. None, or delete `orders-hdr`.

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Producer header syntax mis-parsed | Medium | Medium | Use the explicit separators in Reference commands; verify step 2 before filtering |
| Expecting `header_value` alone to filter | High | Medium | Documented gotcha: pair it with `header_key` |

## Dependencies

- Kafka console producer with `parse.headers=true` support.

## Notes

- **Key learning:** `header_value` is only honoured together with `header_key`; on its own it is ignored (`filtered: false`, all records returned). This is the primary regression risk for header filtering.
