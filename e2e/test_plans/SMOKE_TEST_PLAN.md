# Kotatsu — Smoke Test Plan

## Context

A lightweight, sequential smoke test that catches broad regressions (source
down, cluster not discovered, topic/message read broken, schema registry down,
groups broken) without the depth of the per-module ISTQB cases. Run it after
every build of the image or change to the read path.

All data is **fictitious** (`orders`, `events`, `spread`, `avro-orders`, `truncated`,
`acme.prod.db2.dbz_config`, `qa-group`).

## Scope

| In scope | Out of scope |
|----------|--------------|
| Health, source connectivity, cluster/topic/message read, Avro decode, groups, storage-contract cases (pinned routing prefix, truncation floor) | Load/perf, auth, multi-source, failure injection (see SRC-003) |

## Preconditions

- `docker compose up --build -d` — all services `Up`. The broker must be the
  **Popsink Tansu fork** at the version pinned in `docker-compose.yml`: upstream
  writes a different storage layout, so steps 11–12 below cannot pass against it
  and the rest would pass without proving anything about production (#97).
- Kafka client image `apache/kafka:latest` and `confluentinc/cp-schema-registry:7.6.0` available.
- Base URL: `http://localhost:8080`; cluster: `demo`.

## Seed data (once)

`bash e2e/scripts/seed.sh` does all of this; the commands below are what it runs.

```bash
NET=kafka() { docker run --rm --network kotatsu_default apache/kafka:latest "$@"; }

# topics
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 --create --topic orders      --partitions 1 --replication-factor 1
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 --create --topic events      --partitions 3 --replication-factor 1
# Keyed records, so they actually land in different partitions — `events` is
# keyless and the sticky partitioner puts all six in one (#102).
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 --create --topic spread      --partitions 3 --replication-factor 1
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 --create --topic empty-topic --partitions 1 --replication-factor 1
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 --create --topic avro-orders --partitions 1 --replication-factor 1
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 --create --topic truncated   --partitions 1 --replication-factor 1
# Compacted ⇒ the broker routes it under its own full name, not under `acme.prod.db2`
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 --create --topic acme.prod.db2.dbz_config --partitions 1 --replication-factor 1 --config cleanup.policy=compact

# records
printf 'key-1:{"id":1,"item":"widget"}\nkey-2:{"id":2,"item":"gadget"}\nkey-3:{"id":3,"item":"gizmo"}\n' | \
  docker run -i --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server tansu:9092 --topic orders --property parse.key=true --property key.separator=:

printf '{"n":1}\n{"n":2}\n{"n":3}\n{"n":4}\n{"n":5}\n{"n":6}\n' | \
  docker run -i --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server tansu:9092 --topic events

printf 'k-1:{"n":1}\nk-2:{"n":2}\nk-3:{"n":3}\nk-4:{"n":4}\nk-5:{"n":5}\nk-6:{"n":6}\nk-7:{"n":7}\nk-8:{"n":8}\nk-9:{"n":9}\nk-10:{"n":10}\nk-11:{"n":11}\nk-12:{"n":12}\n' | \
  docker run -i --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server tansu:9092 --topic spread --property parse.key=true --property key.separator=:

printf '{"id":1,"item":"widget"}\n{"id":2,"item":"gadget"}\n' | \
  docker run -i --rm --network kotatsu_default confluentinc/cp-schema-registry:7.6.0 \
  kafka-avro-console-producer --bootstrap-server tansu:9092 --topic avro-orders \
  --property schema.registry.url=http://kora:8080 \
  --property value.schema='{"type":"record","name":"Order","fields":[{"name":"id","type":"int"},{"name":"item","type":"string"}]}'

printf 'cfg-a:{"connector":"db2","state":"RUNNING"}\ncfg-b:{"connector":"db2","state":"PAUSED"}\n' | \
  docker run -i --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server tansu:9092 --topic acme.prod.db2.dbz_config --property parse.key=true --property key.separator=:

# 3 records into `truncated`, then a logical DeleteRecords below offset 2
printf '{"n":1}\n{"n":2}\n{"n":3}\n' | \
  docker run -i --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server tansu:9092 --topic truncated
mkdir -p /tmp/dr && chmod 755 /tmp/dr   # the kafka image runs as non-root and must read the mount
printf '{"partitions":[{"topic":"truncated","partition":0,"offset":2}],"version":1}\n' >/tmp/dr/delete-records.json
docker run --rm --network kotatsu_default -v /tmp/dr:/dr apache/kafka:latest \
  /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server tansu:9092 --offset-json-file /dr/delete-records.json

# consumer group with committed offsets
docker run --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server tansu:9092 \
  --topic orders --from-beginning --group qa-group --max-messages 3 --timeout-ms 8000
```

## Smoke steps

| # | Step | Endpoint / Action | Expected | Ref case |
|---|------|-------------------|----------|----------|
| 1 | Service health | `GET /api/health` | `{"service":"kotatsu","status":"ok"}` | — |
| 2 | Source configured | `GET /api/source` | `configured: true`, `cluster: demo`, no `status` key | SRC-001 |
| 2b | Source reachable | `GET /api/source/status` | `connected: true` | SRC-001 |
| 3 | Cluster discovered | `GET /api/clusters` | `clusters` contains `demo` | SRC-001 |
| 4 | Topics listed | `GET /api/clusters/demo/topics` | `orders`, `events`, `spread`, `nested`, `headers`, `empty-topic`, `avro-orders`, `avro-nested`, `truncated`, `acme.prod.db2.dbz_config` present | TOP-001 |
| 5 | Topic detail | `GET /api/clusters/demo/topics/events` | 3 partitions; `messages: 6` | TOP-002 |
| 6 | Read messages | `GET .../topics/orders/messages?partition=0&offset=earliest` | `count: 3`, watermark `{0,3}` | MSG-001 |
| 6b | Search all partitions | `GET .../topics/spread/messages?partition=all&offset=earliest` | records from every populated partition, oldest first, `order: timestamp_asc` | MSG-011 |
| 6c | Page through a topic | `GET .../topics/spread/messages?partition=all&offset=earliest&limit=5`, then the same with `cursor=` from `partitions[].resume` | the two pages share no record and paging to the end accounts for all 12 | MSG-012 |
| 6c | Topic-wide scan budget | same with `&max_scan=6` | `scanned` ≤ budget + partition count, not budget × partitions | MSG-011 |
| 7 | Empty topic | `GET .../topics/empty-topic/messages?partition=0` | `count: 0`, `exhausted: true` | MSG-005 |
| 8 | Avro decode | `GET .../topics/avro-orders/messages?partition=0&offset=earliest` | values `kind: "avro"`, decoded `{id,item}` | SCH-001 |
| 9 | Schemas listed | `GET /api/schemas` | contains `avro-orders-value` | SCH-001 |
| 10 | Groups + lag | `GET /api/clusters/demo/groups/qa-group` | `committed_offset: 3`, `lag: 0`, `total_lag: 0` | GRP-002 |
| 10b | Lag is opt-in | `GET /api/clusters/demo/groups` | rows carry no `lag` key at all | GRP-003 |
| 10c | Lag in the listing | `GET /api/clusters/demo/groups?lag=true` | `qa-group` carries `lag: {total: 0, topics: 1, max_partition: 0}` | GRP-003 |
| 11 | Compacted topic routing | `GET .../topics/acme.prod.db2.dbz_config/messages?partition=0&offset=earliest` | `count: 2`, watermark `{0,2}`, keys `cfg-a`/`cfg-b`; detail `storage_bytes > 0` | #92 |
| 12 | Truncation floor | `GET .../topics/truncated/messages?partition=0&offset=earliest` and `?partition=0&offset=0` | watermark `{2,3}`, `count: 1`, only offset 2 — the deleted records are not served | #95 |
| 13 | Payload tree | open a `nested` record in the event browser | the value opens as a tree collapsed past depth 2, and `find in payload` = `4711` opens down to the match | MSG-013 |
| 14 | Headers table | open the first `headers` record | **two** rows, `trace`/`abc123` and `span`/`d4e5f6` — one per header, not a joined block | MSG-013 |
| 15 | Headers absent | open the second `headers` record | no headers table at all | MSG-013 |
| 16 | Nested Avro folds | open the `avro-nested` record | a `{…} 2 keys` fold at depth 2, and `find in payload` = `paris` opens through it | MSG-013 |
| 17 | Flat topic search | open `/topics`, search `dbz_config` at the root | no organizations match, and taking the offer lists `acme.prod.db2 / dbz_config` under `?all=1` | NAV-002 |
| 18 | Quick-jump palette | `Ctrl-K` on `/groups`, type `avro-orders` | Topics and Schemas sections; `Enter` opens the first, `Esc` closes without touching the page | NAV-003 |
| 18b | Record size | `GET .../topics/orders/messages?partition=0&offset=earliest` | offset 0 carries `size: 29` — `key-1` plus its 24-byte value | MSG-014 |
| 19 | Newest first, and flipped | `/topics/spread`, `From: latest`, Search | the highest offset is the first row; the order button turns the loaded window around without re-reading | MSG-014 |
| 20 | Unambiguous timestamps | open the first `orders` record row | the timestamp ends in a zone offset; switching `Time` to `utc` ends it in `UTC` | MSG-014 |
| 21 | Column choice persists | `/topics/orders`, Columns ▾, tick `size`, reload | the `size` header is still there after the reload | MSG-014 |
| 22 | Per-partition size | `/topics/orders` partition table | a `size` column, non-zero for a partition holding records | #76 |
| 22b | Cells under their headers | `/topics/orders`, tick `size`, Search | the `data-col` sequence of the row equals that of the header — no column renders under a neighbour's heading | #108 |
| 23 | Topic heading spacing | `/topics/orders` | the heading reads `Topic orders on demo` — a space before `on`, in the text and not only in the layout | #108 |
| 24 | Tree from the keyboard | focus the `acme` row's link on `/topics`, `Enter` | the row navigates to `?p=acme` — a `<tr>` alone took neither focus nor `Enter` | ACC-001 |
| 25 | Message from the keyboard | focus a row's disclosure button, `Enter` | the detail row opens and the button flips from `Expand offset n` / `aria-expanded=false` to `Collapse` / `true` | ACC-001 |
| 26 | Focus is visible | `Tab` once on `/topics` | the focused element has a non-zero `outline-width` | ACC-001 |
| 27 | Theme persists | tick the `light` radio, reload, then tick `system` | `<html data-theme="light">` survives the reload; `system` removes the attribute | ACC-002 |
| 27b | Burger menu | viewport 600×900 on `/topics` | the nav is `display: none` and the burger reads `aria-expanded=false`; a click opens it, `Escape` closes it, and following a link closes it too | ACC-003 |
| 27c | No phantom control | `/topics` at desktop width | the burger is absent from the accessibility tree, and the nav links are visible | ACC-003 |
| 28 | 600 px viewport | `/topics/orders`, Search, viewport 600×900 | the table scrolls inside its own box and `scrollWidth == clientWidth` on the document | ACC-003 |
| 29 | Follow is gated | `/topics/orders`, `From: earliest` then `latest` | the Follow button is absent on the historical read and present on the live-edge one | MSG-015 |
| 30 | Nothing polls when off | search, then idle 6 s | no further `/messages` request leaves the page | MSG-015 |
| 31 | Follow spends visibly, then stops | arm Follow at 2 s on `orders` | `following · n polls · …` appears, requests increase, and after three quiet polls it disarms saying `nothing new`; no request after that | MSG-015 |

## Pass/Fail

- **Pass**: all 31 steps meet their expected result; no 5xx; UI at `http://localhost:8080` loads and shows the topics.
- **Fail**: any step deviates → open the corresponding per-module ISTQB case to isolate.

## Tear down

```bash
docker compose down -v
```
