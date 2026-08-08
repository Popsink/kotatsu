# Kotatsu — Smoke Test Plan

## Context

A lightweight, sequential smoke test that catches broad regressions (source
down, cluster not discovered, topic/message read broken, schema registry down,
groups broken) without the depth of the per-module ISTQB cases. Run it after
every build of the image or change to the read path.

All data is **fictitious** (`orders`, `events`, `avro-orders`, `truncated`,
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
mkdir -p /tmp/dr && printf '{"partitions":[{"topic":"truncated","partition":0,"offset":2}],"version":1}\n' >/tmp/dr/delete-records.json
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
| 2 | Source connected | `GET /api/source` | `configured: true`, `status.connected: true` | SRC-001 |
| 3 | Cluster discovered | `GET /api/clusters` | `clusters` contains `demo` | SRC-001 |
| 4 | Topics listed | `GET /api/clusters/demo/topics` | `orders`, `events`, `empty-topic`, `avro-orders`, `truncated`, `acme.prod.db2.dbz_config` present | TOP-001 |
| 5 | Topic detail | `GET /api/clusters/demo/topics/events` | 3 partitions; `messages: 6` | TOP-002 |
| 6 | Read messages | `GET .../topics/orders/messages?offset=earliest` | `count: 3`, watermark `{0,3}` | MSG-001 |
| 7 | Empty topic | `GET .../topics/empty-topic/messages` | `count: 0`, `exhausted: true` | MSG-005 |
| 8 | Avro decode | `GET .../topics/avro-orders/messages?offset=earliest` | values `kind: "avro"`, decoded `{id,item}` | SCH-001 |
| 9 | Schemas listed | `GET /api/schemas` | contains `avro-orders-value` | SCH-001 |
| 10 | Groups + lag | `GET /api/clusters/demo/groups/qa-group` | `committed_offset: 3`, `lag: 0`, `total_lag: 0` | GRP-002 |
| 11 | Compacted topic routing | `GET .../topics/acme.prod.db2.dbz_config/messages?offset=earliest` | `count: 2`, watermark `{0,2}`, keys `cfg-a`/`cfg-b`; detail `storage_bytes > 0` | #92 |
| 12 | Truncation floor | `GET .../topics/truncated/messages?offset=earliest` and `?offset=0` | watermark `{2,3}`, `count: 1`, only offset 2 — the deleted records are not served | #95 |

## Pass/Fail

- **Pass**: all 12 steps meet their expected result; no 5xx; UI at `http://localhost:8080` loads and shows the topics.
- **Fail**: any step deviates → open the corresponding per-module ISTQB case to isolate.

## Tear down

```bash
docker compose down -v
```
