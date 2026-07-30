# Kotatsu — E2E Test Cases (semi-manual)

Manual / semi-manual end-to-end test cases for Kotatsu, written in **ISTQB**
format. Each case is a `test-case.md` under its module, exercised against a
locally running stack (Tansu → S3 → Kotatsu).

> **Data policy:** all test data is **fictitious and generic** (`orders`,
> `widget`, `key-1`, …). Never use real customer names, real business data,
> PII, credentials or internal identifiers — this repository is public.

## Layout

```
e2e/
├── README.md
├── ci/                    # automated Playwright smoke (run in CI)
├── scripts/
│   └── seed.sh            # seed fictitious demo data (local + CI)
├── reports/               # execution reports + screenshots
├── test_plans/
│   └── SMOKE_TEST_PLAN.md
└── modules/
    ├── source/     # S3 connection & source status        (prefix SRC)
    ├── topics/     # topic listing, detail, watermarks     (prefix TOP)
    ├── messages/   # record read-back, pagination, filters (prefix MSG)
    ├── schemas/    # Avro decode, Kora registry            (prefix SCH)
    ├── groups/     # consumer groups, offsets, lag         (prefix GRP)
    ├── navigation/ # hierarchical topic-tree browsing      (prefix NAV)
    └── health/     # health probe, source meta, stats      (prefix HLT)
```

Each module holds `test-cases/TCxx/test-case.md`. Test Case IDs use the module
prefix and a running number (e.g. `MSG-001`).

## Prerequisites

- Docker (daemon running)
- A Kafka client image for producing test data (`apache/kafka:latest`)
- A browser for the UI checks; `curl` for the API checks

## Bring up the stack

```bash
# from the repo root
docker compose up --build -d
docker compose ps          # all services Up
```

Endpoints:

| Service            | URL                              | Notes                         |
| ------------------ | -------------------------------- | ----------------------------- |
| Kotatsu UI + API   | http://localhost:8080            | UI + `/api/*`                 |
| MinIO console      | http://localhost:9001            | `minioadmin` / `minioadmin`   |
| Tansu broker       | localhost:9092                   | cluster `demo`                |
| Kora registry      | http://localhost:8085            | Confluent-compatible          |

The active cluster is `demo` (see `KOTATSU_CLUSTER` in `docker-compose.yml`).

## Produce fictitious test data

```bash
# create a topic
docker run --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 \
  --create --topic orders --partitions 1 --replication-factor 1

# produce a few keyed JSON records
printf 'key-1:{"id":1,"item":"widget"}\nkey-2:{"id":2,"item":"gadget"}\n' | \
  docker run -i --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server tansu:9092 \
  --topic orders --property parse.key=true --property key.separator=:
```

## API quick reference (verification)

| Endpoint                                             | Purpose                     |
| ---------------------------------------------------- | --------------------------- |
| `GET /api/health`                                    | service health             |
| `GET /api/source`                                    | S3 source + connection      |
| `GET /api/clusters`                                  | discovered clusters         |
| `GET /api/clusters/{cluster}`                        | cluster stats               |
| `GET /api/clusters/{cluster}/topic-tree`             | hierarchical topic tree     |
| `GET /api/clusters/{cluster}/topics`                 | topic list                  |
| `GET /api/clusters/{cluster}/topics/{topic}`         | topic detail                |
| `GET /api/clusters/{cluster}/topics/{topic}/messages`| records + watermarks        |
| `GET /api/clusters/{cluster}/groups`                 | consumer groups             |
| `GET /api/clusters/{cluster}/groups/{group}`         | group detail                |
| `GET /api/schemas`                                   | registered subjects         |
| `GET /api/schemas/{subject}`                         | subject detail + versions   |
| `GET /api/schemas/{subject}/versions/{version}`      | one schema version          |

## Tear down

```bash
docker compose down -v      # -v also removes MinIO/Kora volumes (clean slate)
```
