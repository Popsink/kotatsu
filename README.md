# kotatsu

Read-only, on-demand browser over [Tansu](https://github.com/tansu-io/tansu)'s
**native S3 storage**. Topics, events, consumer groups and simple stats are read
directly from the object store Tansu writes to — **no Kafka broker, no Kafka
client, no background polling**. Every read is triggered by a user action.

Built with **Rust (Axum)** + **Nuxt 3**.

## Architecture

Tansu persists everything to S3 under a known layout (reverse-engineered from
`tansu-storage::dynostore`):

```
clusters/{cluster}/meta.json                                        topic/producer/txn metadata
clusters/{cluster}/topics/{topic}/partitions/{p:010}/watermark.json low/high offsets
clusters/{cluster}/topics/{topic}/partitions/{p:010}/records/{base_offset:020}.batch
clusters/{cluster}/groups/consumers/{group}.json                    consumer group detail
clusters/{cluster}/groups/consumers/{group}/offsets/{topic}/partitions/{p:010}.json
```

Kotatsu reads these objects via the `object_store` crate and decodes the
`.batch` files (raw Kafka record batches) with `tansu-sans-io`. Avro values are
resolved against [Kora](https://github.com/Popsink/kora) (Confluent-compatible
schema registry). See the GitHub issues for the full design.

## Project layout

```
kotatsu/
├── backend/          # Rust (Axum) — object_store + tansu-sans-io, no Kafka client
├── frontend/         # Nuxt 3 (SPA), served as static assets by the backend in prod
├── Dockerfile        # multi-stage → single image (backend serves frontend)
└── docker-compose.yml
```

## Run locally (development)

Two processes, with the frontend proxying `/api` to the backend.

```bash
# 1. backend
cd backend
cargo run            # listens on 0.0.0.0:8080

# 2. frontend (separate terminal)
cd frontend
npm install
npm run dev          # http://localhost:3000, proxies /api → http://localhost:8080
```

Environment variables (backend):

| Var                  | Default          | Purpose                                  |
| -------------------- | ---------------- | ---------------------------------------- |
| `KOTATSU_BIND`       | `0.0.0.0:8080`   | HTTP bind address                        |
| `KOTATSU_STATIC_DIR` | _(unset)_        | Dir of built frontend assets (prod only) |

S3 source variables (`KOTATSU_S3_*`) are consumed starting with the storage
layer (issue #2).

## Run with Docker

```bash
docker compose up --build
```

Starts the Kotatsu app (backend + bundled frontend) on http://localhost:8080 and
a MinIO S3 on http://localhost:9000 (console at :9001, `minioadmin`/`minioadmin`)
with a `tansu` bucket created automatically.

It also starts a **Tansu broker** (`localhost:9092`, cluster `demo`) writing to
that bucket, so you can generate real events:

```bash
# create a topic + produce a few messages with any Kafka client
docker run --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server tansu:9092 \
  --create --topic orders --partitions 1 --replication-factor 1

printf 'key-1:{"id":1}\n' | docker run -i --rm --network kotatsu_default apache/kafka:latest \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server tansu:9092 \
  --topic orders --property parse.key=true --property key.separator=:
```

The records land under `clusters/demo/topics/orders/…` in the bucket and are
read back by Kotatsu.

The stack also runs **Kora** (Confluent-compatible schema registry) on
`localhost:8085` with its own PostgreSQL; the app resolves Avro schemas via
`KOTATSU_KORA_URL=http://kora:8080`. To produce Confluent-framed **Avro** events
(schema auto-registered in Kora):

```bash
printf '{"id":1,"item":"widget"}\n' | docker run -i --rm --network kotatsu_default \
  confluentinc/cp-schema-registry:7.6.0 kafka-avro-console-producer \
  --bootstrap-server tansu:9092 --topic avro-orders \
  --property schema.registry.url=http://kora:8080 \
  --property value.schema='{"type":"record","name":"Order","fields":[{"name":"id","type":"int"},{"name":"item","type":"string"}]}'
```

Kotatsu decodes these in the event browser and lists the schema under **Schemas**.

To build the single production image on its own:

```bash
docker build -t kotatsu .
docker run -p 8080:8080 kotatsu
```

Pushed images are published to **`ghcr.io/popsink/kotatsu`** by the `release`
workflow on every push to `main` (tagged `main` + `sha`) and on `v*` tags
(semver + `latest`), built for `linux/amd64` and `linux/arm64`.

```bash
docker run -p 8080:8080 ghcr.io/popsink/kotatsu:latest
```

## Kubernetes (Helm)

A Helm chart is published as an OCI artifact to
**`oci://ghcr.io/popsink/kotatsu/charts/kotatsu`** by the `chart-release`
workflow.

```bash
helm install kotatsu oci://ghcr.io/popsink/kotatsu/charts/kotatsu --version 0.1.1 \
  --set s3.bucket=tansu \
  --set s3.cluster=demo \
  --set koraUrl=http://kora:8080
```

`s3.cluster` and `s3.bucket` are required. Provide static keys via
`s3.accessKey`/`s3.secretKey`, or omit them to use the pod's IAM role — attach
it through `serviceAccount.annotations` (EKS IRSA) or a Pod Identity
association. See [`chart/kotatsu/values.yaml`](chart/kotatsu/values.yaml) for all
options.

## Tests

```bash
cd backend
cargo test                      # unit tests (decode, keys, parsing) — no services needed
```

Integration tests under `backend/tests/` are **`#[ignore]`-gated** so CI stays
green without infrastructure. They are self-contained (they seed their own
data and clean up), needing only the relevant services running:

```bash
docker compose up -d minio createbucket kora kora-db
cargo test -- --ignored          # runs s3 / groups / schema integration tests
# or per suite:
cargo test --test groups_integration -- --ignored
cargo test --test schema_integration -- --ignored
```

## Pointing at an S3 source

Set the bucket/endpoint and the Tansu cluster name (see `docker-compose.yml`
for the variable names). A single source per instance for now; multi-source
comes later.

| Var | Purpose |
| --- | --- |
| `KOTATSU_S3_BUCKET` | bucket holding Tansu's storage |
| `KOTATSU_CLUSTER` | Tansu cluster id (`clusters/{cluster}/` prefix) |
| `KOTATSU_S3_ENDPOINT` | custom endpoint (MinIO/R2); omit for AWS |
| `KOTATSU_S3_REGION` | region (default `us-east-1`) |
| `KOTATSU_S3_FORCE_PATH_STYLE` | `true` for MinIO/most S3s; set `false` for AWS S3 |
| `KOTATSU_S3_ACCESS_KEY` / `_SECRET_KEY` | static keys (optional) |
| `KOTATSU_S3_SESSION_TOKEN` | session token for static temporary creds (optional) |

### Credentials: static keys or an IAM role

If `KOTATSU_S3_ACCESS_KEY`/`KOTATSU_S3_SECRET_KEY` are set they are used
directly. **Otherwise Kotatsu resolves credentials from the ambient AWS chain**,
so it can run with no secrets:

1. environment — `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`
2. **web identity (EKS IRSA)** — `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`
3. **EKS Pod Identity / ECS** — container credential endpoints
4. **EC2/ECS instance role** — IMDS

Temporary credentials are refreshed automatically. On EKS, attach a role to the
pod's ServiceAccount (IRSA annotation `eks.amazonaws.com/role-arn`) or via an
EKS Pod Identity association — the platform injects the env above and Kotatsu
picks it up. For real AWS S3 also set `KOTATSU_S3_FORCE_PATH_STYLE=false` and
leave `KOTATSU_S3_ENDPOINT` unset.
