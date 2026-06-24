# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.2] - 2026-06-24

### Fixed
- **Tansu beta.6 metadata layout** — topics are now read from per-topic
  `topic-metadata/{name}.json` objects (the authority since Tansu beta.6),
  falling back to the legacy monolithic `meta.json` for unmigrated clusters.
  The cluster summary tolerates a `meta.json` with no topics (or none at all).
- **Lazy watermarks** — Tansu beta.6 persists `watermark.json` `high` only on a
  cold read, so a produced-but-unconsumed partition reported zero messages and
  an empty event browser. The high is now derived from the last record batch
  (`base + lastOffsetDelta + 1`) when the stored value is unusable.

## [0.3.1] - 2026-06-18

### Added
- **GCS storage backend** — Google Cloud Storage is now a supported storage
  provider alongside S3. Set `KOTATSU_STORAGE_PROVIDER=gcs` (default: `s3`) to
  read from a GCS bucket. Credentials are resolved from `GOOGLE_SERVICE_ACCOUNT`
  (JSON key content), `GOOGLE_SERVICE_ACCOUNT_PATH`, or
  `GOOGLE_APPLICATION_CREDENTIALS`; on GKE, Workload Identity is picked up
  automatically with no credentials needed.

### Changed
- **Helm chart** — the `s3:` values section is renamed to `storage:` and gains
  a `provider` field (`"s3"` or `"gcs"`, default `"s3"`). Existing S3
  deployments must rename the key in their `values.yaml` / `--set` flags.
- CI multi-arch images are now built with native runners instead of QEMU,
  removing the emulation overhead from release builds.

## [0.3.0] - 2026-06-15

### Added
- **Python bindings** (`bindings/python`): the reader's full read API is now
  consumable as an async Python extension (PyO3 + `pyo3-async-runtimes`,
  abi3 wheel via maturin). An async `Source` exposes clusters, topics,
  consumer groups (with lag), schemas and messages (formats + filters +
  bounded scan), returning plain Python objects and raising `KotatsuError`.
  Enables reading Tansu's S3 storage directly from Python services (e.g.
  data-plane) without a Kafka broker.
- CI builds and import-tests the wheel on CPython 3.12.

### Changed
- The message decode/filter/bounded-scan core moved into a reusable `query`
  module shared by the HTTP API and the Python bindings, so both behave
  identically. No HTTP API behavior change.

## [0.2.1] - 2026-06-12

### Fixed
- Schema-registry HTTP client now has bounded timeouts (connect 2s / request
  5s): an unreachable Kora fails fast instead of hanging ~25–30s on message
  search.
- A navigation loader (`NuxtLoadingIndicator`) is shown when moving between
  pages (the `await useFetch` route suspense previously gave no feedback).
- User-facing errors no longer leak the registry's internal REST route or
  in-cluster URL — `subject '<name>' not found` / `schema registry is
  unreachable` (details kept to server logs).

## [0.2.0] - 2026-06-03

### Added
- **Search & pagination** on the topics, consumer-groups and schemas lists
  (`?search=&limit=&offset=`), with loading spinners across the UI.
- **Serializer choice** in the event browser — `auto` / `avro` / `json` / `raw`
  per key and value, remembered per topic.
- **Message filters** — filter by key/value substring (or regex) and header,
  with a bounded forward scan (`max_scan`) honoring the on-demand model.
- **Export & copy** — download the current messages as JSON / NDJSON and copy a
  single message.
- **Cross-navigation links** — topic ↔ schema subjects, group offsets → topics,
  decoded message → its schema, and a lazy "consumer groups consuming this
  topic" section.
- **Topic configuration** on the topic detail (replication factor + config
  overrides).
- **Schema browser**: view any version and the subject's compatibility level.
- **Consumer group detail**: total lag and per-member partition assignments
  (best-effort decode of the Kafka assignment blob).

## [0.1.1] - 2026-06-02

### Fixed
- Event browser: Confluent-framed Avro values containing `decimal`, `bytes` or
  `fixed` fields (e.g. CDC/Debezium events) are now decoded to JSON instead of
  being shown as raw hex. Decode and schema-registry errors are surfaced in the
  field result and in the UI.

### Added
- Helm chart (`chart/kotatsu`) for Kubernetes deployment.

## [0.1.0] - 2026-06-02

First release. A read-only, on-demand browser over [Tansu](https://github.com/tansu-io/tansu)'s
native S3 storage — **no Kafka broker, no Kafka client, no background polling**.
Built with Rust (Axum) + Nuxt 3.

### Added

- **S3 storage access layer** — reads Tansu's native S3 layout directly via
  `object_store`; on-demand only, no background tasks. `GET /api/source`
  reports connectivity.
- **Storage reader** — decodes `.batch` objects (raw Kafka record batches) with
  `tansu-sans-io`; predecessor-based offset seek, `latest`/`earliest`/specific
  offset and batch-header time seek; control batches skipped.
- **Source overview** — `GET /api/clusters`, `GET /api/clusters/{cluster}`;
  cluster metadata (topics / producers / transactions) from `meta.json`.
- **Topics** — list and per-topic detail with per-partition low/high watermarks
  and approximate message counts.
- **Event browser** — fetch and display messages from a topic partition;
  key/value as UTF-8 or hex, headers, expandable rows.
- **Consumer groups** — list with derived state, committed offsets and lag
  (`high − committed`), read from `groups/consumers/`.
- **Avro deserialization** — decodes Confluent-framed Avro keys/values against
  the [Kora](https://github.com/Popsink/kora) schema registry, with a no-TTL
  schema cache; plus a schema browser (`GET /api/schemas`, `/api/schemas/{subject}`).
- **S3 authentication** — static keys or the ambient AWS credential chain
  (environment, EKS IRSA web identity, EKS Pod Identity / ECS, EC2/ECS instance
  role); temporary credentials refresh automatically.
- **Popsink branding** — logo, Geist font and brand palette across the UI.
- **Packaging & CI** — multi-stage Docker image (single image, backend serves
  the bundled frontend); `ci` workflow (fmt, clippy, unit + integration tests);
  `release` workflow publishing multi-arch images to `ghcr.io/popsink/kotatsu`.

[0.3.1]: https://github.com/Popsink/kotatsu/releases/tag/v0.3.1
[0.2.1]: https://github.com/Popsink/kotatsu/releases/tag/v0.2.1
[0.2.0]: https://github.com/Popsink/kotatsu/releases/tag/v0.2.0
[0.1.1]: https://github.com/Popsink/kotatsu/releases/tag/v0.1.1
[0.1.0]: https://github.com/Popsink/kotatsu/releases/tag/v0.1.0
