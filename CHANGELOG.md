# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/Popsink/kotatsu/releases/tag/v0.1.0
