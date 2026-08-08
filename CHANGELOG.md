# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Decode with the Popsink `tansu-sans-io`, not crates.io 0.6** (#96) — the
  record-batch decoder is now a git dependency on `Popsink/tansu` pinned to
  `v0.7.0-beta.39`, the version the deployed broker writes with. It was resolving
  upstream `tansu-io` 0.6.0 from crates.io — a different codebase from the writer,
  missing every decode-side fix made in the fork since: the record format decided
  at the magic byte rather than by CRC accident (Popsink/tansu#323), no record
  `Vec` sized from the wire's `record_count` (Popsink/tansu#306 — the reachable
  one, since Kotatsu decodes whatever objects are in the bucket), and the
  per-struct tag buffer counted (Popsink/tansu#324). Pinned to a tag rather than
  floating on `main`: a decoder that changes under us is worse than one that lags
  visibly, and the version to track is the broker's — the same one the test stack
  runs, so `TANSU_VERSION` in `docker-compose.yml` and this tag move together.
- **Footer v3 is what production writes** (#96) — this supersedes the 0.9.0 note
  below, which said "nothing in Tansu emits v3 yet". Since Popsink/tansu#188
  (`0.7.0-beta.25`) v3 is emitted unconditionally on every write path, including
  both compactions; v1 and v2 are read-only history. The decoding itself was
  already right — the producer-coordinate stride is 22 bytes at v1/v2 and 23 at
  v3, checked against `encode_footer` at `v0.7.0-beta.39` — only the comments
  implied the v3 branch was untested speculation rather than the only branch that
  runs on current data.

## [0.9.0] - 2026-07-27

### Added
- **Accept segment footer v3** — the segment decoder now accepts format version 3
  in addition to v1/v2, reading the producer-coordinate table with a
  version-dependent stride: **23 bytes at v3, 22 below it**, because v3 adds a
  one-byte `flags` field per coordinate. Nothing in Tansu emits v3 yet; this ships
  first so that when the broker's writer flips (Popsink/tansu#174), this reader
  does not mis-parse every segment it opens. Without the stride change a v3
  segment's coordinate table would be read one byte short per entry, desynchronising
  the cursor and corrupting the footer index that follows it.

## [0.8.0] - 2026-07-24

### Added
- **Browse topics by prefix** — the flat, alphabetical topic list is replaced by
  a drill-down that follows the Tansu coalescing prefix, **org → env → connector
  → topic**, with search scoped to each level. New endpoint
  `GET /api/clusters/{cluster}/topic-tree?prefix=&search=&limit=&offset=`: below
  the connector level it groups the cached name index by the next dotted
  component (a pure in-memory grouping, no per-node storage reads, so it stays
  cheap at ~15k topics); at the connector level it lists the topics under the
  prefix with per-page summaries, like the existing list. A topic with fewer than
  three components surfaces as a directly-linkable leaf. The chosen path lives in
  the URL (`?p=`) for back-button and deep-link support, with a breadcrumb and an
  on-disk size column at the leaf.

### Changed
- Added a `.dockerignore` so `backend/target` (~9 GB) no longer enters the Docker
  build context.

## [0.7.0] - 2026-07-23

### Added
- **Cached topic/group catalog** — listing and searching topics and consumer
  groups are now served from a short-TTL in-process cache instead of re-scanning
  the object store on every request. The name index is cached (so search filters
  an in-memory list per keystroke rather than re-listing the prefix), and the
  per-row summaries (topic partition/message/size counts; group state/members)
  are filled lazily as pages are served and reused within the TTL. Warmed on a
  miss with no background poller; per-cluster and per-process, like the existing
  high-watermark and segment-footer caches. Detail views remain exact and
  uncached (#84).

## [0.6.0] - 2026-07-23

### Added
- **Tansu prefix-coalesced virtual-topic segments** — Kotatsu now reads Tansu's
  virtual-topic segments (Tansu `#56`, shipped in tansu `0.7.0-beta.13`)
  alongside the legacy per-topic layout in the same bucket. Many topics'
  records are multiplexed into shared, immutable per-prefix segment objects
  (`prefixes/{prefix}/segments/{seq:020}.seg`) with a self-describing footer
  index, read via ranged GET. The footer decoder accepts format versions `1`
  and `2` (the v2 per-flush nonce and per-batch producer coordinates are
  parsed-and-skipped) and rejects unknown versions; an object with no `TSEG`
  trailer is treated as a legacy v0 batch concatenation. Watermark, earliest,
  time-seek and message reads derive from the footer; overlapping segments left
  by a compaction or writer failover resolve by highest `writer_epoch`; hybrid
  topics stitch legacy `records/` `[0, C)` and segments `[C, ∞)` across the
  seam; and a segment deleted by compaction mid-read is retried. `watermark.json`
  is now optional, matching the leaseless writer that never persists it on the
  produce path (#82).

## [0.5.0] - 2026-07-17

### Added
- **Tansu coalesced record-batch objects** — with Tansu's server-side produce
  coalescing (Tansu `#50`), a `records/{base}.batch` object can hold several
  Kafka record batches concatenated (a `deflated::Frame`), still named by the
  first batch's base offset. Each object is now decoded as a sequence of
  batches: absolute offsets run from the object-name base, advancing by
  `lastOffsetDelta + 1` per sub-batch; control sub-batches are skipped
  individually; the high watermark sums the tail object's span over all its
  sub-batches; and time-seek compares against each object's newest timestamp
  across all sub-batches. A single-batch object is a one-element frame and
  decodes exactly as before (#80).

## [0.4.0] - 2026-07-06

### Added
- **Per-topic storage size** — the topic API now exposes `storage_bytes`, the
  physical on-disk size in S3 (compressed bytes of the record segments):
  top-level and per-partition on `GET /api/clusters/{cluster}/topics/{topic}`,
  and a per-topic total on the topics list. Computed from S3 object metadata in
  a single recursive listing of the topic's `partitions/` prefix (no content
  scan). Fills the data-plane datamodel Usage tab's "Storage size" tile (#76).

### Fixed
- **Storage errors no longer leak the S3 object layout** — storage-layer
  not-found responses returned the raw object key (e.g.
  `clusters/tansu/topics/x/partitions/0000000000/watermark.json`), exposing the
  bucket path layout and partition encoding. A missing topic and an
  out-of-range partition now return distinct, sanitized messages; the raw key
  stays in the server logs only, mirroring the schema-registry error hygiene
  (#63).
- **Invisible failure states on the topic page** — a failed consumer-groups
  load rendered as "none", indistinguishable from a topic with zero groups, and
  now shows "couldn't load consumer groups" (#66); a rejected "Copy JSON" was
  silently swallowed, leaving the button on its default label, and now shows a
  transient "Copy failed" (#65).

## [0.3.3] - 2026-06-25

### Fixed
- **Watermarks on Tansu beta.6 storage** — `watermark.json` is only a lazily
  persisted hint (null/stale), so it must not be treated as authoritative. Low
  is now derived from the earliest surviving batch instead of defaulting to 0
  (#71); high is always derived from the record objects, with the stored value
  used only as a floor, so it no longer caps below the real tail (#72).
- **Topic-listing performance** — the high watermark is found via a bounded
  `list_with_offset` tail scan from that floor plus a per-process monotonic
  in-memory cache, instead of listing every batch in the partition (#73).

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

[0.5.0]: https://github.com/Popsink/kotatsu/releases/tag/v0.5.0
[0.4.0]: https://github.com/Popsink/kotatsu/releases/tag/v0.4.0
[0.3.1]: https://github.com/Popsink/kotatsu/releases/tag/v0.3.1
[0.2.1]: https://github.com/Popsink/kotatsu/releases/tag/v0.2.1
[0.2.0]: https://github.com/Popsink/kotatsu/releases/tag/v0.2.0
[0.1.1]: https://github.com/Popsink/kotatsu/releases/tag/v0.1.1
[0.1.0]: https://github.com/Popsink/kotatsu/releases/tag/v0.1.0
