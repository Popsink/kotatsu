# Kotatsu — August 2026 QA Plan (scale & load)

> Part of the Popsink August 2026 non-functional QA effort. Sister plans live in
> `data-plane/docs/qa/`, `kora/loadtest/`, and `popsink-partner-portal/e2e_tests/`.

## Objective

Prove Kotatsu's **read-only** browse path holds up at scale — a large topic
population (~15k topics, the order of magnitude called out in the README) and a
Kora registry loaded to **30 000 schemas** — and publish per-endpoint latency
baselines with a focus on **p99 tail latency**.

## Guardrails (non-negotiable)

- 🚫 **No client / real data.** Same policy as the existing `e2e/` cases: fictitious
  and generic only (`orders`, `widget`, `key-1`, `avro-perf-{i}`). This repo is public.
- Tests run against a **dedicated, disposable environment** — never prod, never a shared stack.

## Tooling & environment

- **k6** — new here; there is **no `loadtest/` yet**. Scaffold one by copying
  `kora/loadtest/` (scenario taxonomy + tagged `helpers.js` pattern).
- **Dedicated env** with **Balkis (SRE)**; k6 → **Prometheus → Grafana**.
- Existing semi-manual ISTQB cases under `e2e/modules/` stay as-is (functional layer);
  this plan adds the **non-functional** layer on top.

## What makes Kotatsu different (test design implications)

- It is **read-only** and reads directly from **S3 (MinIO locally)** — decode
  `.batch` files, resolve Avro against Kora. So a slow result may be the **object
  store or Kora**, not Kotatsu. Every report must **isolate the tier** (S3 fetch
  vs decode vs Kora resolve vs Kotatsu HTTP).
- Every read is **user-triggered** (no background polling) — load = simulated user
  browsing, which maps cleanly to k6 VUs.

## Workstreams

### WS-T1 — Scaffold the k6 harness
- Create `kotatsu/loadtest/` from the kora template: `helpers.js` (tagged helpers
  for the read endpoints), `justfile` recipes, `docker-compose.loadtest.yml`
  (Kotatsu + MinIO + Kora + Kora-DB), a seed script that produces synthetic topics
  and Avro schemas.
- Baseline (`smoke`, 1 VU) capturing p50 / p95 / **p99** per endpoint on a small seed.

### WS-T2 — Topic-population scale
- Seed a large synthetic topic set (target ~15k) via a producer script (see README
  seed commands) and exercise:
  - `GET /api/clusters/{c}/topics` (list + pagination),
  - `GET /api/clusters/{c}/topic-tree?prefix=&search=` (the drill-down; verify the
    in-memory grouping stays cheap at scale, as claimed in CHANGELOG 0.8.0),
  - `GET /api/clusters/{c}/topics/{t}/messages` (read-back + filters).

### WS-T3 — Schema scale (depends on Kora WS-K2)
- Point Kotatsu at a Kora loaded with **30k schemas**; exercise
  `GET /api/schemas`, `GET /api/schemas/{subject}`, and Avro decode on message reads.
- Confirm decode + registry-resolve latency does not blow up as the subject count grows.

### WS-T4 — Sustained load
- `load` / `stress` (ramp VUs) on the hottest read endpoints to find the knee.
- Short `soak` to check for leaks / cursor issues in the batch decoder under repeated reads.

## Metrics & acceptance

- Per run: **p50 / p95 / p99** and max per endpoint, throughput, error rate, **plus a
  per-tier breakdown** (S3 / decode / Kora / HTTP) so a regression is attributable.
- **p99 thresholds derived from the WS-T1 baseline.** A run fails on a p99 breach or any error.

## Deliverables

- New `kotatsu/loadtest/` harness + synthetic seed, committed.
- Per-run perf reports (HTML export / Grafana), with the tier breakdown.
- Issues for any regression: **frontend → Gwen**, **backend → Romain**.

## Risks

- MinIO throughput can mask/inflate Kotatsu latency → always report the tier breakdown.
- WS-T3 depends on Kora's 30k seed (WS-K2) → sequence after Kora.
- August holidays (FR) → lock the dedicated-env slot with Balkis early.
