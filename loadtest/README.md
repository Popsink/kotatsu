# Kotatsu — Load tests (k6)

k6 load/scale tests for Kotatsu's read-only browse path. Structured like
`kora/loadtest/` (tagged helpers + per-scenario files). Part of the August 2026
non-functional QA effort — see `e2e/AUGUST_2026_QA_PLAN.md`.

> **Data policy:** synthetic data only (`orders`, `avro-perf-{i}`, …). Never real
> customer data — this repo is public.

## Prerequisites

- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) (`brew install k6`)
- A running Kotatsu with data. Two options:
  - **local** — the repo's `docker compose up` stack (uses MinIO), or
  - **dedicated test env** — Kotatsu + **rustfs** (S3-compatible, mirrors prod's
    AWS S3) + Kora + Kora-DB, provisioned with the SRE. Kotatsu reaches the store
    directly (no auth).

## Run

```bash
cd loadtest
KOTATSU_URL=http://localhost:8080 KOTATSU_CLUSTER=demo \
  k6 run --summary-trend-stats="avg,min,med,p(90),p(95),p(99),max" scenarios/smoke.js
```

Config via env vars:

| Var | Default | Purpose |
| --- | --- | --- |
| `KOTATSU_URL` | `http://localhost:8080` | Kotatsu base URL (UI + `/api/*`) |
| `KOTATSU_CLUSTER` | `demo` | Tansu cluster id |

## Reporting

- Local visual report: `K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_EXPORT=report.html k6 run …`
- Shared/historical: `--out experimental-prometheus-rw` → Prometheus → Grafana
  (with `K6_PROMETHEUS_RW_SERVER_URL` set). Focus metric: **p99 per endpoint**.
- Every report should isolate the tier — **S3 (rustfs) fetch vs batch decode vs
  Kora resolve vs Kotatsu HTTP** — since a slow read may be the store or Kora,
  not Kotatsu.

## Scenarios

| File | Status | Purpose |
| --- | --- | --- |
| `scenarios/smoke.js` | ready | 1 VU / 30s baseline — full read journey, data-agnostic (discovers topics/schemas/groups in `setup()`) |
| topic-scale (~15k topics) | TODO (WS-T2) | list / topic-tree / messages at scale |
| schema-scale (30k, needs Kora) | TODO (WS-T3) | `/schemas` + Avro decode at scale |
| load / stress / soak | TODO (WS-T4) | ramp VUs, find the knee, check decoder leaks |

Note: the local stack currently uses MinIO; the dedicated env uses **rustfs**. A
local S3-compatible store is faster than real AWS S3, so latency figures are
optimistic — a real-S3 validation run may follow.
