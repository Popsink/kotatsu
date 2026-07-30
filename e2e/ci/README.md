# `e2e/ci/` — automated smoke (Playwright)

A standalone `@playwright/test` smoke that mirrors
[`../test_plans/SMOKE_TEST_PLAN.md`](../test_plans/SMOKE_TEST_PLAN.md). Unlike the
semi-manual cases (driven interactively with the Playwright **CLI**), this is a
deterministic spec run by `npx playwright test` — it is what CI executes.

Two layers in `smoke.spec.ts`:

- **API smoke** — assertions on the read path (health, source, topics, messages,
  Avro decode, schemas, groups/lag, 4xx errors). No browser needed.
- **UI smoke** — the Nuxt SPA renders each screen and its figures match the API.

## Run locally

```bash
# 1. from the repo root: bring up + seed the stack
docker compose up -d --build
NETWORK=kotatsu_default ./e2e/scripts/seed.sh

# 2. run the smoke
cd e2e/ci
npm install
npx playwright install chromium
BASE_URL=http://localhost:8080 npx playwright test

# API layer only (no browser):
npx playwright test -g "API smoke"
```

Override the target with `BASE_URL` (default `http://localhost:8080`).

## CI

The `smoke-e2e` job in [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
runs on every PR/push: it builds the app image from the ref, brings up the full
compose stack, seeds data via `e2e/scripts/seed.sh`, runs this smoke, and uploads
the HTML report as an artifact.
