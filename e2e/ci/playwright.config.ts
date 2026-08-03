import { defineConfig, devices } from '@playwright/test';

/**
 * Automated smoke for Kotatsu, run against a locally running docker-compose
 * stack (see e2e/README.md). No auth: the UI is a read-only browser.
 *
 * The stack must already be up and seeded (see e2e/scripts/seed.sh) before
 * running — this config does NOT start a webServer.
 *
 * Override the target with BASE_URL (default http://localhost:8080).
 */
const BASE_URL = process.env.BASE_URL ?? 'http://localhost:8080';

export default defineConfig({
  testDir: '.',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 2 : undefined,
  timeout: 30_000,
  expect: { timeout: 10_000 },
  reporter: process.env.CI
    ? [['html', { open: 'never' }], ['list']]
    : [['list']],
  use: {
    baseURL: BASE_URL,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
  ],
});
