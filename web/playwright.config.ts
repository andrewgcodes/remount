import { defineConfig, devices } from '@playwright/test';

import { baseURLs } from './tests/e15/state';

// Every project drives a real `remount` binary started by the E15 global
// setup. There is no mock HTTP server and no routed WebSocket: the console
// bytes, the fleet, the PTY and the credential decisions all come from Go.
export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: false,
  workers: 1,
  // The specs mutate durable server state (a workspace is created, a snapshot
  // taken, a workspace moved, an approval decided). A retry would replay those
  // against already-changed state, so a failure is reported, not re-rolled.
  retries: 0,
  reporter: process.env.CI ? 'github' : 'line',
  timeout: 180_000,
  expect: { timeout: 45_000 },
  globalSetup: './tests/e15/global-setup.mjs',
  globalTeardown: './tests/e15/global-teardown.mjs',
  use: {
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'standalone',
      testMatch: /standalone-.*\.spec\.ts$/,
      use: { ...devices['Desktop Chrome'], baseURL: baseURLs.standalone },
    },
    {
      name: 'rbac',
      testMatch: /rbac\.spec\.ts$/,
      use: { ...devices['Desktop Chrome'], baseURL: baseURLs.rbac },
    },
  ],
});
