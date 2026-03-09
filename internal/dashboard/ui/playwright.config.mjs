import { defineConfig } from "@playwright/test";

const PORT = Number(process.env.PLAYWRIGHT_DASHBOARD_PORT || 4173);

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  timeout: 30_000,
  expect: {
    timeout: 10_000,
  },
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    headless: true,
    trace: "retain-on-failure",
  },
  webServer: {
    command: "node scripts/serve-smoke-dashboard.mjs",
    cwd: ".",
    url: `http://127.0.0.1:${PORT}/dashboard/`,
    reuseExistingServer: !process.env.CI,
    timeout: 30_000,
    env: {
      PLAYWRIGHT_DASHBOARD_PORT: String(PORT),
    },
  },
});
