import { defineConfig, devices } from "@playwright/test";

const baseURL = process.env.GUI_BASE_URL ?? "http://localhost:18080";
const apiBaseURL = process.env.API_BASE_URL ?? "http://localhost:18000/api/v1";

export default defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: [["list"], ["html", { open: "never" }]],
  use: {
    baseURL,
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
    permissions: ["clipboard-read", "clipboard-write"],
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  metadata: {
    baseURL,
    apiBaseURL,
  },
});

