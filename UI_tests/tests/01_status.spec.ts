import { expect, test } from "@playwright/test";
import { expectBannerContains, seedExample, selectNamespace, waitForAppReady } from "./_helpers";

test.describe("Status view", () => {
  test.beforeAll(async () => {
    await seedExample("basic_flags", "er", true);
  });

  test("refresh renders backend + redis status", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");

    await page.locator('.nav-item[data-view="status"]').click();
    await page.locator("#btnStatusRefresh").click();

    await expect(page.locator("#backendStatus")).not.toHaveText("");
    await expect(page.locator("#backendStatus")).toContainText("Memory Used:");
    await expect(page.locator("#backendStatus")).toContainText("Backend Version:");
    await expect(page.locator("#backendStatus")).toContainText("Redis:");
  });

  test("namespace discovery populates table", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");

    await page.locator('.nav-item[data-view="status"]').click();
    await page.locator("#btnNsDiscover").click();

    await expectBannerContains(page, "Discovery completed");
    await expect(page.locator("#nsDiscoverTbody tr").first()).toBeVisible();
  });
});
