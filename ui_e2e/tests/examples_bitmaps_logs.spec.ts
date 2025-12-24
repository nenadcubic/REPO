import { expect, test } from "@playwright/test";
import { navTo, seedExample, selectNamespace, waitForAppReady } from "./_helpers";

test.describe("examples + bitmaps + logs", () => {
  test("Examples list loads and can run a seed example", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "examples");

    await expect(page.locator("#examplesSelect")).toBeVisible();
    await page.locator("#examplesNs").selectOption("er");
    await page.locator("#examplesSelect").selectOption("basic_flags");
    await page.locator("#examplesReset").check();
    await page.locator("#btnExamplesRun").click();
    await expect(page.locator("#examplesOut")).toContainText("Example: basic_flags");
  });

  test("Logs refresh works", async ({ page }) => {
    await waitForAppReady(page);
    await navTo(page, "logs");
    await page.locator("#logsTail").fill("50");
    await page.locator("#btnLogs").click();
    await expect(page.locator("#logsOut")).not.toHaveText("");
  });

  test("Bit-maps renders list and search filters", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "bitmaps");

    await page.locator("#btnBitmapsRefresh").click();
    await expect(page.locator("#bitmapsTbody tr").first()).toBeVisible();

    await page.locator("#bitmapsSearch").fill("risk");
    // client-side filter; allow 0+ rows
    await expect(page.locator("#bitmapsMeta")).toBeVisible();
  });

  test("API seed works (sanity)", async () => {
    await seedExample("basic_flags", "er", false);
  });
});
