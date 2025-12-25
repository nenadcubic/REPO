import { expect, test } from "@playwright/test";
import { expectBannerContains, navTo, seedExample, selectNamespace, waitForAppReady } from "./_helpers";

test.describe("Examples + Logs", () => {
  test("Examples list loads and can run basic_flags seed", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "examples");

    await expect(page.locator("#examplesSelect")).toBeVisible();
    await expect(page.locator("#examplesNs")).toBeVisible();

    await page.locator("#examplesNs").selectOption("er");
    await page.locator("#examplesSelect").selectOption("basic_flags");
    await page.locator("#examplesReset").check();
    await page.locator("#btnExamplesRun").click();
    await expect(page.locator("#examplesOut")).toContainText("Example: basic_flags");
  });

  test("Logs rejects invalid tail and refresh works", async ({ page }) => {
    await waitForAppReady(page);
    await navTo(page, "logs");

    await page.locator("#logsTail").fill("abc");
    await page.locator("#btnLogs").click();
    await expectBannerContains(page, "Must be a number");

    await page.locator("#logsTail").fill("50");
    await page.locator("#btnLogs").click();
    await expect(page.locator("#logsOut")).not.toHaveText("");
  });

  test("API seed works (backend sanity)", async () => {
    await seedExample("basic_flags", "er", true);
  });
});

