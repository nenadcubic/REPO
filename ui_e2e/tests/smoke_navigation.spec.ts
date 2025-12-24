import { expect, test } from "@playwright/test";
import { navTo, selectNamespace, waitForAppReady } from "./_helpers";

test("main GUI loads and navigates", async ({ page }) => {
  await waitForAppReady(page);

  await expect(page.locator(".sidebar-title")).toHaveText("element-redis");
  await expect(page.locator('a.nav-item[href="/explorer/"]')).toBeVisible();
  await expect(page.locator('#nsSelect option[value="er"]')).toHaveCount(1);
  await expect(page.locator('#nsSelect option[value="or"]')).toHaveCount(1);

  await navTo(page, "status");
  await navTo(page, "elements");
  await navTo(page, "queries");
  await navTo(page, "store");
  await navTo(page, "examples");
  await navTo(page, "logs");
  await navTo(page, "bitmaps");

  await selectNamespace(page, "er");
  await expect(page.locator("#nsSelect")).toHaveValue("er");
});
