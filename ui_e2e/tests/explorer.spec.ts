import { expect, test } from "@playwright/test";
import { seedExample } from "./_helpers";

test("Explorer shows namespaces (including or) and can open element view", async ({ page }) => {
  await seedExample("basic_flags", "er", true);
  await page.goto("/explorer/", { waitUntil: "domcontentloaded" });

  await expect(page.getByTestId("explorer-root")).toBeVisible();
  await expect(page.getByTestId("explorer-namespaces")).toBeVisible();

  await expect(page.getByTestId("explorer-ns-row-er")).toBeVisible();
  await expect(page.getByTestId("explorer-ns-row-or")).toBeVisible();

  await page.getByTestId("explorer-ns-row-er").click();
  await expect(page.getByTestId("explorer-elements")).toBeVisible();

  const firstRow = page.getByTestId("explorer-element-row").first();
  await expect(firstRow).toBeVisible();
  await firstRow.click();

  await expect(page.getByTestId("explorer-details")).toBeVisible();
});
