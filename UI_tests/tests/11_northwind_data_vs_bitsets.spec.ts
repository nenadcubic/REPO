import { expect, test } from "@playwright/test";
import { apiPost, seedExample } from "./_helpers";

test.describe("Northwind: Data vs Bitsets", () => {
  test.beforeAll(async ({}, testInfo) => {
    testInfo.setTimeout(300_000);
    await seedExample("northwind_compare", "or", true);
    await apiPost("explorer/northwind/data_ingest", { ns: "or", reset: true, tables: ["Customers"] });
  });

  test("ingests Customers and compares Country = UK", async ({ page }) => {
    await page.goto("/explorer/data/", { waitUntil: "domcontentloaded" });

    await expect(page.getByTestId("data-root")).toBeVisible();
    await expect(page.getByTestId("data-table-row-Customers")).toBeVisible();

    await page.getByTestId("data-preset-cust-country-uk").click();
    await page.getByTestId("data-run-compare").click();

    const counts = page.getByTestId("data-counts");
    await expect(counts).toBeVisible();
    await expect(page.getByTestId("data-count-sql")).not.toHaveText("0");
    await expect(page.getByTestId("data-count-intersection")).not.toHaveText("0");
    await expect(page.getByTestId("data-intersection")).toBeVisible();
  });

  test("shows a user-facing error for empty values", async ({ page }) => {
    await page.goto("/explorer/data/", { waitUntil: "domcontentloaded" });
    await expect(page.getByTestId("data-root")).toBeVisible();

    // Clear value in the first condition and attempt to run.
    const firstValueInput = page.getByTestId("data-cond-row-0").locator("input");
    await firstValueInput.fill("");
    await page.getByTestId("data-run-compare").click();

    await expect(page.locator(".banner.error")).toBeVisible();
  });
});

