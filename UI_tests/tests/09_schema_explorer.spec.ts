import { expect, test } from "@playwright/test";
import { seedExample } from "./_helpers";

test.describe("Schema Explorer (northwind_meta_v0)", () => {
  test.beforeAll(async ({}, testInfo) => {
    testInfo.setTimeout(300_000);
    await seedExample("northwind_compare", "or", true);
  });

  test("shows tables and decodes Orders columns + relations", async ({ page }) => {
    await page.goto("/explorer/schema/", { waitUntil: "domcontentloaded" });

    await expect(page.getByTestId("schema-root")).toBeVisible();
    await expect(page.getByTestId("schema-tables")).toBeVisible();

    await expect(page.getByTestId("schema-table-row-Customers")).toBeVisible();
    await expect(page.getByTestId("schema-table-row-Orders")).toBeVisible();

    await page.getByTestId("schema-table-row-Orders").click();
    await expect(page.getByTestId("schema-columns")).toBeVisible();
    await expect(page.getByTestId("schema-relations")).toBeVisible();

    const orderIdRow = page.getByTestId("schema-column-row-OrderID");
    await expect(orderIdRow).toBeVisible();
    await expect(orderIdRow).toContainText("INTEGER");
    await expect(orderIdRow).toContainText("NOT NULL");
    await expect(orderIdRow).toContainText("PK");

    const relRow = page.getByTestId(/schema-relation-row-Orders-Customers-fk\d+/);
    await expect(relRow.first()).toBeVisible();
    await expect(relRow.first()).toContainText("1:N");
  });
});
