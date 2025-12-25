import { expect, test } from "@playwright/test";
import { seedExample } from "./_helpers";

test.describe("Explorer app", () => {
  test.beforeAll(async () => {
    await seedExample("basic_flags", "er", true);
  });

  test("can browse namespaces and open an element details view", async ({ page }) => {
    await page.goto("/explorer/", { waitUntil: "domcontentloaded" });

    await expect(page.getByTestId("explorer-root")).toBeVisible();
    await expect(page.getByTestId("explorer-namespaces")).toBeVisible();
    await expect(page.getByTestId("explorer-elements")).toBeVisible();

    await expect(page.getByTestId("explorer-ns-row-er")).toBeVisible();
    await expect(page.getByTestId("explorer-ns-row-or")).toBeVisible();

    await page.getByTestId("explorer-ns-row-er").click();
    const firstRow = page.getByTestId("explorer-element-row").first();
    await expect(firstRow).toBeVisible();
    await firstRow.click();

    await expect(page.getByTestId("explorer-details")).toBeVisible();
    await expect(page.getByTestId("explorer-details")).toContainText("Namespace:");
  });

  test("quick create validates inputs (empty name, empty bits)", async ({ page }) => {
    await page.goto("/explorer/", { waitUntil: "domcontentloaded" });
    await page.getByTestId("explorer-ns-row-er").click();

    const quickCreatePanel = page.locator('.panel:has(.panel-title:has-text("Quick create (debug)"))');
    const nameInput = quickCreatePanel.locator("input.input").first();
    const bitsInput = quickCreatePanel.locator('input.input[placeholder="e.g. 1 2 3"]');
    const saveBtn = page.getByRole("button", { name: "Save element" });

    await nameInput.fill("");
    await bitsInput.fill("1");
    await saveBtn.click();
    await expect(page.getByText("Name must be 1..100 chars.")).toBeVisible();

    await nameInput.fill("pw_explorer_tmp");
    await bitsInput.fill("");
    await saveBtn.click();
    await expect(page.getByText("Bits must include at least one valid bit (0..4095).")).toBeVisible();
  });

  test("OR namespace disables Namespace bitmap tab", async ({ page }) => {
    await page.goto("/explorer/", { waitUntil: "domcontentloaded" });
    await page.getByTestId("explorer-ns-row-or").click();

    const bitmapTab = page.getByRole("button", { name: "Namespace bitmap" });
    await expect(bitmapTab).toBeDisabled();
  });
});
