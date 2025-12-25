import { expect, test } from "@playwright/test";

test.describe("Associations (WordNet) UI", () => {
  test("renders demo board, checks an answer, shows explanation, and solves all", async ({ page }) => {
    await page.goto("/explorer/assoc/?mode=demo", { waitUntil: "domcontentloaded" });

    await expect(page.getByTestId("assoc-root")).toBeVisible();
    await expect(page.getByTestId("assoc-status")).toBeVisible();
    await expect(page.getByTestId("assoc-copy-ingest-docker")).toBeVisible();
    await expect(page.getByTestId("assoc-copy-ingest-host")).toBeVisible();
    await expect(page.getByTestId("assoc-grid")).toBeVisible();

    await expect(page.getByTestId("assoc-cell-A1")).toBeVisible();
    await expect(page.getByTestId("assoc-cell-B1")).toBeVisible();
    await expect(page.getByTestId("assoc-cell-C1")).toBeVisible();
    await expect(page.getByTestId("assoc-cell-D1")).toBeVisible();
    await expect(page.getByTestId("assoc-cell-final")).toBeVisible();

    const finalCell = page.getByTestId("assoc-cell-final");
    await finalCell.locator("input").fill("festival");
    await finalCell.getByRole("button", { name: "Check" }).click();
    await expect(finalCell).toHaveAttribute("data-correct", "true");

    const a1 = page.getByTestId("assoc-cell-A1");
    await a1.locator("input").click();
    await expect(page.getByTestId("assoc-explain")).not.toContainText("Click “Explain”");

    await page.getByRole("button", { name: "Solve all" }).click();
    await expect(page.getByTestId("assoc-cell-A").locator("input")).toHaveValue("music");
    await expect(page.getByTestId("assoc-cell-final").locator("input")).toHaveValue("festival");
    await expect(page.getByTestId("assoc-cell-final")).toHaveAttribute("data-correct", "true");
  });
});
