import { expect, test } from "@playwright/test";
import { navTo, runQueryFind, seedExample, selectNamespace, waitForAppReady } from "./_helpers";

test.describe("elements + queries + store flows", () => {
  test.beforeAll(async () => {
    await seedExample("basic_flags", "er", true);
  });

  test("Elements Get shows seeded element", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "elements");
    await page.locator('.tab[data-tabgroup="elements"][data-tab="elements-get"]').click();

    await page.locator("#getName").fill("alpha");
    await page.locator("#getLimit").fill("200");
    await page.locator("#btnGet").click();

    await expect(page.locator("#getOut")).toContainText("Bits: [1, 2, 3]");
    await expect(page.locator("#getOut")).toContainText("Found: 3 bits");
  });

  test("Matrix renders for seeded element", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "elements");
    await page.locator('.tab[data-tabgroup="elements"][data-tab="elements-matrix"]').click();

    await page.locator("#matrixName").fill("alpha");
    await page.locator("#btnMatrixFetch").click();

    await expect(page.locator("#matrixCanvas")).toBeVisible();
    await page.locator("#matrixCanvas").hover({ position: { x: 8, y: 8 } });
    await expect(page.locator("#matrixTooltip")).not.toHaveClass(/hidden/);
  });

  test("Query Find returns expected names", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await runQueryFind(page, "2");

    const names = page.locator("#queryNames");
    await expect(names).toContainText("alpha");
    await expect(names).toContainText("beta");
  });

  test("Store results can be created and inspected", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "store");

    await page.locator("#storeTtl").fill("60");
    await page.locator('.tab[data-tabgroup="store-create"][data-tab="sc-any"]').click();
    await page.locator("#storeBitsAny").fill("1 2");
    await page.locator("#btnStore").click();
    await expect(page.locator("#storeOut")).toContainText("Stored as:");

    const outText = await page.locator("#storeOut").innerText();
    const match = outText.match(/Stored as:\s*([^\s]+)/);
    expect(match?.[1], `store_key not found in output: ${outText}`).toBeTruthy();
    const storeKey = match![1];

    await page.locator('.tab[data-tabgroup="store-main"][data-tab="store-inspect"]').click();
    await page.locator("#inspectKey").fill(storeKey);
    await page.locator("#inspectLimit").fill("200");
    await page.locator("#btnInspect").click();

    await expect(page.locator("#inspectOut")).toContainText("alpha");
    await expect(page.locator("#inspectOut")).toContainText("beta");
  });
});
