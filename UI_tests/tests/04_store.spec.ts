import { expect, test } from "@playwright/test";
import { expectBannerContains, navTo, seedExample, selectNamespace, waitForAppReady } from "./_helpers";

test.describe("Store + TTL: happy path + validation + delete", () => {
  test.beforeAll(async () => {
    await seedExample("basic_flags", "er", true);
  });

  test("Store validates TTL and bits", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "store");

    await page.locator("#storeTtl").fill("0");
    await page.locator("#storeBitsAll").fill("1 2");
    await page.locator("#btnStore").click();
    await expectBannerContains(page, "Invalid value");

    await page.locator("#storeTtl").fill("86401");
    await page.locator("#btnStore").click();
    await expectBannerContains(page, "Invalid value");

    await page.locator("#storeTtl").fill("60");
    await page.locator("#storeBitsAll").fill("2");
    await page.locator("#btnStore").click();
    await expectBannerContains(page, "Invalid value");
  });

  test("Store ANY -> Inspect -> Delete -> Inspect fails", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "store");

    await page.locator("#storeTtl").fill("60");
    await page.locator('.tab[data-tabgroup="store-create"][data-tab="sc-any"]').click();
    await page.locator("#storeBitsAny").fill("4 5");
    await page.locator("#btnStore").click();
    await expect(page.locator("#storeOut")).toContainText("✅ Stored as:");

    const outText = await page.locator("#storeOut").innerText();
    const match = outText.match(/Stored as:\s*([^\s]+)/);
    expect(match?.[1], `store_key not found in output: ${outText}`).toBeTruthy();
    const storeKey = match![1];

    await page.locator('.tab[data-tabgroup="store-main"][data-tab="store-inspect"]').click();
    await page.locator("#inspectKey").fill(storeKey);
    await page.locator("#inspectLimit").fill("200");
    await page.locator("#btnInspect").click();
    await expect(page.locator("#inspectOut")).toContainText("beta");
    await expect(page.locator("#inspectOut")).toContainText("gamma");

    await page.locator("#btnDeleteStore").click();
    await expect(page.locator("#inspectOut")).toContainText("Deleted:");

    await page.locator("#btnInspect").click();
    await expect(async () => {
      const banner = await page.locator("#banner").textContent();
      const out = await page.locator("#inspectOut").textContent();
      expect((banner || "") + "\n" + (out || "")).toMatch(/Request failed|0 elements|NOT_FOUND/i);
    }).toPass();
  });

  test("Inspect validates empty store_key", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "store");
    await page.locator('.tab[data-tabgroup="store-main"][data-tab="store-inspect"]').click();

    await page.locator("#inspectKey").fill("");
    await page.locator("#btnInspect").click();
    await expectBannerContains(page, "required");
  });
});
