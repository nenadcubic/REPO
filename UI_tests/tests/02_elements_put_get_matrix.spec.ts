import { expect, test } from "@playwright/test";
import { expectBannerContains, navTo, seedExample, selectNamespace, waitForAppReady } from "./_helpers";

test.describe("Elements: put/get/matrix + validation", () => {
  test.beforeAll(async () => {
    await seedExample("basic_flags", "er", true);
  });

  test("Put: rejects empty + too-long name, and invalid bits", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "elements");

    await page.locator("#putName").fill("");
    await page.locator("#putBits").fill("1 2");
    await page.locator("#btnPut").click();
    await expectBannerContains(page, "required");

    await page.locator("#putName").fill("x".repeat(101));
    await page.locator("#putBits").fill("1 2");
    await page.locator("#btnPut").click();
    await expectBannerContains(page, "max 100");

    await page.locator("#putName").fill("pw_invalid_bits");
    await page.locator("#putBits").fill("1 nope");
    await page.locator("#btnPut").click();
    await expectBannerContains(page, "Invalid value");

    await page.locator("#putBits").fill("4096");
    await page.locator("#btnPut").click();
    await expectBannerContains(page, "0 and 4095");
  });

  test("Put: de-dupes/sorts bits and double-click sends one request", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "elements");

    const name = `pw_put_${test.info().workerIndex}_${Date.now()}`;

    let putRequests = 0;
    page.on("request", (req) => {
      if (req.url().includes("/api/v1/elements/put")) putRequests += 1;
    });

    await page.locator("#putName").fill(name);
    await page.locator("#putBits").fill("3 2 2 1");
    await page.locator("#btnPut").dblclick();

    await expect(page.locator("#putOut")).toContainText(`Saved: ${name}`);
    expect(putRequests).toBe(1);

    await page.locator('.tab[data-tabgroup="elements"][data-tab="elements-get"]').click();
    await page.locator("#getName").fill(name);
    await page.locator("#btnGet").click();
    await expect(page.locator("#getOut")).toContainText("Bits: [1, 2, 3]");
  });

  test("Get: validates limit and shows NOT_FOUND for unknown element", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "elements");
    await page.locator('.tab[data-tabgroup="elements"][data-tab="elements-get"]').click();

    await page.locator("#getName").fill("alpha");
    await page.locator("#getLimit").fill("0");
    await page.locator("#btnGet").click();
    await expectBannerContains(page, "Invalid value");

    await page.locator("#getLimit").fill("4097");
    await page.locator("#btnGet").click();
    await expectBannerContains(page, "Invalid value");

    await page.locator("#getName").fill(`does_not_exist_${Date.now()}`);
    await page.locator("#getLimit").fill("200");
    await page.locator("#btnGet").click();
    await expectBannerContains(page, "No element found");
  });

  test("Get: supports low/high limits + copy buttons", async ({ context, page }) => {
    await context.grantPermissions(["clipboard-read", "clipboard-write"]);
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "elements");
    await page.locator('.tab[data-tabgroup="elements"][data-tab="elements-get"]').click();

    await page.locator("#getName").fill("alpha");
    await page.locator("#getLimit").fill("1");
    await page.locator("#btnGet").click();
    await expect(page.locator("#getOut")).toContainText("Returned: 1/1");

    await page.locator("#getLimit").fill("4096");
    await page.locator("#btnGet").click();
    await expect(page.locator("#getOut")).toContainText("Bits: [1, 2, 3]");

    await page.locator("#btnCopyBitsCsv").click();
    await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe("1,2,3");

    await page.locator("#btnCopyBitsList").click();
    await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe("1\n2\n3");
  });

  test("Matrix: renders and shows tooltip on hover", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "elements");
    await page.locator('.tab[data-tabgroup="elements"][data-tab="elements-matrix"]').click();

    await page.locator("#matrixName").fill("alpha");
    await page.locator("#btnMatrixFetch").click();

    await expect(page.locator("#matrixCanvas")).toBeVisible();
    await expect(page.locator("#matrixMeta")).toContainText("Found:");

    await page.locator("#matrixCanvas").hover({ position: { x: 8, y: 8 } });
    await expect(page.locator("#matrixTooltip")).not.toHaveClass(/hidden/);
    await expect(page.locator("#matrixTooltip")).toContainText(":");
  });
});

