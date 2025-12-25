import { expect, test } from "@playwright/test";
import { apiGet, apiPut, navTo, selectNamespace, waitForAppReady } from "./_helpers";

type BitmapsResponse = {
  ok: true;
  data: { document?: unknown; items?: unknown[] };
};

test.describe("Bit-maps: browse + edit-mode validations (restored)", () => {
  test("browse and client-side search filter work", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, "er");
    await navTo(page, "bitmaps");

    await page.locator("#btnBitmapsRefresh").click();
    await expect(page.locator("#bitmapsTbody tr").first()).toBeVisible();
    await expect(page.locator("#bitmapsMeta")).toContainText("Loaded:");

    await page.locator("#bitmapsSearch").fill("risk");
    await expect(page.locator("#bitmapsMeta")).toBeVisible();
  });

  test("edit mode: invalid bit/group rejected; group CRUD + bulk assign produce output; restores original document", async ({ page }) => {
    const original = await apiGet<BitmapsResponse>("bitmaps?ns=er");
    const originalDoc = original?.data?.document;
    expect(originalDoc).toBeTruthy();

    try {
      await waitForAppReady(page);
      await selectNamespace(page, "er");
      await navTo(page, "bitmaps");
      await page.locator("#btnBitmapsRefresh").click();

      await page.locator("#bitmapsEditMode").check();
      await expect(page.locator("#bitmapsEditOnly")).not.toHaveClass(/hidden/);

      await page.locator("#bitmapsTbody tr").first().click();
      await expect(page.locator("#bitmapsItemPanel")).not.toHaveClass(/hidden/);
      await expect(page.locator("#bmEditBit")).not.toHaveValue("");

      await page.evaluate(() => {
        const bitEl = document.getElementById("bmEditBit") as HTMLInputElement | null;
        if (bitEl) bitEl.value = "5000";
      });
      await page.locator("#btnBmItemSave").click();
      await expect(page.locator("#banner")).toContainText("between 0 and 4095");

      await page.evaluate(() => {
        const bitEl = document.getElementById("bmEditBit") as HTMLInputElement | null;
        const groupEl = document.getElementById("bmEditGroup") as HTMLSelectElement | null;
        if (bitEl) bitEl.value = "1";
        if (groupEl) groupEl.value = "pw_nonexistent_group";
      });
      await page.locator("#btnBmItemSave").click();
      await expect(page.locator("#banner")).toContainText("Invalid value");

      const groupId = `pw_tmp_${Date.now()}`;
      await page.locator("#bmGroupId").fill(groupId);
      await page.locator("#bmGroupLabel").fill("PW Tmp");
      await page.locator("#bmGroupOrder").fill("999");
      await page.locator("#bmGroupColor").fill("teal");
      await page.locator("#btnBmGroupSave").click();
      await expect(page.locator("#banner")).toContainText("Saved");
      await expect(page.locator(`#bmGroupsTbody tr[data-group="${groupId}"]`)).toBeVisible();

      await page.locator("#bmBulkText").fill(`${groupId}: 10-12`);
      await page.locator("#btnBmBulkApply").click();
      await expect(page.locator("#bmBulkOut")).not.toHaveText("");

      page.once("dialog", (d) => d.accept());
      await page.locator(`#bmGroupsTbody tr[data-group="${groupId}"] button[data-action="delete"]`).click();
      await expect(page.locator("#banner")).toContainText("Saved");
      await expect(page.locator(`#bmGroupsTbody tr[data-group="${groupId}"]`)).toHaveCount(0);
    } finally {
      await apiPut("bitmaps?ns=er", originalDoc);
    }
  });
});
