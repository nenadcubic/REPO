import { expect, test } from "@playwright/test";
import { expectBannerContains, navTo, putElement, selectNamespace, waitForAppReady } from "./_helpers";

test.describe("Queries: all tabs + negative cases", () => {
  const ns = "er";
  const uniq = String(Date.now());
  const a = 3991;
  const b = 3992;
  const c = 3993;
  const d = 3994;
  const e = 3995;

  const nameAll = `pw_q_all_${uniq}`;
  const nameOnlyA = `pw_q_only_a_${uniq}`;
  const nameOnlyB = `pw_q_only_b_${uniq}`;
  const nameOnlyC = `pw_q_only_c_${uniq}`;
  const nameOnlyD = `pw_q_only_d_${uniq}`;
  const nameOnlyE = `pw_q_only_e_${uniq}`;

  test.beforeAll(async () => {
    await putElement(nameAll, [a, b], ns);
    await putElement(nameOnlyA, [a], ns);
    await putElement(nameOnlyB, [b], ns);
    await putElement(nameOnlyC, [c], ns);
    await putElement(nameOnlyD, [d], ns);
    await putElement(nameOnlyE, [e], ns);
  });

  test("Find: rejects empty/out-of-range bit, then returns expected names", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, ns);
    await navTo(page, "queries");

    await page.locator("#qBit").fill("");
    await page.locator("#btnQuery").click();
    await expectBannerContains(page, "required");

    await page.locator("#qBit").fill("4096");
    await page.locator("#btnQuery").click();
    await expectBannerContains(page, "0–4095");

    await page.locator("#qBit").fill(String(a));
    await page.locator("#btnQuery").click();
    await expect(page.locator("#queryNames")).toContainText(nameAll);
    await expect(page.locator("#queryNames")).toContainText(nameOnlyA);
  });

  test("Find ALL/ANY/NOT/Universe NOT: validates minimum bits and returns expected names", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, ns);
    await navTo(page, "queries");

    const tabAll = page.locator('.tab[data-tabgroup="queries"][data-tab="q-find-all"]');
    await tabAll.click();
    await expect(tabAll).toHaveClass(/active/);
    await page.locator("#qBitsAll").fill(String(a));
    await page.locator("#btnQuery").click();
    await expectBannerContains(page, "Invalid value");

    await page.locator("#qBitsAll").fill(`${a} ${b}`);
    await page.locator("#btnQuery").click();
    await expect(page.locator("#queryNames")).toContainText(nameAll);
    const allNames = (await page.locator("#queryNames").innerText()).split("\n").filter(Boolean);
    expect(allNames).toContain(nameAll);
    expect(allNames).not.toContain(nameOnlyA);
    expect(allNames).not.toContain(nameOnlyB);

    const tabAny = page.locator('.tab[data-tabgroup="queries"][data-tab="q-find-any"]');
    await tabAny.click();
    await expect(tabAny).toHaveClass(/active/);
    await page.locator("#qBitsAny").fill(`${c} ${d}`);
    await page.locator("#btnQuery").click();
    await expect(page.locator("#queryNames")).toContainText(nameOnlyC);
    const anyNames = (await page.locator("#queryNames").innerText()).split("\n").filter(Boolean);
    expect(anyNames).toContain(nameOnlyC);
    expect(anyNames).toContain(nameOnlyD);

    const tabNot = page.locator('.tab[data-tabgroup="queries"][data-tab="q-find-not"]');
    await tabNot.click();
    await expect(tabNot).toHaveClass(/active/);
    await page.locator("#qIncludeBit").fill(String(a));
    await page.locator("#qExcludeBits").fill("");
    await page.locator("#btnQuery").click();
    await expectBannerContains(page, "Invalid value");

    await page.locator("#qExcludeBits").fill(String(b));
    await page.locator("#btnQuery").click();
    await expect(page.locator("#queryNames")).toContainText(nameOnlyA);
    const notNames = (await page.locator("#queryNames").innerText()).split("\n").filter(Boolean);
    expect(notNames).toContain(nameOnlyA);
    expect(notNames).not.toContain(nameAll);

    const tabUNot = page.locator('.tab[data-tabgroup="queries"][data-tab="q-find-unot"]');
    await tabUNot.click();
    await expect(tabUNot).toHaveClass(/active/);
    await page.locator("#qExcludeUniverseBits").fill(String(a));
    await page.locator("#btnQuery").click();
    await expect(page.locator("#queryNames")).toContainText(nameOnlyE);
    const uNotNames = (await page.locator("#queryNames").innerText()).split("\n").filter(Boolean);
    expect(uNotNames).toContain(nameOnlyE);
    expect(uNotNames).not.toContain(nameOnlyA);
    expect(uNotNames).not.toContain(nameAll);
  });

  test("Query limit: empty uses default, too-large rejected", async ({ page }) => {
    await waitForAppReady(page);
    await selectNamespace(page, ns);
    await navTo(page, "queries");

    await page.locator("#queryLimit").fill("5001");
    await page.locator("#qBit").fill(String(a));
    await page.locator("#btnQuery").click();
    await expectBannerContains(page, "Invalid value");
  });
});
