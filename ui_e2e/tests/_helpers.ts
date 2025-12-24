import { expect, request as requestFactory, type APIRequestContext, type Page } from "@playwright/test";

export function apiBaseURL(): string {
  return process.env.API_BASE_URL ?? "http://localhost:18000/api/v1";
}

export async function apiContext(): Promise<APIRequestContext> {
  const baseURL = apiBaseURL().replace(/\/+$/, "") + "/";
  return await requestFactory.newContext({ baseURL });
}

export async function seedExample(exampleId: string, ns: string, reset: boolean): Promise<void> {
  const request = await apiContext();
  const res = await request.post(`examples/${encodeURIComponent(exampleId)}/run`, {
    data: { ns, reset },
  });
  if (!res.ok()) {
    throw new Error(`seedExample failed: ${res.status()} ${await res.text()}`);
  }
  await request.dispose();
}

export async function waitForAppReady(page: Page): Promise<void> {
  await page.goto("/", { waitUntil: "domcontentloaded" });
  await expect(page.locator("#viewTitle")).toBeVisible();
  await expect(page.locator("#nsSelect")).toBeVisible();
}

export async function selectNamespace(page: Page, ns: string): Promise<void> {
  const select = page.locator("#nsSelect");
  await expect(select).toBeVisible();
  await select.selectOption(ns);
  await expect(select).toHaveValue(ns);
}

export async function navTo(page: Page, view: "status" | "elements" | "queries" | "store" | "examples" | "logs" | "bitmaps"): Promise<void> {
  await page.locator(`.nav-item[data-view="${view}"]`).click();
  const expectedTitle =
    view === "status"
      ? "System Status"
      : view === "elements"
        ? "Element Operations"
        : view === "queries"
          ? "Find Matching Elements"
          : view === "store"
            ? "Store Results with Expiry"
            : view === "examples"
              ? "Examples"
              : view === "logs"
                ? "Backend Logs (read-only)"
                : "Bit-maps";
  await expect(page.locator("#viewTitle")).toHaveText(expectedTitle);
}

export async function runQueryFind(page: Page, bit: string): Promise<void> {
  await navTo(page, "queries");
  await page.locator('.tab[data-tabgroup="queries"][data-tab="q-find"]').click();
  await page.locator("#qBit").fill(bit);
  await page.locator("#btnQuery").click();
  await expect(page.locator("#queryNames")).toBeVisible();
}
