import { expect, request as requestFactory, type APIRequestContext, type Page } from "@playwright/test";

export function guiBaseURL(): string {
  return process.env.GUI_BASE_URL ?? "http://localhost:18080";
}

export function apiBaseURL(): string {
  return process.env.API_BASE_URL ?? "http://localhost:18000/api/v1";
}

export async function apiContext(): Promise<APIRequestContext> {
  const baseURL = apiBaseURL().replace(/\/+$/, "") + "/";
  return await requestFactory.newContext({ baseURL });
}

export async function apiGet<T>(path: string): Promise<T> {
  const request = await apiContext();
  try {
    const res = await request.get(path.replace(/^\/+/, ""));
    const body = await res.json().catch(() => null);
    if (!res.ok()) throw new Error(`GET ${path} failed: ${res.status()} ${JSON.stringify(body)}`);
    return body as T;
  } finally {
    await request.dispose();
  }
}

export async function apiPost<T>(path: string, data: unknown): Promise<T> {
  const request = await apiContext();
  try {
    const res = await request.post(path.replace(/^\/+/, ""), { data });
    const body = await res.json().catch(() => null);
    if (!res.ok()) throw new Error(`POST ${path} failed: ${res.status()} ${JSON.stringify(body)}`);
    return body as T;
  } finally {
    await request.dispose();
  }
}

export async function apiPut<T>(path: string, data: unknown): Promise<T> {
  const request = await apiContext();
  try {
    const res = await request.put(path.replace(/^\/+/, ""), { data });
    const body = await res.json().catch(() => null);
    if (!res.ok()) throw new Error(`PUT ${path} failed: ${res.status()} ${JSON.stringify(body)}`);
    return body as T;
  } finally {
    await request.dispose();
  }
}

export async function putElement(name: string, bits: number[], ns: string): Promise<void> {
  await apiPost("elements/put?ns=" + encodeURIComponent(ns), { name, bits });
}

export async function seedExample(exampleId: string, ns: string, reset: boolean): Promise<void> {
  const request = await apiContext();
  try {
    const res = await request.post(`examples/${encodeURIComponent(exampleId)}/run`, { data: { ns, reset } });
    if (!res.ok()) throw new Error(`seedExample failed: ${res.status()} ${await res.text()}`);
  } finally {
    await request.dispose();
  }
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

export async function navTo(
  page: Page,
  view: "status" | "elements" | "queries" | "store" | "examples" | "logs" | "bitmaps",
): Promise<void> {
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

export async function expectBannerContains(page: Page, text: string): Promise<void> {
  await expect(page.locator("#banner")).toBeVisible();
  await expect(page.locator("#banner")).toContainText(text);
}

export async function clearAndType(page: Page, selector: string, value: string): Promise<void> {
  const el = page.locator(selector);
  await expect(el).toBeVisible();
  await el.fill(value);
}
