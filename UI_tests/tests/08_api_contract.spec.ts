import { expect, test } from "@playwright/test";
import { apiContext } from "./_helpers";

test.describe("API contract (backend logical assumptions)", () => {
  test("health is ok and includes redis metrics", async () => {
    const request = await apiContext();
    try {
      const res = await request.get("health");
      expect(res.ok()).toBeTruthy();
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.data.redis.ok).toBe(true);
      expect(typeof body.data.backend_version).toBe("string");
    } finally {
      await request.dispose();
    }
  });

  test("validation errors return {ok:false,error:{code,message,details}}", async () => {
    const request = await apiContext();
    try {
      const res = await request.get("elements/get?ns=er&name=&limit=200");
      expect(res.status()).toBe(422);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(typeof body.error.code).toBe("string");
      expect(typeof body.error.message).toBe("string");
      expect(body.error.details).toBeTruthy();
    } finally {
      await request.dispose();
    }
  });

  test("limits are enforced (logs tail, explorer page_size)", async () => {
    const request = await apiContext();
    try {
      const logs = await request.get("logs?tail=999999");
      expect(logs.status()).toBe(422);

      const explorer = await request.get("explorer/namespaces/er/elements?page=1&page_size=500");
      expect(explorer.status()).toBe(422);
    } finally {
      await request.dispose();
    }
  });
});

