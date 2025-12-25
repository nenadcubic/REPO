import { expect, test } from "@playwright/test";
import { waitForAppReady } from "./_helpers";

test('home page loads', async ({ page }) => {
  await waitForAppReady(page);
  await expect(page).toHaveTitle(/element-redis/i);
});
