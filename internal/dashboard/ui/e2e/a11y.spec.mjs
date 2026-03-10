import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { installDashboardMocks } from "./dashboardMocks.mjs";

async function openDashboard(page, route = "overview") {
  await page.goto(`/dashboard/#${route}`);
  await expect(page.getByText("Connecting to command center…")).toHaveCount(0);
}

test.beforeEach(async ({ page }) => {
  await installDashboardMocks(page);
});

for (const route of ["overview", "portfolio/overview", "operations/queue", "health"]) {
  test(`axe smoke: ${route}`, async ({ page }) => {
    test.setTimeout(60000);
    await openDashboard(page, route);
    const builder = new AxeBuilder({ page });
    builder.include("main");
    const results = await builder.analyze();
    expect(results.violations, JSON.stringify(results.violations, null, 2)).toEqual([]);
  });
}
