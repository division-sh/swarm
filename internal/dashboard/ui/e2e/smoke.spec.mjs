import { expect, test } from "@playwright/test";
import { installDashboardMocks, trackPageErrors } from "./dashboardMocks.mjs";

async function openDashboard(page, route = "overview") {
  await page.goto(`/dashboard/#${route}`);
  await expect(page.getByText("Connecting to command center…")).toHaveCount(0);
}

function tabButton(page, label) {
  return page.locator(".view-nav").getByRole("button", { name: new RegExp(`^${label}`) });
}

test.beforeEach(async ({ page }) => {
  await installDashboardMocks(page);
});

test("loads the dashboard shell and overview", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "overview");

  await expect(tabButton(page, "Overview")).toBeVisible();
  await expect(tabButton(page, "Agents")).toBeVisible();
  await expect(tabButton(page, "Observability")).toBeVisible();
  await expect(tabButton(page, "Workflow")).toBeVisible();
  await expect(tabButton(page, "Portfolio")).toBeVisible();
  await expect(tabButton(page, "Operations")).toBeVisible();
  await expect(tabButton(page, "Health")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await expect(page.getByText("Workflow Audit Warnings")).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("renders the main top-level tabs", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "overview");

  await tabButton(page, "Agents").click();
  await expect(page.getByRole("heading", { name: "Agent Activity" })).toBeVisible();
  await page.locator("tr.agent-row").first().click();
  await expect(page.getByText("Agent Health").first()).toBeVisible();

  await tabButton(page, "Observability").click();
  await expect(page.getByRole("heading", { name: "Observability" })).toBeVisible();
  await expect(page.getByText("Focus Context")).toBeVisible();

  await tabButton(page, "Workflow").click();
  await expect(page.getByText("Workflow Focus")).toBeVisible();
  await expect(page.getByRole("button", { name: "Trace", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Runs", exact: true })).toBeVisible();

  await tabButton(page, "Portfolio").click();
  await expect(page.getByRole("heading", { name: /Portfolio/ }).first()).toBeVisible();
  await expect(page.getByRole("heading", { name: "Workbench" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Triage" })).toBeVisible();

  await tabButton(page, "Operations").click();
  await expect(page.getByRole("heading", { name: "Operations" })).toBeVisible();
  await expect(page.getByText("Control + Mailbox")).toBeVisible();

  await tabButton(page, "Health").click();
  await expect(page.getByRole("heading", { name: "Health" })).toBeVisible();
  await expect(page.getByText("Diagnostic Scope")).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("resolves deep links into consolidated subviews", async ({ page }) => {
  const errors = trackPageErrors(page);

  await openDashboard(page, "observability/incidents");
  await expect(page.getByRole("heading", { name: "Observability" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Incident Response" })).toBeVisible();

  await openDashboard(page, "workflow/issues");
  await expect(page.getByText("Workflow Focus")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Issues" })).toBeVisible();

  await openDashboard(page, "portfolio/triage");
  await expect(page.getByRole("heading", { name: "Workbench" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Portfolio Triage" })).toBeVisible();

  await openDashboard(page, "operations/tasks");
  await expect(page.getByRole("heading", { name: "Operations" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Human Tasks" })).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("agent task drilldown opens operations tasks with the selected item", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "agents");

  await expect(page.getByRole("heading", { name: "Agent Activity" })).toBeVisible();
  await page.locator("tr.agent-row").first().click();
  await page.locator(".agent-drop").getByRole("button", { name: "Open Task" }).first().click();

  await expect(page).toHaveURL(/#operations\/tasks$/);
  await expect(page.getByRole("heading", { name: "Operations" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Human Tasks" })).toBeVisible();
  const selectedTaskPanel = page.locator(".row.body > div").nth(1);
  await expect(selectedTaskPanel.getByText("Selected Task")).toBeVisible();
  await expect(selectedTaskPanel.locator(".desc-text")).toHaveText("Review Alpha AI validation package and approve or reject.");

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("workflow workbench buttons sync the active subview into the route", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "workflow/flow");

  await expect(page.getByText("Workflow Focus")).toBeVisible();

  await page.getByRole("button", { name: "Issues", exact: true }).click();
  await expect(page).toHaveURL(/#workflow\/issues$/);
  await expect(page.getByRole("heading", { name: "Issues" })).toBeVisible();

  await page.getByRole("button", { name: "Compare", exact: true }).click();
  await expect(page).toHaveURL(/#workflow\/compare$/);
  await expect(page.getByRole("heading", { name: "Compare" })).toBeVisible();

  await page.getByRole("button", { name: "Runs", exact: true }).click();
  await expect(page).toHaveURL(/#workflow\/runs$/);
  await expect(page.getByRole("heading", { name: "Runs" })).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("portfolio workbench buttons sync the active subview into the route", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "portfolio/overview");

  await expect(page.getByRole("heading", { name: "Workbench" })).toBeVisible();

  await page.getByRole("button", { name: "Triage", exact: true }).click();
  await expect(page).toHaveURL(/#portfolio\/triage$/);
  await expect(page.getByRole("heading", { name: "Portfolio Triage" })).toBeVisible();

  await page.getByRole("button", { name: "Board", exact: true }).click();
  await expect(page).toHaveURL(/#portfolio\/holding$/);
  await expect(page.getByRole("heading", { name: "Holding" })).toBeVisible();

  await page.locator(".portfolio-workbench-shell > .head").getByRole("button", { name: "Funnel", exact: true }).click();
  await expect(page).toHaveURL(/#portfolio\/pipeline$/);
  await expect(page.getByRole("heading", { name: "Pipeline Funnel" })).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});
