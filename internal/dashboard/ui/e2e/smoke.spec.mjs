import { expect, test } from "@playwright/test";
import { installDashboardMocks, trackPageErrors } from "./dashboardMocks.mjs";

async function openDashboard(page, route = "overview") {
  await page.goto(`/dashboard/#${route}`);
  await expect(page.getByText("Connecting to command center…")).toHaveCount(0);
}

function tabButton(page, label) {
  return page.locator(".view-nav").getByRole("button", { name: new RegExp(`^${label}`) });
}

function pageHeading(page, label) {
  return page.getByRole("heading", { name: new RegExp(label) }).first();
}

async function clickWorkflowSubview(page, label) {
  await page.evaluate((targetLabel) => {
    const root = document.querySelector('[data-testid="workflow-subview-nav"]');
    if (!root) throw new Error("Missing workflow subview nav");
    const buttons = [...root.querySelectorAll("button")];
    const button = buttons.find((node) => node.textContent?.trim() === targetLabel);
    if (!button) throw new Error(`Missing workflow button: ${targetLabel}`);
    button.click();
  }, label);
}

async function clickFirstButtonByText(page, label) {
  await page.evaluate((targetLabel) => {
    const buttons = [...document.querySelectorAll("button")];
    const button = buttons.find((node) => node.textContent?.trim() === targetLabel);
    if (!button) throw new Error(`Missing button: ${targetLabel}`);
    button.click();
  }, label);
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
  await expect(pageHeading(page, "Operations")).toBeVisible();
  await expect(page.getByRole("button", { name: "Control + Mailbox", exact: true })).toBeVisible();

  await tabButton(page, "Health").click();
  await expect(page.getByRole("heading", { name: "Health" })).toBeVisible();
  await expect(page.getByText("Diagnostic Scope")).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("resolves deep links into consolidated subviews", async ({ page }) => {
  const errors = trackPageErrors(page);
  const workflowStatus = page.locator(".workflow-workbench-shell > .head .tiny.mono");

  await openDashboard(page, "observability/incidents");
  await expect(page.getByRole("heading", { name: "Observability" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Incident Response" })).toBeVisible();

  await openDashboard(page, "workflow/issues");
  await expect(page.getByText("Workflow Focus")).toBeVisible();
  await expect(workflowStatus).toHaveText(/issues active/);

  await openDashboard(page, "portfolio/triage");
  await expect(page.getByRole("heading", { name: "Workbench" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Portfolio Triage" })).toBeVisible();

  await openDashboard(page, "operations/tasks");
  await expect(pageHeading(page, "Operations")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Human Tasks" })).toBeVisible();

  await openDashboard(page, "operations/queue");
  await expect(pageHeading(page, "Operations")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Needs Action" })).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("agent task drilldown opens operations tasks with the selected item", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "agents");

  await expect(page.getByRole("heading", { name: "Agent Activity" })).toBeVisible();
  await page.locator("tr.agent-row").first().click();
  await page.locator(".agent-drop").getByRole("button", { name: "Open Task" }).first().click();

  await expect(page).toHaveURL(/#operations\/tasks$/);
  await expect(pageHeading(page, "Operations")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Human Tasks" })).toBeVisible();
  await expect(page.getByText("Selected Task", { exact: true })).toBeVisible();
  await expect(page.locator(".desc-text").last()).toHaveText("Review Alpha AI validation package and approve or reject.");

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("overview urgent queue pivots into the intended operator surfaces", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "overview");

  const urgentTable = page.getByText("Urgent Now", { exact: true }).locator("xpath=ancestor::div[contains(@class,'holding-detail-section')][1]");

  await urgentTable.getByRole("button", { name: "Open", exact: true }).first().click();
  await expect(page).toHaveURL(/#agents$/);

  await openDashboard(page, "overview");
  const urgentTableAgain = page.getByText("Urgent Now", { exact: true }).locator("xpath=ancestor::div[contains(@class,'holding-detail-section')][1]");
  await urgentTableAgain.getByRole("row").filter({ hasText: "mailbox" }).getByRole("button", { name: "Open", exact: true }).click();
  await expect(page).toHaveURL(/#operations\/queue$/);

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("overview workflow triage rows pivot into portfolio", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "overview");

  const triageSection = page.getByText("Workflow Triage", { exact: true }).locator("xpath=ancestor::div[contains(@class,'holding-detail-section')][1]");
  await triageSection.getByRole("button").first().click();

  await expect(page).toHaveURL(/#portfolio\/holding$/);
  await expect(page.getByRole("heading", { name: "Holding" })).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("agent quick actions open observability surfaces", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "agents");

  await expect(page.getByRole("heading", { name: "Agent Activity" })).toBeVisible();
  await page.locator("tr.agent-row").first().click();

  const agentDrop = page.locator(".agent-drop").first();

  await agentDrop.getByRole("button", { name: "Open Event Trace", exact: true }).click();
  await expect(page).toHaveURL(/#observability\/events$/);

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("agent quick actions open runtime logs", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "agents");

  await expect(page.getByRole("heading", { name: "Agent Activity" })).toBeVisible();
  await page.locator("tr.agent-row").first().click();

  const agentDrop = page.locator(".agent-drop").first();

  await agentDrop.getByRole("button", { name: "Open Runtime Logs", exact: true }).click();
  await expect(page).toHaveURL(/#observability\/logs$/);
  await expect(page.getByRole("heading", { name: "Logs" })).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("workflow workbench buttons sync the active subview into the route", async ({ page }) => {
  test.slow();
  const errors = trackPageErrors(page);
  await openDashboard(page, "workflow/flow");
  const workflowStatus = page.locator(".workflow-workbench-shell > .head .tiny.mono");

  await expect(page.getByText("Workflow Focus")).toBeVisible();

  await clickWorkflowSubview(page, "Issues");
  await expect(page).toHaveURL(/#workflow\/issues$/);
  await expect(workflowStatus).toHaveText(/issues active/);

  await clickWorkflowSubview(page, "Runs");
  await expect(page).toHaveURL(/#workflow\/runs$/);
  await expect(workflowStatus).toHaveText(/runs active/);

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("observability workbench buttons sync the active subview into the route", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "observability/overview");

  await expect(page.getByRole("heading", { name: "Workbench" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Investigation Hotspots" })).toBeVisible();

  const workbenchHead = page.locator(".observability-workbench-shell > .head");

  await workbenchHead.getByRole("button", { name: "Event Trace", exact: true }).click();
  await expect(page).toHaveURL(/#observability\/events$/);
  await expect(page.getByRole("heading", { name: "Event Flow" })).toBeVisible();

  await workbenchHead.getByRole("button", { name: "Runtime Logs", exact: true }).click();
  await expect(page).toHaveURL(/#observability\/logs$/);
  await expect(page.getByRole("heading", { name: "Logs" })).toBeVisible();

  await workbenchHead.getByRole("button", { name: "Incidents", exact: true }).click();
  await expect(page).toHaveURL(/#observability\/incidents$/);
  await expect(page.getByRole("heading", { name: "Incident Response" })).toBeVisible();

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

test("portfolio presets and saved views restore the intended workspace state", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "portfolio/overview");

  await expect(page.getByText("Portfolio Presets")).toBeVisible();

  await page.getByRole("button", { name: /^Drift Only/ }).click();
  await expect(page).toHaveURL(/#portfolio\/holding$/);
  await expect(page.getByRole("heading", { name: "Holding" })).toBeVisible();

  const savedViewCard = page.getByText("Saved View 1", { exact: true }).locator("xpath=ancestor::div[contains(@class,'health-card')][1]");
  await savedViewCard.getByRole("button", { name: "Save", exact: true }).click();
  await expect(savedViewCard.getByText(/holding/i)).toBeVisible();

  await page.getByRole("button", { name: "Overview", exact: true }).click();
  await expect(page).toHaveURL(/#portfolio\/overview$/);

  await savedViewCard.getByRole("button", { name: "Open", exact: true }).click();
  await expect(page).toHaveURL(/#portfolio\/holding$/);
  await expect(page.getByRole("heading", { name: "Holding" })).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("portfolio triage cards pivot into workflow and operations", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "portfolio/triage");

  await expect(page.getByRole("heading", { name: "Portfolio Triage" })).toBeVisible();

  const staleSection = page.getByText("Stale / Timer Heavy", { exact: true }).locator("xpath=ancestor::div[contains(@class,'holding-detail-section')][1]");
  await staleSection.getByRole("button", { name: "Workflow", exact: true }).first().click();
  await expect(page).toHaveURL(/#workflow\/flow$/);

  await openDashboard(page, "portfolio/triage");
  const humanSection = page.getByText("Human Needed / Retry Scans", { exact: true }).locator("xpath=ancestor::div[contains(@class,'holding-detail-section')][1]");
  await humanSection.getByRole("button", { name: "Operations", exact: true }).first().click();
  await expect(page).toHaveURL(/#operations\/(control|tasks)$/);

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("operations workbench buttons sync the active subview into the route", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "operations/queue");

  await expect(page.getByRole("heading", { name: "Workbench" })).toBeVisible();

  await page.getByRole("button", { name: "Control + Mailbox", exact: true }).click();
  await expect(page).toHaveURL(/#operations\/control$/);

  await page.getByRole("button", { name: "Tasks", exact: true }).click();
  await expect(page).toHaveURL(/#operations\/tasks$/);

  await page.getByRole("button", { name: "Needs Action", exact: true }).click();
  await expect(page).toHaveURL(/#operations\/queue$/);

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("operations queue opens mailbox detail in the control panel", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "operations/queue");

  await expect(page.getByRole("heading", { name: "Needs Action" })).toBeVisible();
  await page.getByLabel("Operations queue list").getByRole("button", { name: "Open Mailbox" }).click();

  await expect(page).toHaveURL(/#operations\/control$/);
  await expect(page.getByRole("heading", { name: "Mailbox + Decisions" })).toBeVisible();
  const selectedRequestCard = page.getByText("Selected Request", { exact: true }).locator("xpath=ancestor::div[contains(@class,'card')]");
  await expect(selectedRequestCard).toBeVisible();
  await expect(selectedRequestCard.getByRole("button", { name: "Workflow" })).toBeVisible();
  await expect(selectedRequestCard.getByText("alpha-ai")).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("operations mailbox detail pivots to workflow and related task", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "operations/queue");

  await page.getByLabel("Operations queue list").getByRole("button", { name: "Open Mailbox" }).click();
  const selectedRequestCard = page.getByText("Selected Request", { exact: true }).locator("xpath=ancestor::div[contains(@class,'card')]");

  await selectedRequestCard.getByRole("button", { name: "Workflow", exact: true }).click();
  await expect(page).toHaveURL(/#workflow\/flow$/);

  await openDashboard(page, "operations/control");
  const selectedRequestCardAgain = page.getByText("Selected Request", { exact: true }).locator("xpath=ancestor::div[contains(@class,'card')]");
  await selectedRequestCardAgain.getByRole("button", { name: "Related Task", exact: true }).click();
  await expect(page).toHaveURL(/#operations\/tasks$/);
  await expect(page.getByText("Selected Task", { exact: true })).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("operations queue opens task detail in the tasks panel", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "operations/queue");

  await expect(page.getByRole("heading", { name: "Needs Action" })).toBeVisible();
  await page.getByLabel("Operations queue list").getByRole("button", { name: "Open Task" }).click();

  await expect(page).toHaveURL(/#operations\/tasks$/);
  await expect(page.getByRole("heading", { name: "Human Tasks" })).toBeVisible();
  await expect(page.getByText("Selected Task", { exact: true })).toBeVisible();
  await expect(page.locator(".desc-text").last()).toHaveText("Review Alpha AI validation package and approve or reject.");

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("operations task detail pivots to portfolio and related mailbox", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "operations/queue");

  await page.getByLabel("Operations queue list").getByRole("button", { name: "Open Task" }).click();
  const selectedTaskPane = page.locator("section").filter({ has: page.getByText("Selected Task", { exact: true }) }).last();

  await selectedTaskPane.getByRole("button", { name: "Related Mailbox", exact: true }).click();
  await expect(page).toHaveURL(/#operations\/control$/);
  await expect(page.getByText("Selected Request", { exact: true })).toBeVisible();

  await openDashboard(page, "operations/queue");
  await page.getByLabel("Operations queue list").getByRole("button", { name: "Open Task" }).click();
  const selectedTaskPaneAgain = page.locator("section").filter({ has: page.getByText("Selected Task", { exact: true }) }).last();
  await selectedTaskPaneAgain.getByRole("button", { name: "Portfolio", exact: true }).click();
  await expect(page).toHaveURL(/#portfolio\/holding$/);

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("operations task detail pivots to workflow", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "operations/queue");

  await page.getByLabel("Operations queue list").getByRole("button", { name: "Open Task" }).click();
  const selectedTaskPane = page.locator("section").filter({ has: page.getByText("Selected Task", { exact: true }) }).last();

  await selectedTaskPane.getByRole("button", { name: "Workflow", exact: true }).click();
  await expect(page).toHaveURL(/#workflow\/flow$/);
  await expect(page.getByText("Workflow Focus")).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("task completion refreshes the tasks surface", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "operations/queue");

  await page.getByLabel("Operations queue list").getByRole("button", { name: "Open Task" }).click();
  await expect(page).toHaveURL(/#operations\/tasks$/);
  await expect(page.getByText("Selected Task", { exact: true })).toBeVisible();

  await page.getByLabel("Task completion result").fill("Validated and approved.");
  await page.getByRole("button", { name: "Complete", exact: true }).click();

  await expect(page.getByText("Select a task to claim/complete it.")).toBeVisible();
  await expect(page.getByLabel("Task list").getByText("Review Alpha AI validation package and approve or reject.")).toHaveCount(0);

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("task rejection refreshes the tasks surface", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "operations/queue");

  await page.getByLabel("Operations queue list").getByRole("button", { name: "Open Task" }).click();
  await expect(page).toHaveURL(/#operations\/tasks$/);
  await expect(page.getByText("Selected Task", { exact: true })).toBeVisible();

  await page.getByLabel("Task rejection reason").fill("Insufficient review package.");
  await page.getByRole("button", { name: "Reject", exact: true }).click();

  await expect(page.getByText("Select a task to claim/complete it.")).toBeVisible();
  await expect(page.getByLabel("Task list").getByText("Review Alpha AI validation package and approve or reject.")).toHaveCount(0);

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("mailbox decision refreshes the mailbox surface", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "operations/queue");

  await page.getByLabel("Operations queue list").getByRole("button", { name: "Open Mailbox" }).click();
  await expect(page).toHaveURL(/#operations\/control$/);
  await expect(page.getByText("Selected Request", { exact: true })).toBeVisible();

  await page.getByRole("button", { name: "Decide", exact: true }).click();

  const pendingCard = page.getByText("Pending", { exact: true }).locator("xpath=ancestor::div[contains(@class,'metric-card')][1]");
  await expect(pendingCard.getByText("0", { exact: true })).toBeVisible();
  const selectedRequestCard = page.getByText("Selected Request", { exact: true }).locator("xpath=ancestor::div[contains(@class,'card')]");
  await expect(selectedRequestCard.getByText("approved")).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("health vertical deploy row opens portfolio for a vertical", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "health");

  const verticalRow = page.getByRole("row").filter({ has: page.getByRole("button", { name: "alpha-ai", exact: true }) }).first();
  await verticalRow.getByRole("button", { name: "Portfolio", exact: true }).click();
  await expect(page).toHaveURL(/#portfolio\/holding$/);

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("health vertical deploy row opens workflow for a vertical", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "health");

  const verticalRow = page.getByRole("row").filter({ has: page.getByRole("button", { name: "alpha-ai", exact: true }) }).first();
  await verticalRow.getByRole("button", { name: "Workflow", exact: true }).click();
  await expect(page).toHaveURL(/#workflow\/flow$/);
  await expect(page.getByText("Workflow Focus")).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("observability log detail pivots into workflow", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "observability/logs");

  await expect(page.getByRole("heading", { name: "Logs" })).toBeVisible();
  await page.locator(".timeline-item.runtime-log-item").first().click();
  const logDetail = page.locator("section").filter({ has: page.getByRole("heading", { name: "Log Detail" }) }).last();
  await logDetail.getByRole("button", { name: "Workflow", exact: true }).click();

  await expect(page).toHaveURL(/#workflow\/flow$/);
  await expect(page.getByText("Workflow Focus")).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("observability log detail pivots into incidents", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "observability/logs");

  await expect(page.getByRole("heading", { name: "Logs" })).toBeVisible();

  await page.locator(".timeline-item.runtime-log-item").first().click();
  await page.getByRole("button", { name: "Related Incidents", exact: true }).click();

  await expect(page).toHaveURL(/#observability\/incidents$/);
  await expect(page.getByRole("heading", { name: "Incident Response" })).toBeVisible();

  expect(errors.map((error) => error.message)).toEqual([]);
});

test("health diagnostics pivots route to the intended investigation surfaces", async ({ page }) => {
  const errors = trackPageErrors(page);
  await openDashboard(page, "health");

  await expect(page.getByRole("heading", { name: "Health" })).toBeVisible();

  await page.getByRole("button", { name: "Open Workflow Issues" }).first().click();
  await expect(page).toHaveURL(/#workflow\/issues$/);

  await tabButton(page, "Health").click();
  await page.getByRole("button", { name: "Open Portfolio" }).first().click();
  await expect(page).toHaveURL(/#portfolio\/holding$/);

  await tabButton(page, "Health").click();
  await page.getByRole("button", { name: "Open Logs" }).first().click();
  await expect(page).toHaveURL(/#observability\/logs$/);

  expect(errors.map((error) => error.message)).toEqual([]);
});
