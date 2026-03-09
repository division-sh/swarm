import test from "node:test";
import assert from "node:assert/strict";
import { deriveOverviewState } from "./useOverviewDerivedState.js";

test("deriveOverviewState builds an urgent now queue across dashboard domains", () => {
  const result = deriveOverviewState({
    agentsResp: {
      agents: [
        { id: "agent-1", state: "stuck", role: "manager", vertical_slug: "holding", pending_events: 3, stuck_reason: "waiting on review" },
      ],
    },
    incidentsData: [
      { code: "MCP_TIMEOUT", count: 2, component: "workflow" },
    ],
    mailbox: {
      items: [
        { id: "mb-1", status: "pending", priority: "critical", summary: "Review Alpha", from_agent: "holding-manager", vertical_slug: "alpha" },
      ],
    },
    tasksResp: {
      tasks: [
        { id: "task-1", status: "open", priority: "p1", description: "Approve package", requesting_agent: "holding-manager", vertical_slug: "alpha" },
      ],
    },
    holdingData: {
      verticals: [
        { id: "v-1", slug: "alpha", stage: "ready_for_review", workflow_current_stage: "validation", active_timer_count: 1 },
      ],
    },
  });

  assert.equal(result.urgentNow.length, 5);
  assert.equal(result.urgentNow[0].kind, "agent");
  assert.equal(result.urgentNow.some((item) => item.kind === "mailbox"), true);
  assert.equal(result.urgentNow.some((item) => item.kind === "incident"), true);
  assert.equal(result.urgentNow.some((item) => item.kind === "task"), true);
  assert.equal(result.urgentNow.some((item) => item.kind === "vertical"), true);
});
