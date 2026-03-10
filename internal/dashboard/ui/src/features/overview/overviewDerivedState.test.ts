import test from "node:test";
import assert from "node:assert/strict";
import { deriveOverviewState } from "./useOverviewDerivedState.ts";

test("deriveOverviewState prioritizes urgent items", () => {
  const result = deriveOverviewState({
    agentsResp: {
      agents: [
        { id: "agent-1", state: "stuck", stuck_reason: "blocked", pending_events: 3, role: "holding-manager", vertical_slug: "alpha" },
      ],
      states: {},
    },
    incidentsData: [{ code: "MCP_TIMEOUT", component: "mcp", count: 2 }],
    mailbox: {
      summary: {},
      items: [{ id: "mb-1", status: "pending", summary: "Approve alpha", priority: "critical", from_agent: "holding-manager", vertical_slug: "alpha" }],
    },
    tasksResp: {
      tasks: [{ id: "task-1", status: "open", description: "Review alpha", priority: "p1", requesting_agent: "holding-manager", vertical_slug: "alpha" }],
      weekly_budget: {},
    },
    holdingData: {
      campaigns: [],
      verticals: [{ id: "v-1", slug: "alpha", stage: "scoring", workflow_current_stage: "validation", active_timer_count: 1 }],
      agent_counts: {},
      summary: {},
      workflow_summary: {},
    },
  });

  assert.equal(result.urgentNow.length, 5);
  assert.equal(result.urgentNow[0].kind, "agent");
});
