import test from "node:test";
import assert from "node:assert/strict";
import { deriveObservabilityState } from "./useObservabilityDerivedState.ts";

test("deriveObservabilityState summarizes counts and hotspots", () => {
  const derived = deriveObservabilityState({
    events: {
      state: {
        filteredEvents: [
          { id: "evt-1", type: "runtime.error", error_count: 1, pending_count: 2, source_agent: "agent-alpha", vertical_slug: "alpha-ai" },
          { id: "evt-2", type: "mailbox.request", error_count: 0, pending_count: 0 },
        ],
        filteredRuntimeLogs: [
          { id: "rlog-1", level: "error", component: "workflow", action: "step", agent_id: "agent-alpha", vertical_id: "alpha-ai" },
        ],
      },
    },
    logs: {
      state: {
        filteredLogsData: [
          { id: "log-1", error_code: "E_TIMEOUT", component: "mcp", action: "call", agent_id: "agent-beta", vertical_id: "beta-labs" },
        ],
      },
    },
    incidents: {
      state: {
        incidentsData: [
          { code: "MCP_TIMEOUT", count: 3, root_cause: "tool timeout", agents: ["agent-beta"] },
        ],
      },
    },
    focus: {
      agent: "agent-alpha",
      vertical: "alpha-ai",
      chips: ["agent:agent-alpha", "vertical:alpha-ai"],
    },
  });

  assert.equal(derived.summary.filteredEvents, 2);
  assert.equal(derived.summary.runtimeErrors, 2);
  assert.equal(derived.summary.criticalIncidents, 1);
  assert.equal(derived.summary.focusActive, 2);
  assert.equal(derived.hotspots.length, 3);
  assert.equal(derived.hotspots[0].kind, "incident");
});
