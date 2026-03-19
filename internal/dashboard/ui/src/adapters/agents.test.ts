import assert from "node:assert/strict";
import test from "node:test";
import { adaptAgents } from "./agents.ts";

test("adaptAgents maps generic agent rows into dashboard agent response", () => {
  const out = adaptAgents([
    {
      id: "agent-1",
      role: "worker",
      type: "stub",
      mode: "operating",
      status: "active",
      entity_id: "entity-1",
      started_at: "2026-03-17T12:00:00Z",
    },
    {
      id: "agent-2",
      status: "terminated",
    },
  ]);

  assert.equal(out.agents.length, 2);
  assert.equal(out.agents[0].id, "agent-1");
  assert.equal(out.agents[0].state, "idle");
  assert.equal(out.agents[1].state, "terminated");
  assert.equal(out.states.idle, 1);
  assert.equal(out.states.terminated, 1);
});
