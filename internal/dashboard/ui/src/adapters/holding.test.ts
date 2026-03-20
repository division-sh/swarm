import assert from "node:assert/strict";
import test from "node:test";
import { adaptHolding } from "./holding.ts";

test("adaptHolding derives holding response from generic instances and agents", () => {
  const out = adaptHolding([
    {
      instance_id: "alpha",
      workflow_version: "1",
      current_state: "validation",
      entered_stage_at: "2026-03-18T12:00:00Z",
      timer_state: [{ timer_id: "t1", cancelled: false }],
      metadata: { slug: "alpha", name: "Alpha", stage: "scoring", revision_count: 2, geography: "US" },
      updated_at: "2026-03-19T12:00:00Z",
    },
    {
      instance_id: "beta",
      current_state: "killed",
      entered_stage_at: "2026-03-18T12:00:00Z",
      timer_state: [],
      metadata: { slug: "beta", stage: "killed", kill_reason: "failed" },
      updated_at: "2026-03-19T12:00:00Z",
    },
  ], [
    { id: "agent-1", entity_id: "alpha", state: "running" },
    { id: "agent-2", entity_id: "alpha", state: "terminated" },
  ]);

  assert.equal(out.summary.total, 2);
  assert.equal(out.summary.killed, 1);
  assert.equal(out.workflow_summary.drift, 1);
  assert.equal(out.workflow_summary.active_timers, 1);
  assert.equal(out.workflow_summary.revisioned, 1);
  assert.equal(out.agent_counts.alpha?.total, 2);
  assert.equal(out.agent_counts.alpha?.active, 1);
});
