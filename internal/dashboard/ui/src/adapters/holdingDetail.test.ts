import assert from "node:assert/strict";
import test from "node:test";
import { adaptHoldingDetail } from "./holdingDetail.ts";

test("adaptHoldingDetail keeps runtime-owned detail and omits product artifacts", () => {
  const out = adaptHoldingDetail({
    instance: {
      instance_id: "vertical-1",
      workflow_name: "venture",
      workflow_version: "v1",
      current_state: "scoring",
      entered_stage_at: "2026-03-19T10:00:00Z",
      transition_history: [{ transition_id: "t1" }],
      timer_state: [{ timer_id: "timer-1", cancelled: false }],
      state_buckets: { scores: [80, 90] },
      metadata: { slug: "alpha", name: "Alpha", stage: "validation", composite_score: 88 },
      created_at: "2026-03-19T08:00:00Z",
      updated_at: "2026-03-19T12:00:00Z",
    },
    agents: [{ id: "agent-1", entity_id: "vertical-1", current_task_id: "task-1", started_at: "2026-03-19T09:00:00Z" }],
    events: [{ id: "e1", type: "venture.scored", created_at: "2026-03-19T12:00:00Z" }],
    mailbox: [{ id: "m1", entity_id: "vertical-1", status: "pending", created_at: "2026-03-19T11:00:00Z" }],
  });

  assert.equal((out.vertical as Record<string, unknown>).id, "vertical-1");
  assert.equal((out.workflow_state as Record<string, unknown>).active_timer_count, 1);
  assert.equal(Array.isArray(out.events), true);
  assert.equal(Array.isArray(out.mailbox), true);
  assert.deepEqual(out.artifacts, []);
});
