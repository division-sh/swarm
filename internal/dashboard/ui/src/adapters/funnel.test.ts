import assert from "node:assert/strict";
import test from "node:test";
import { adaptFunnel } from "./funnel.ts";

test("adaptFunnel derives throughput and stuck rows from generic instances", () => {
  const now = new Date().toISOString();
  const out = adaptFunnel([
    {
      instance_id: "alpha",
      current_state: "validation",
      entered_stage_at: now,
      metadata: { slug: "alpha", stage: "scoring" },
      timer_state: [{ timer_id: "t1", cancelled: false }],
      updated_at: now,
    },
    {
      instance_id: "beta",
      current_state: "approved",
      entered_stage_at: now,
      metadata: { slug: "beta", stage: "approved" },
      timer_state: [],
      updated_at: now,
    },
  ]);

  assert.equal(out.throughput.discoveries_14d, 2);
  assert.equal(out.throughput.specs_approved_or_live, 1);
  assert.equal(out.stuck.length, 1);
  assert.equal(out.stuck[0].slug, "alpha");
});
