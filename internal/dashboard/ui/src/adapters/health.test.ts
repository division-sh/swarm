import assert from "node:assert/strict";
import test from "node:test";
import { adaptHealth } from "./health.ts";

test("adaptHealth maps generic health checks into dashboard health shape", () => {
  const out = adaptHealth({
    ok: true,
    timestamp: "2026-03-17T12:00:00Z",
    checks: {
      runtime: {
        ready: true,
        agents: 12,
        flows: 3,
        nodes: 8,
        events: 21,
      },
      database: {
        ok: true,
      },
    },
  });

  assert.equal(out.ok, true);
  assert.equal(out.runtime?.running, true);
  assert.equal(out.runtime?.loaded_agents, 12);
  assert.equal(out.runtime?.flow_count, 3);
  assert.equal((out.database as Record<string, unknown>).ok, true);
  assert.deepEqual(out.workflow_audit?.warnings, []);
});
