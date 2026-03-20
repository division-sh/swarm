import assert from "node:assert/strict";
import test from "node:test";
import { adaptOverview } from "./overview.ts";

test("adaptOverview summarizes generic dashboard resources", () => {
  const out = adaptOverview({
    agentsResp: {
      agents: [
        { id: "a1", state: "running" },
        { id: "a2", state: "stuck" },
        { id: "a3", state: "terminated" },
      ],
      states: {},
    },
    health: { runtime: { running: true } },
    mailbox: { summary: { pending: 3 }, items: [] },
    tasksResp: { tasks: [{ id: "t1" }, { id: "t2" }] },
    events: [{ id: "e1" }, { id: "e2" }, { id: "e3" }],
    incidents: [{ code: "X1" }],
    generatedAt: "2026-03-19T12:00:00Z",
  });

  assert.equal(out.generated_at, "2026-03-19T12:00:00Z");
  assert.equal(out.agents_active, 2);
  assert.equal(out.events_24h, 3);
  assert.equal(out.summary?.runtime_running, true);
  assert.equal(out.summary?.incidents_open, 1);
  assert.equal(out.summary?.mailbox_pending, 3);
  assert.equal(out.summary?.tasks_open, 2);
});
