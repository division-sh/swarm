import assert from "node:assert/strict";
import test from "node:test";
import { adaptDigest } from "./digest.ts";

test("adaptDigest composes a digest from generic resources", () => {
  const out = adaptDigest({
    agentsResp: {
      agents: [
        { id: "a1", state: "stuck", pending_events: 2 },
        { id: "a2", state: "idle" },
      ],
      states: {},
    },
    health: { runtime: { running: true } },
    mailbox: { summary: { pending: 4 }, items: [] },
    tasksResp: { tasks: [{ id: "t1" }] },
    incidents: [{ code: "CODE-1", count: 2, component: "runtime", level: "warn", last_seen: "2026-03-19T12:00:00Z" }],
    generatedAt: "2026-03-19T12:00:00Z",
  });

  assert.equal(out?.generated_at, "2026-03-19T12:00:00Z");
  const lastCompiled = out?.last_compiled as { at?: string; payload?: Record<string, unknown> } | undefined;
  const current = out?.current as { text?: string } | undefined;
  assert.equal(lastCompiled?.at, "2026-03-19T12:00:00Z");
  assert.equal(lastCompiled?.payload?.attention_agents, 1);
  assert.equal(lastCompiled?.payload?.pending_mailbox, 4);
  assert.match(String(current?.text), /Runtime is running/);
  assert.equal(Array.isArray(out?.top), true);
  assert.equal(out?.top?.length, 1);
});
