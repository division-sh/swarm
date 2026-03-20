import assert from "node:assert/strict";
import test from "node:test";
import { adaptTrace } from "./trace.ts";

test("adaptTrace maps generic events into pipeline trace rows", () => {
  const out = adaptTrace([
    {
      id: "evt-1",
      type: "venture.scored",
      created_at: "2026-03-19T12:00:00Z",
      source_agent: "scorer-1",
      vertical_id: "alpha",
      pending_count: 2,
    },
  ]);

  assert.equal(out.length, 1);
  assert.equal(out[0].id, "evt-1");
  assert.equal(out[0].kind, "event");
  assert.equal(out[0].event_type, "venture.scored");
  assert.equal(out[0].source_agent, "scorer-1");
  assert.equal(out[0].pending_count, 2);
});
