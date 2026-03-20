import assert from "node:assert/strict";
import test from "node:test";
import { adaptIncidentArtifacts } from "./incidentArtifacts.ts";

test("adaptIncidentArtifacts maps generic conversation detail into incident artifacts view data", () => {
  const out = adaptIncidentArtifacts({
    agent_id: "agent-1",
    scope_key: "entity-1",
    runtime_mode: "session",
    status: "active",
    summary: "recent parse failure",
    updated_at: "2026-03-19T12:00:00Z",
    messages: [{ role: "assistant", content: "hello" }],
    runtime_state: {
      summary: "recent parse failure",
      artifacts: [{ kind: "tool_error" }],
      last_turn: {
        parse_ok: false,
        response_payload: {
          tool_calls: [{ name: "lookup_data" }],
          assistant_text: "retrying",
        },
      },
    },
  });

  assert.equal(out.agent_id, "agent-1");
  assert.equal(out.summary, "recent parse failure");
  assert.equal(Array.isArray(out.messages), true);
  assert.equal(Array.isArray(out.turns), true);
  assert.equal(Array.isArray(out.artifacts), true);
});
