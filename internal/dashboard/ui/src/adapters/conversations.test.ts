import assert from "node:assert/strict";
import test from "node:test";
import { adaptConversationDetail, adaptConversationSummaries } from "./conversations.ts";

test("adaptConversationSummaries maps generic summaries into dashboard records", () => {
  const out = adaptConversationSummaries([{
    agent_id: "agent-1",
    summary: "brief",
    updated_at: "2026-03-17T12:00:00Z",
    runtime_mode: "session",
  }]);

  assert.equal(out.length, 1);
  assert.equal(out[0].agent_id, "agent-1");
  assert.equal(out[0].summary, "brief");
  assert.equal(out[0].runtime_mode, "session");
});

test("adaptConversationDetail lifts last_turn into dashboard turns", () => {
  const out = adaptConversationDetail({
    agent_id: "agent-1",
    updated_at: "2026-03-17T12:00:00Z",
    messages: [
      { role: "user", content: "hello" },
      { role: "assistant", content: [{ type: "text", text: "done" }] },
    ],
    runtime_state: {
      last_turn: {
        parse_ok: true,
        latency_ms: 42,
        response_payload: {
          assistant_text: "done",
          tool_calls: [{ name: "emit_task_completed" }],
        },
      },
    },
  });

  assert.equal(out.messages.length, 2);
  assert.equal(out.messages[0].text, "hello");
  assert.equal(out.turns.length, 1);
  assert.equal(out.turns[0].parse_ok, true);
  assert.equal(out.turns[0].latency_ms, 42);
  assert.equal(out.turns[0].assistant_text, "done");
});
