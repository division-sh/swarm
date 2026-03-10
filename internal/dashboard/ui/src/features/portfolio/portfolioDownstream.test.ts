import test from "node:test";
import assert from "node:assert/strict";
import { derivePortfolioDownstreamState } from "./usePortfolioDownstreamState.ts";

test("derivePortfolioDownstreamState matches vertical-linked tasks, mailbox, and agents", () => {
  const result = derivePortfolioDownstreamState({
    verticals: [{ id: "v-1", slug: "alpha" }],
    focusSummary: { key: "alpha", vertical: { id: "v-1", slug: "alpha" } },
    tasks: [
      { id: "task-1", vertical_slug: "alpha", status: "open", priority: "high", category: "review" },
      { id: "task-2", vertical_slug: "other", status: "open", priority: "high", category: "review" },
    ],
    mailboxItems: [
      { id: "mb-1", vertical_id: "v-1", status: "pending", priority: "critical" },
      { id: "mb-2", vertical_slug: "other", status: "pending", priority: "critical" },
    ],
    targets: [
      { agent_id: "opco-ceo-v-1", vertical_id: "v-1", role: "opco-ceo", status: "active" },
    ],
    agents: [
      { id: "opco-ceo-v-1", vertical_id: "v-1", role: "opco-ceo", state: "running" },
    ],
  });

  assert.equal(result.current.summary.tasks, 1);
  assert.equal(result.current.summary.mailbox, 1);
  assert.equal(result.current.summary.agents, 1);
  assert.equal(result.current.primaryTask.id, "task-1");
  assert.equal(result.current.primaryMailbox.id, "mb-1");
  assert.equal(result.current.primaryAgent.id, "opco-ceo-v-1");
});

test("derivePortfolioDownstreamState falls back cleanly when no related context exists", () => {
  const result = derivePortfolioDownstreamState({
    verticals: [{ id: "v-1", slug: "alpha" }],
    focusSummary: { key: "alpha", vertical: { id: "v-1", slug: "alpha" } },
    tasks: [],
    mailboxItems: [],
    targets: [],
    agents: [],
  });

  assert.equal(result.current.summary.tasks, 0);
  assert.equal(result.current.summary.mailbox, 0);
  assert.equal(result.current.summary.agents, 0);
  assert.equal(result.current.primaryTask, null);
  assert.equal(result.current.primaryMailbox, null);
  assert.equal(result.current.primaryAgent, null);
});
