import test from "node:test";
import assert from "node:assert/strict";
import { deriveMailboxDerivedState } from "./useMailboxDerivedState.ts";

test("deriveMailboxDerivedState summarizes mailbox queues and selection", () => {
  const result = deriveMailboxDerivedState({
    mailbox: {
      items: [
        { id: "mb-1", status: "pending", priority: "critical", created_at: "2026-03-08T11:00:00Z" },
        { id: "mb-2", status: "approved", priority: "high", created_at: "2026-03-08T10:00:00Z" },
      ],
    },
    selectedMailboxItem: "mb-1",
  });

  assert.equal(result.summary.loaded, 2);
  assert.equal(result.summary.pending, 1);
  assert.equal(result.summary.critical, 1);
  assert.equal(result.summary.decided, 1);
  assert.equal(result.selected?.id, "mb-1");
  assert.equal(result.queue.pending[0].id, "mb-1");
  assert.equal(result.queue.decided[0].id, "mb-2");
});
