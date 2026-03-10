import test from "node:test";
import assert from "node:assert/strict";
import { deriveOperationsDerivedState } from "./useOperationsDerivedState.ts";

test("deriveOperationsDerivedState summarizes mailbox and task urgency", () => {
  const result = deriveOperationsDerivedState({
    mailbox: {
      items: [
        { id: "mb-1", status: "pending", priority: "critical", summary: "Review Alpha", vertical_slug: "alpha", from_agent: "holding-manager", created_at: "2026-03-08T10:00:00Z" },
        { id: "mb-2", status: "approved", priority: "low", summary: "Old", created_at: "2026-03-07T10:00:00Z" },
      ],
    },
    tasksResp: {
      tasks: [
        { id: "task-1", status: "open", priority: "p1", description: "Do review", requesting_agent: "holding-manager", vertical_slug: "alpha", deadline: "2026-03-01T10:00:00Z", created_at: "2026-03-08T09:00:00Z" },
        { id: "task-2", status: "completed", priority: "p2", description: "Done", created_at: "2026-03-07T09:00:00Z" },
      ],
    },
    selectedTask: { id: "task-1", status: "open", description: "Do review", requesting_agent: "holding-manager", vertical_slug: "alpha" },
    selectedMailboxItem: "",
  });

  assert.equal(result.summary.pendingMailbox, 1);
  assert.equal(result.summary.criticalMailbox, 1);
  assert.equal(result.summary.actionableTasks, 1);
  assert.equal(result.summary.overdueTasks, 1);
  assert.equal(result.focus?.type, "task");
  assert.equal(result.related.mailbox.length, 1);
  assert.equal(result.queue.mailbox[0].id, "mb-1");
  assert.equal(result.queue.tasks[0].id, "task-1");
});

test("deriveOperationsDerivedState falls back to mailbox focus when no task is selected", () => {
  const result = deriveOperationsDerivedState({
    mailbox: {
      items: [
        { id: "mb-1", status: "pending", priority: "high", summary: "Review Alpha", vertical_slug: "alpha", from_agent: "holding-manager" },
      ],
    },
    tasksResp: {
      tasks: [
        { id: "task-1", status: "open", priority: "p1", description: "Do review", requesting_agent: "holding-manager", vertical_slug: "alpha" },
      ],
    },
    selectedTask: null,
    selectedMailboxItem: "mb-1",
  });

  assert.equal(result.focus?.type, "mailbox");
  assert.equal(result.focus?.id, "mb-1");
  assert.equal(result.related.tasks.length, 1);
  assert.equal(result.related.mailbox.length, 1);
});

test("deriveOperationsDerivedState builds a unified urgency queue across mailbox and tasks", () => {
  const result = deriveOperationsDerivedState({
    mailbox: {
      items: [
        { id: "mb-1", status: "pending", priority: "critical", summary: "Critical approval", created_at: "2026-03-08T10:00:00Z" },
      ],
    },
    tasksResp: {
      tasks: [
        { id: "task-1", status: "open", priority: "p1", description: "Overdue task", deadline: "2026-03-01T00:00:00Z", created_at: "2026-03-08T09:00:00Z" },
      ],
    },
    selectedTask: null,
    selectedMailboxItem: "",
  });

  assert.equal(result.queue.unified.length, 2);
  assert.equal(result.queue.unified[0].kind, "mailbox");
  assert.equal(result.queue.unified[0].id, "mb-1");
  assert.equal(result.queue.unified[1].kind, "task");
  assert.equal(result.queue.unified[1].id, "task-1");
});
