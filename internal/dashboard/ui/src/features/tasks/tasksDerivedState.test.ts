import test from "node:test";
import assert from "node:assert/strict";
import { deriveTasksDerivedState } from "./useTasksDerivedState.ts";

test("deriveTasksDerivedState summarizes queue pressure and weekly budget", () => {
  const result = deriveTasksDerivedState({
    tasksResp: {
      tasks: [
        { id: "task-1", status: "open", priority: "p1", deadline: "2026-03-01T00:00:00Z", created_at: "2026-03-08T09:00:00Z" },
        { id: "task-2", status: "pending_review", priority: "p2", created_at: "2026-03-08T08:00:00Z" },
        { id: "task-3", status: "assigned", priority: "p3", created_at: "2026-03-08T07:00:00Z" },
      ],
      weekly_budget: {
        approved_this_week: 5,
        max_tasks_per_week: 12,
        reset_day: "monday",
      },
    },
    tasksStats: {
      completed: 4,
      rejected: 1,
      open: 3,
    },
  });

  assert.equal(result.summary.loaded, 3);
  assert.equal(result.summary.actionable, 3);
  assert.equal(result.summary.overdue, 1);
  assert.equal(result.summary.review, 1);
  assert.equal(result.summary.assigned, 1);
  assert.equal(result.summary.completed, 4);
  assert.equal(result.budget.approvedThisWeek, 5);
  assert.equal(result.queue.overdue[0].id, "task-1");
});
