import test from "node:test";
import assert from "node:assert/strict";
import { deriveTasksDerivedState } from "./useTasksDerivedState.js";

test("deriveTasksDerivedState summarizes task urgency and budget", () => {
  const result = deriveTasksDerivedState({
    tasksResp: {
      tasks: [
        {
          id: "task-overdue",
          status: "open",
          priority: "p1",
          description: "Overdue review",
          deadline: "2026-03-01T00:00:00Z",
          created_at: "2026-03-08T10:00:00Z",
        },
        {
          id: "task-review",
          status: "pending_review",
          priority: "p2",
          description: "Needs review",
          created_at: "2026-03-08T09:00:00Z",
        },
        {
          id: "task-complete",
          status: "completed",
          priority: "p3",
          description: "Done",
          created_at: "2026-03-07T09:00:00Z",
        },
      ],
      weekly_budget: {
        approved_this_week: 2,
        max_tasks_per_week: 5,
        reset_day: "monday",
        week_start_utc: "2026-03-02T00:00:00Z",
      },
    },
    tasksStats: {
      open: 1,
      completed: 1,
      rejected: 0,
    },
  });

  assert.equal(result.summary.loaded, 3);
  assert.equal(result.summary.actionable, 2);
  assert.equal(result.summary.overdue, 1);
  assert.equal(result.summary.review, 1);
  assert.equal(result.summary.completed, 1);
  assert.equal(result.summary.budgetUsed, 2);
  assert.equal(result.summary.budgetMax, 5);
  assert.equal(result.queue.overdue[0].id, "task-overdue");
  assert.equal(result.queue.review[0].id, "task-review");
});
