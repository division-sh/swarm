import type { TasksResponse, TaskRecord, WeeklyBudget } from "../../types/core.ts";

type TaskStats = {
  completed?: number;
  rejected?: number;
  open?: number;
  [key: string]: unknown;
};

function normalizePriority(value: unknown): number {
  const v = String(value || "").toLowerCase();
  if (v === "critical") return 4;
  if (v === "high" || v === "p1") return 3;
  if (v === "medium" || v === "p2") return 2;
  if (v === "low" || v === "p3") return 1;
  return 0;
}

function isOverdue(deadline: unknown): boolean {
  if (!deadline) return false;
  const d = new Date(String(deadline));
  return Number.isFinite(d.getTime()) && d.getTime() < Date.now();
}

function isActionable(status: unknown): boolean {
  const v = String(status || "").toLowerCase();
  return v === "open" || v === "pending_review" || v === "assigned";
}

function sortTasks(a: TaskRecord, b: TaskRecord): number {
  const overdueDelta = Number(isOverdue(b.deadline)) - Number(isOverdue(a.deadline));
  if (overdueDelta !== 0) return overdueDelta;
  const pri = normalizePriority(b.priority) - normalizePriority(a.priority);
  if (pri !== 0) return pri;
  return new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime();
}

export function deriveTasksDerivedState({
  tasksResp,
  tasksStats,
}: {
  tasksResp: TasksResponse;
  tasksStats: TaskStats | null;
}) {
  const tasks = Array.isArray(tasksResp?.tasks) ? tasksResp.tasks : [];
  const stats = tasksStats || {};
  const actionable = tasks.filter((task) => isActionable(task.status)).sort(sortTasks);
  const overdue = actionable.filter((task) => isOverdue(task.deadline));
  const review = actionable.filter((task) => String(task.status || "").toLowerCase() === "pending_review");
  const highPriority = actionable.filter((task) => normalizePriority(task.priority) >= 3);
  const assigned = actionable.filter((task) => String(task.status || "").toLowerCase() === "assigned");
  const weeklyBudget: WeeklyBudget | null = tasksResp?.weekly_budget || null;
  const approvedThisWeek = Number(weeklyBudget?.approved_this_week || 0);
  const maxTasksPerWeek = Number(weeklyBudget?.max_tasks_per_week || 0);

  return {
    summary: {
      loaded: tasks.length,
      actionable: actionable.length,
      overdue: overdue.length,
      review: review.length,
      assigned: assigned.length,
      completed: Number(stats.completed || 0),
      rejected: Number(stats.rejected || 0),
      open: Number(stats.open || 0),
      budgetUsed: approvedThisWeek,
      budgetMax: maxTasksPerWeek,
    },
    queue: {
      overdue: overdue.slice(0, 5),
      review: review.slice(0, 5),
      highPriority: highPriority.slice(0, 5),
    },
    budget: {
      approvedThisWeek,
      maxTasksPerWeek,
      resetDay: weeklyBudget?.reset_day || "monday",
      weekStart: weeklyBudget?.week_start_utc || "",
    },
  };
}
