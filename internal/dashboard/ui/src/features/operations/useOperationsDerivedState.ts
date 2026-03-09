function normalizePriority(value: unknown): number {
  const v = String(value || "").toLowerCase();
  if (v === "critical") return 4;
  if (v === "high" || v === "p1") return 3;
  if (v === "medium" || v === "p2") return 2;
  if (v === "low" || v === "p3") return 1;
  return 0;
}

function isPendingMailbox(item: Record<string, any> | null | undefined): boolean {
  return String(item?.status || "").toLowerCase() === "pending";
}

function isActionableTask(task: Record<string, any> | null | undefined): boolean {
  const status = String(task?.status || "").toLowerCase();
  return status === "open" || status === "pending_review" || status === "assigned";
}

function isOverdue(deadline: unknown): boolean {
  if (!deadline) return false;
  const d = new Date(String(deadline));
  return Number.isFinite(d.getTime()) && d.getTime() < Date.now();
}

function mailboxSort(a: Record<string, any>, b: Record<string, any>): number {
  const pri = normalizePriority(b.priority) - normalizePriority(a.priority);
  if (pri !== 0) return pri;
  return new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime();
}

function taskSort(a: Record<string, any>, b: Record<string, any>): number {
  const overdue = Number(isOverdue(b.deadline)) - Number(isOverdue(a.deadline));
  if (overdue !== 0) return overdue;
  const pri = normalizePriority(b.priority) - normalizePriority(a.priority);
  if (pri !== 0) return pri;
  return new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime();
}

function firstNonEmpty(...values: unknown[]): string {
  for (const value of values) {
    const text = typeof value === "string" ? value.trim() : "";
    if (text) return text;
  }
  return "";
}

export function deriveOperationsDerivedState({
  mailbox,
  tasksResp,
  selectedTask,
  selectedMailboxItem,
}: {
  mailbox: Record<string, any>;
  tasksResp: Record<string, any>;
  selectedTask: Record<string, any> | null;
  selectedMailboxItem: string;
}) {
  const mailboxItems = Array.isArray(mailbox?.items) ? mailbox.items : [];
  const tasks = Array.isArray(tasksResp?.tasks) ? tasksResp.tasks : [];

  const pendingMailbox = mailboxItems.filter(isPendingMailbox).sort(mailboxSort);
  const actionableTasks = tasks.filter(isActionableTask).sort(taskSort);
  const overdueTasks = actionableTasks.filter((task) => isOverdue(task.deadline));
  const reviewTasks = actionableTasks.filter((task) => String(task.status || "").toLowerCase() === "pending_review");
  const highPriorityTasks = actionableTasks.filter((task) => normalizePriority(task.priority) >= 3);
  const criticalMailbox = pendingMailbox.filter((item) => normalizePriority(item.priority) >= 4);

  const currentTask = selectedTask || null;
  const currentMailbox = mailboxItems.find((item) => item.id === selectedMailboxItem) || null;

  const focus = currentTask
    ? {
      type: "task",
      id: currentTask.id,
      title: currentTask.description || currentTask.category || currentTask.id,
      vertical: currentTask.vertical_slug || "",
      agent: currentTask.requesting_agent || "",
      status: currentTask.status || "",
    }
    : currentMailbox
      ? {
        type: "mailbox",
        id: currentMailbox.id,
        title: firstNonEmpty(currentMailbox.summary, currentMailbox.type, currentMailbox.id),
        vertical: currentMailbox.vertical_slug || currentMailbox.vertical_id || "",
        agent: currentMailbox.from_agent || "",
        status: currentMailbox.status || "",
      }
      : null;

  const unifiedQueue = [
    ...pendingMailbox.map((item) => ({
      kind: "mailbox",
      id: item.id,
      priorityScore: normalizePriority(item.priority) + 10,
      title: firstNonEmpty(item.summary, item.type, item.id),
      vertical: item.vertical_slug || item.vertical_id || "",
      agent: item.from_agent || "",
      status: item.status || "",
      priority: item.priority || "",
      created_at: item.created_at || "",
      record: item,
    })),
    ...actionableTasks.map((task) => ({
      kind: "task",
      id: task.id,
      priorityScore: normalizePriority(task.priority) + (isOverdue(task.deadline) ? 10 : 0),
      title: firstNonEmpty(task.description, task.category, task.id),
      vertical: task.vertical_slug || "",
      agent: task.requesting_agent || "",
      status: task.status || "",
      priority: task.priority || "",
      created_at: task.created_at || "",
      deadline: task.deadline || "",
      record: task,
    })),
  ].sort((a, b) => {
    const pri = b.priorityScore - a.priorityScore;
    if (pri !== 0) return pri;
    return new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime();
  });

  const related = focus
    ? {
      tasks: actionableTasks.filter((task) => focus.vertical && task.vertical_slug === focus.vertical).slice(0, 3),
      mailbox: pendingMailbox.filter((item) => focus.vertical && (item.vertical_slug === focus.vertical || item.vertical_id === focus.vertical)).slice(0, 3),
    }
    : { tasks: [], mailbox: [] };

  return {
    summary: {
      pendingMailbox: pendingMailbox.length,
      criticalMailbox: criticalMailbox.length,
      actionableTasks: actionableTasks.length,
      overdueTasks: overdueTasks.length,
      reviewTasks: reviewTasks.length,
      highPriorityTasks: highPriorityTasks.length,
    },
    queue: {
      mailbox: pendingMailbox.slice(0, 5),
      tasks: actionableTasks.slice(0, 5),
      criticalMailbox,
      overdueTasks,
      reviewTasks,
      highPriorityTasks,
      unified: unifiedQueue,
    },
    focus,
    related,
  };
}
