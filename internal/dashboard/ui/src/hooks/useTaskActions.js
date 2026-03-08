import { useCallback } from "react";
import { fetchJSON, postJSON } from "../api/client.js";

export function useTaskActions({
  runControl,
  loadTasks,
  selectedTaskID,
  taskResultText,
  setTaskResultText,
  taskOutcome,
  setTaskOutcome,
  taskFollowUpNeeded,
  setTaskFollowUpNeeded,
  taskRejectReason,
  setTaskRejectReason,
  setTasksStats,
}) {
  const claimSelectedTask = useCallback(async () => {
    if (!selectedTaskID) return;
    await runControl(() => postJSON(`/api/tasks/${encodeURIComponent(selectedTaskID)}/claim`, {}));
    await loadTasks();
  }, [loadTasks, runControl, selectedTaskID]);

  const completeSelectedTask = useCallback(async () => {
    if (!selectedTaskID) return;
    const body = {
      result_text: taskResultText.trim(),
      outcome: taskOutcome,
      follow_up_needed: !!taskFollowUpNeeded,
    };
    await runControl(() => postJSON(`/api/tasks/${encodeURIComponent(selectedTaskID)}/complete`, body));
    setTaskResultText("");
    setTaskOutcome("success");
    setTaskFollowUpNeeded(false);
    await loadTasks();
  }, [
    loadTasks,
    runControl,
    selectedTaskID,
    setTaskFollowUpNeeded,
    setTaskOutcome,
    setTaskResultText,
    taskFollowUpNeeded,
    taskOutcome,
    taskResultText,
  ]);

  const rejectSelectedTask = useCallback(async () => {
    if (!selectedTaskID) return;
    const body = { reason: (taskRejectReason || "").trim() };
    await runControl(() => postJSON(`/api/tasks/${encodeURIComponent(selectedTaskID)}/reject`, body));
    setTaskRejectReason("");
    await loadTasks();
  }, [loadTasks, runControl, selectedTaskID, setTaskRejectReason, taskRejectReason]);

  const loadTaskStats = useCallback(async () => {
    const d = await fetchJSON("/api/tasks/stats");
    setTasksStats(d || null);
  }, [setTasksStats]);

  return {
    claimSelectedTask,
    completeSelectedTask,
    rejectSelectedTask,
    loadTaskStats,
  };
}
