import { useCallback } from "react";
import { postJSON } from "../api/client.ts";

type TaskActionsInput = {
  runControl: (fn: () => Promise<Record<string, any>>) => Promise<Record<string, any>>;
  loadTasks: () => Promise<unknown>;
  loadTaskStats: () => Promise<unknown>;
  selectedTaskID: string;
  taskResultText: string;
  setTaskResultText: (value: string) => void;
  taskOutcome: string;
  setTaskOutcome: (value: string) => void;
  taskFollowUpNeeded: boolean;
  setTaskFollowUpNeeded: (value: boolean) => void;
  taskRejectReason: string;
  setTaskRejectReason: (value: string) => void;
};

export function useTaskActions({
  runControl,
  loadTasks,
  loadTaskStats,
  selectedTaskID,
  taskResultText,
  setTaskResultText,
  taskOutcome,
  setTaskOutcome,
  taskFollowUpNeeded,
  setTaskFollowUpNeeded,
  taskRejectReason,
  setTaskRejectReason,
}: TaskActionsInput) {
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

  return {
    claimSelectedTask,
    completeSelectedTask,
    rejectSelectedTask,
    loadTaskStats,
  };
}
