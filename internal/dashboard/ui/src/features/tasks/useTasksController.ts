import { useMemo } from "react";
import type { TaskRecord, TasksResponse, TaskStats } from "../../types/core.ts";

type AsyncAction = () => Promise<unknown>;

type TasksControllerInput = {
  tasksResp: TasksResponse;
  tasksStats: TaskStats | null;
  selectedTask: TaskRecord | null;
  taskStatus: string;
  selectedTaskID: string;
  taskResultText: string;
  taskOutcome: string;
  taskFollowUpNeeded: boolean;
  taskRejectReason: string;
  setTaskStatus: (value: string) => void;
  setSelectedTaskID: (value: string) => void;
  setTaskResultText: (value: string) => void;
  setTaskOutcome: (value: string) => void;
  setTaskFollowUpNeeded: (value: boolean) => void;
  setTaskRejectReason: (value: string) => void;
  refreshTasks: AsyncAction;
  loadTaskStats: AsyncAction;
  claimSelectedTask: AsyncAction;
  completeSelectedTask: AsyncAction;
  rejectSelectedTask: AsyncAction;
};

export function useTasksController({
  tasksResp,
  tasksStats,
  selectedTask,
  taskStatus,
  selectedTaskID,
  taskResultText,
  taskOutcome,
  taskFollowUpNeeded,
  taskRejectReason,
  setTaskStatus,
  setSelectedTaskID,
  setTaskResultText,
  setTaskOutcome,
  setTaskFollowUpNeeded,
  setTaskRejectReason,
  refreshTasks,
  loadTaskStats,
  claimSelectedTask,
  completeSelectedTask,
  rejectSelectedTask,
}: TasksControllerInput) {
  return useMemo(() => ({
    state: {
      tasksResp,
      tasksStats,
      selectedTask,
      taskStatus,
      selectedTaskID,
      taskResultText,
      taskOutcome,
      taskFollowUpNeeded,
      taskRejectReason,
    },
    actions: {
      setTaskStatus,
      setSelectedTaskID,
      setTaskResultText,
      setTaskOutcome,
      setTaskFollowUpNeeded,
      setTaskRejectReason,
      refreshTasks,
      loadTaskStats,
      claimSelectedTask,
      completeSelectedTask,
      rejectSelectedTask,
    },
  }), [
    claimSelectedTask,
    completeSelectedTask,
    loadTaskStats,
    refreshTasks,
    rejectSelectedTask,
    selectedTask,
    selectedTaskID,
    setSelectedTaskID,
    setTaskFollowUpNeeded,
    setTaskOutcome,
    setTaskRejectReason,
    setTaskResultText,
    setTaskStatus,
    taskFollowUpNeeded,
    taskOutcome,
    taskRejectReason,
    taskResultText,
    taskStatus,
    tasksResp,
    tasksStats,
  ]);
}
