import { useMemo } from "react";
import { useActionRunner } from "../hooks/useActionRunner.ts";
import { useControlActions } from "../hooks/useControlActions.ts";
import { useNavigationActions } from "../hooks/useNavigationActions.ts";
import { useTaskActions } from "../hooks/useTaskActions.ts";
import type { ModalContent } from "./useDashboardUIState.ts";

type UIActionState = {
  activeSubview: string;
  setActiveView: (value: string) => void;
  setViewRoute: (view: string, subview?: string) => void;
  setModalContent: (value: ModalContent) => void;
};

type TaskActionState = {
  selectedTaskID: string;
  setSelectedTaskID: (value: string) => void;
  setTaskStatus: (value: string) => void;
  taskResultText: string;
  setTaskResultText: (value: string) => void;
  taskOutcome: string;
  setTaskOutcome: (value: string) => void;
  taskFollowUpNeeded: boolean;
  setTaskFollowUpNeeded: (value: boolean) => void;
  taskRejectReason: string;
  setTaskRejectReason: (value: string) => void;
};

type RuntimeActionState = {
  setSelectedEventID: (value: string) => void;
  setSelectedConv: (value: string) => void;
  setEventsFilter: (value: unknown) => void;
  setEventsIncludeRuntime: (value: boolean) => void;
  setEventsRuntimeErrorsOnly: (value: boolean) => void;
  setLogsFilter: (value: unknown) => void;
  setLogsRuntimeErrorsOnly: (value: boolean) => void;
};

type OpsActionState = {
  setControlOutput: (value: unknown) => void;
  setControlTarget: (value: string) => void;
  setSelectedAgentID: (value: string) => void;
};

type DashboardActionCompositionInput = {
  ui: UIActionState;
  taskState: TaskActionState;
  runtimeState: RuntimeActionState;
  opsState: OpsActionState;
  addToast: (message: string, type?: string) => void;
  loadAgents: () => Promise<unknown>;
  loadTasks: () => Promise<unknown>;
  loadTaskStats: () => Promise<unknown>;
  loadEvents: () => Promise<unknown>;
  loadMailbox: () => Promise<unknown>;
  loadTargets: () => Promise<unknown>;
  loadOverview: () => Promise<unknown>;
  loadFunnel: () => Promise<unknown>;
};

export function useDashboardActionComposition({
  ui,
  taskState,
  runtimeState,
  opsState,
  addToast,
  loadAgents,
  loadTasks,
  loadTaskStats,
  loadEvents,
  loadMailbox,
  loadTargets,
  loadOverview,
  loadFunnel,
}: DashboardActionCompositionInput) {
  const refreshAfterControl = useMemo(
    () => async () => Promise.all([loadAgents(), loadTasks(), loadEvents(), loadMailbox(), loadTargets(), loadOverview(), loadFunnel()]),
    [loadAgents, loadEvents, loadFunnel, loadMailbox, loadOverview, loadTargets, loadTasks],
  );

  const { runControl } = useActionRunner({
    addToast,
    setControlOutput: opsState.setControlOutput,
    refreshAfterControl,
  });

  const navigationActions = useNavigationActions({
    addToast,
    loadAgents,
    loadTargets,
    activeSubview: ui.activeSubview,
    setActiveView: ui.setActiveView,
    setViewRoute: ui.setViewRoute,
    setModalContent: ui.setModalContent,
    setControlTarget: opsState.setControlTarget,
    setSelectedAgentID: opsState.setSelectedAgentID,
    setSelectedTaskID: taskState.setSelectedTaskID,
    setTaskStatus: taskState.setTaskStatus,
    setSelectedEventID: runtimeState.setSelectedEventID,
    setSelectedConv: runtimeState.setSelectedConv,
    setEventsFilter: runtimeState.setEventsFilter,
    setEventsIncludeRuntime: runtimeState.setEventsIncludeRuntime,
    setEventsRuntimeErrorsOnly: runtimeState.setEventsRuntimeErrorsOnly,
    setLogsFilter: runtimeState.setLogsFilter,
    setLogsRuntimeErrorsOnly: runtimeState.setLogsRuntimeErrorsOnly,
  });

  const controlActions = useControlActions({
    addToast,
    runControl,
    loadAgents,
    loadMailbox,
  });

  const taskActions = useTaskActions({
    runControl,
    loadTasks,
    loadTaskStats,
    selectedTaskID: taskState.selectedTaskID,
    taskResultText: taskState.taskResultText,
    setTaskResultText: taskState.setTaskResultText,
    taskOutcome: taskState.taskOutcome,
    setTaskOutcome: taskState.setTaskOutcome,
    taskFollowUpNeeded: taskState.taskFollowUpNeeded,
    setTaskFollowUpNeeded: taskState.setTaskFollowUpNeeded,
    taskRejectReason: taskState.taskRejectReason,
    setTaskRejectReason: taskState.setTaskRejectReason,
  });

  return {
    navigationActions,
    controlActions,
    taskActions,
  };
}
