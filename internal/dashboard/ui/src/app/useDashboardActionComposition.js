import { useMemo } from "react";
import { useActionRunner } from "../hooks/useActionRunner.js";
import { useControlActions } from "../hooks/useControlActions.js";
import { useNavigationActions } from "../hooks/useNavigationActions.js";
import { useTaskActions } from "../hooks/useTaskActions.js";

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
}) {
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
