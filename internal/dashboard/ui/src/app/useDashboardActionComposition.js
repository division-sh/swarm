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
    setActiveView: ui.setActiveView,
    setModalContent: ui.setModalContent,
    setControlTarget: opsState.setControlTarget,
    setSelectedAgentID: opsState.setSelectedAgentID,
    setSelectedTaskID: taskState.setSelectedTaskID,
    setSelectedEventID: runtimeState.setSelectedEventID,
    setSelectedConv: runtimeState.setSelectedConv,
    setEventsFilter: runtimeState.setEventsFilter,
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
    selectedTaskID: taskState.selectedTaskID,
    taskResultText: taskState.taskResultText,
    setTaskResultText: taskState.setTaskResultText,
    taskOutcome: taskState.taskOutcome,
    setTaskOutcome: taskState.setTaskOutcome,
    taskFollowUpNeeded: taskState.taskFollowUpNeeded,
    setTaskFollowUpNeeded: taskState.setTaskFollowUpNeeded,
    taskRejectReason: taskState.taskRejectReason,
    setTaskRejectReason: taskState.setTaskRejectReason,
    setTasksStats: taskState.setTasksStats,
  });

  return {
    navigationActions,
    controlActions,
    taskActions,
  };
}
