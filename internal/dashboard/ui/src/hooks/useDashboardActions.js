import { useMemo } from "react";
import { useActionRunner } from "./useActionRunner.js";
import { useControlActions } from "./useControlActions.js";
import { useNavigationActions } from "./useNavigationActions.js";
import { useTaskActions } from "./useTaskActions.js";

export function useDashboardActions({
  addToast,
  setControlOutput,
  loadAgents,
  loadTasks,
  loadEvents,
  loadMailbox,
  loadTargets,
  loadOverview,
  loadFunnel,
  setControlTarget,
  setActiveView,
  setSelectedAgentID,
  selectedTaskID,
  setSelectedTaskID,
  taskResultText,
  setTaskResultText,
  taskOutcome,
  setTaskOutcome,
  taskFollowUpNeeded,
  setTaskFollowUpNeeded,
  taskRejectReason,
  setTaskRejectReason,
  setTasksStats,
  setSelectedEventID,
  setSelectedConv,
  setEventsFilter,
  setEventsRuntimeErrorsOnly,
  setLogsFilter,
  setLogsRuntimeErrorsOnly,
}) {
  const refreshAfterControl = useMemo(
    () => async () => Promise.all([loadAgents(), loadTasks(), loadEvents(), loadMailbox(), loadTargets(), loadOverview(), loadFunnel()]),
    [loadAgents, loadEvents, loadFunnel, loadMailbox, loadOverview, loadTargets, loadTasks],
  );

  const { runControl } = useActionRunner({
    addToast,
    setControlOutput,
    refreshAfterControl,
  });

  const navigationActions = useNavigationActions({
    addToast,
    loadAgents,
    loadTargets,
    setActiveView,
    setControlTarget,
    setSelectedAgentID,
    setSelectedTaskID,
    setSelectedEventID,
    setSelectedConv,
    setEventsFilter,
    setEventsRuntimeErrorsOnly,
    setLogsFilter,
    setLogsRuntimeErrorsOnly,
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
  });

  return {
    ...navigationActions,
    ...controlActions,
    ...taskActions,
  };
}
