import { useCallback, useEffect } from "react";
import { getEmpireKey } from "../api/client.js";
import { useDashboardPolling } from "../hooks/useDashboardPolling.js";
import { useEventStream } from "../hooks/useEventStream.js";
import { useFlowRuntimeStream } from "../hooks/useFlowRuntimeStream.js";
import { useReplayTicker } from "../hooks/useReplayTicker.js";

export function useDashboardLifecycle({
  ui,
  runtimeState,
  pipelineState,
  refreshers,
  addToast,
  loadOverview,
  loadAgents,
  loadDigest,
  loadTasks,
  loadMailbox,
  loadHealth,
  loadIncidents,
  loadTargets,
  loadFunnel,
  loadVerticals,
  loadHolding,
  loadPipelineFlow,
}) {
  const refreshAll = useCallback(async () => {
    await Promise.all([
      refreshers.loadOverview(),
      refreshers.loadAgents(),
      refreshers.loadDigest(),
      refreshers.loadTasks(),
      refreshers.loadEvents(),
      refreshers.loadRuntimeLogs(),
      refreshers.loadConversations(),
      refreshers.loadTargets(),
      refreshers.loadFunnel(),
      refreshers.loadShardScans(),
      refreshers.loadMailbox(),
      refreshers.loadHealth(),
      refreshers.loadVerticals(),
      refreshers.loadHolding(),
      refreshers.loadIncidents(),
    ]);
  }, [refreshers]);

  useEffect(() => {
    if (!pipelineState.graphFullscreen) return;
    function onKey(event) {
      if (event.key === "Escape") pipelineState.setGraphFullscreen(false);
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [pipelineState]);

  useEffect(() => {
    refreshAll()
      .catch((err) => { ui.setStatusText(`Dashboard error: ${err.message}`); })
      .finally(() => ui.setInitialLoading(false));
  }, [refreshAll, ui]);

  useEventStream({
    eventsFilter: runtimeState.eventsFilter,
    eventsIncludeRuntime: runtimeState.eventsIncludeRuntime,
    eventsRuntimeErrorsOnly: runtimeState.eventsRuntimeErrorsOnly,
    getKey: getEmpireKey,
    loadEvents: refreshers.loadEvents,
    loadRuntimeLogs: refreshers.loadRuntimeLogs,
    addToast,
  });

  useFlowRuntimeStream({
    activeView: ui.activeView,
    flowView: pipelineState.flowView,
    flowVertical: pipelineState.flowVertical,
    getKey: getEmpireKey,
    setFlowEvents: pipelineState.setFlowEvents,
  });

  useReplayTicker({
    flowView: pipelineState.flowView,
    flowReplayOn: pipelineState.flowReplayOn,
    flowReplaySpeed: pipelineState.flowReplaySpeed,
    flowEvents: pipelineState.flowEvents,
    setFlowReplayIndex: pipelineState.setFlowReplayIndex,
    setFlowReplayOn: pipelineState.setFlowReplayOn,
  });

  useDashboardPolling({
    loadOverview,
    loadAgents,
    loadDigest,
    loadTasks,
    loadMailbox,
    loadHealth,
    loadRuntimeLogs,
    loadIncidents,
    loadTargets,
    loadFunnel,
    loadVerticals,
    loadHolding,
    activeView: ui.activeView,
    flowView: pipelineState.flowView,
    loadPipelineFlow,
  });
}
