import { useCallback, useEffect } from "react";
import { getEmpireKey } from "../api/client.ts";
import { useDashboardPolling } from "../hooks/useDashboardPolling.js";
import { useEventStream } from "../hooks/useEventStream.js";
import { useFlowRuntimeStream } from "../hooks/useFlowRuntimeStream.js";
import { useReplayTicker } from "../hooks/useReplayTicker.js";

export function useDashboardLifecycle({
  ui,
  runtimeState,
  pipelineState,
  workflowStream,
  flowEvents,
  refreshers,
  addToast,
  loadLogs,
  loadRuntimeLogs,
  loadIncidents,
  loadConversations,
  loadGraph,
  loadPipelineFlow,
}) {
  const {
    activeView,
    activeSubview,
    setStatusText,
    setInitialLoading,
  } = ui;
  const {
    graphFullscreen,
    setGraphFullscreen,
    flowView,
    flowVertical,
    flowReplayOn,
    flowReplaySpeed,
    setFlowReplayIndex,
    setFlowReplayOn,
  } = pipelineState;

  const refreshAll = useCallback(async () => {
    const jobs = [
      ["events", refreshers.loadEvents],
      ["runtimeLogs", refreshers.loadRuntimeLogs],
      ["conversations", refreshers.loadConversations],
      ["incidents", refreshers.loadIncidents],
    ];
    const results = await Promise.allSettled(jobs.map(([, job]) => job()));
    const failures = results
      .map((result, index) => ({ result, key: jobs[index][0] }))
      .filter(({ result }) => result.status === "rejected");
    if (failures.length > 0) {
      const first = failures[0].result.reason;
      throw new Error(`${failures.length} refreshes failed${first?.message ? ` (${first.message})` : ""}`);
    }
  }, [refreshers]);

  useEffect(() => {
    if (!graphFullscreen) return;
    function onKey(event) {
      if (event.key === "Escape") setGraphFullscreen(false);
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [graphFullscreen, setGraphFullscreen]);

  useEffect(() => {
    refreshAll()
      .catch((err) => { setStatusText(`Dashboard error: ${err.message}`); })
      .finally(() => setInitialLoading(false));
  }, [refreshAll, setInitialLoading, setStatusText]);

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
    activeView,
    activeSubview,
    flowView,
    flowVertical,
    getKey: getEmpireKey,
    patchFlowEvent: workflowStream.patchRuntimeFlowEvent,
  });

  useReplayTicker({
    flowView,
    flowReplayOn,
    flowReplaySpeed,
    flowEvents,
    setFlowReplayIndex,
    setFlowReplayOn,
  });

  useDashboardPolling({
    loadLogs,
    loadRuntimeLogs,
    loadIncidents,
    loadConversations,
    activeView,
    activeSubview,
    loadGraph,
    flowView,
    loadPipelineFlow,
  });
}
