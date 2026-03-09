import { useCallback, useEffect } from "react";
import { getEmpireKey } from "../api/client.ts";
import { useDashboardPolling } from "../hooks/useDashboardPolling.ts";
import { useEventStream } from "../hooks/useEventStream.ts";
import { useFlowRuntimeStream } from "../hooks/useFlowRuntimeStream.ts";
import { useReplayTicker } from "../hooks/useReplayTicker.ts";

type DashboardLifecycleInput = {
  ui: Record<string, any>;
  runtimeState: Record<string, any>;
  pipelineState: Record<string, any>;
  workflowStream: { patchRuntimeFlowEvent: (item: Record<string, any>) => void };
  flowEvents: Record<string, any>[];
  refreshers: {
    loadEvents: () => Promise<unknown>;
    loadRuntimeLogs: () => Promise<unknown>;
    loadConversations: () => Promise<unknown>;
    loadIncidents: () => Promise<unknown>;
  };
  addToast: (message: string, type?: string) => void;
  loadLogs: () => Promise<unknown>;
  loadRuntimeLogs: () => Promise<unknown>;
  loadIncidents: () => Promise<unknown>;
  loadConversations: () => Promise<unknown>;
  loadGraph: () => Promise<unknown>;
  loadPipelineFlow: () => Promise<unknown>;
};

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
}: DashboardLifecycleInput) {
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
    const jobs: Array<[string, () => Promise<unknown>]> = [
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
      const first = (failures[0].result as PromiseRejectedResult).reason;
      throw new Error(`${failures.length} refreshes failed${first?.message ? ` (${first.message})` : ""}`);
    }
  }, [refreshers]);

  useEffect(() => {
    if (!graphFullscreen) return;
    function onKey(event: KeyboardEvent) {
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
