import { useEffect } from "react";

type DashboardPollingInput = {
  loadRuntimeLogs: () => Promise<unknown>;
  loadLogs: () => Promise<unknown>;
  loadIncidents: () => Promise<unknown>;
  loadConversations: () => Promise<unknown>;
  loadGraph: () => Promise<unknown>;
  activeView: string;
  activeSubview: string;
  flowView: string;
  loadPipelineFlow: () => Promise<unknown>;
};

export function useDashboardPolling({
  loadRuntimeLogs,
  loadLogs,
  loadIncidents,
  loadConversations,
  loadGraph,
  activeView,
  activeSubview,
  flowView,
  loadPipelineFlow,
}: DashboardPollingInput) {
  useEffect(() => {
    const observabilitySubview = activeView === "observability" ? (activeSubview || "events") : activeView;
    const workflowSubview = activeView === "workflow" ? (activeSubview || "flow") : activeView;

    const i1 = setInterval(() => {
      if (document.hidden) return;
      loadRuntimeLogs().catch(() => {});
      loadIncidents().catch(() => {});
      if (observabilitySubview === "logs") {
        loadLogs().catch(() => {});
      }
      if (activeView === "agents") {
        loadConversations().catch(() => {});
      }
    }, 15000);
    const i2 = setInterval(() => {
      if (document.hidden) return;
      if (workflowSubview === "graph") {
        loadGraph().catch(() => {});
      }
      if (workflowSubview === "flow" && flowView !== "runtime") {
        loadPipelineFlow().catch(() => {});
      }
    }, 22000);
    return () => {
      clearInterval(i1);
      clearInterval(i2);
    };
  }, [loadRuntimeLogs, loadLogs, loadIncidents, loadConversations, loadGraph, activeView, activeSubview, flowView, loadPipelineFlow]);
}
