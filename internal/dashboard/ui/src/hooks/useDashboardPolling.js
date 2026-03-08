import { useEffect } from "react";

export function useDashboardPolling({
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
  activeView,
  flowView,
  loadPipelineFlow,
}) {
  useEffect(() => {
    const i1 = setInterval(() => {
      if (document.hidden) return;
      loadOverview().catch(() => {});
      loadAgents().catch(() => {});
      loadDigest().catch(() => {});
      loadTasks().catch(() => {});
      loadMailbox().catch(() => {});
      loadHealth().catch(() => {});
      loadRuntimeLogs().catch(() => {});
      loadIncidents().catch(() => {});
    }, 15000);
    const i2 = setInterval(() => {
      if (document.hidden) return;
      loadTargets().catch(() => {});
      loadFunnel().catch(() => {});
      loadVerticals().catch(() => {});
      loadHolding().catch(() => {});
      if (activeView === "flow" && flowView !== "runtime") {
        loadPipelineFlow().catch(() => {});
      }
    }, 22000);
    return () => {
      clearInterval(i1);
      clearInterval(i2);
    };
  }, [loadOverview, loadAgents, loadDigest, loadTasks, loadMailbox, loadHealth, loadRuntimeLogs, loadIncidents, loadTargets, loadFunnel, loadVerticals, loadHolding, activeView, flowView, loadPipelineFlow]);
}
