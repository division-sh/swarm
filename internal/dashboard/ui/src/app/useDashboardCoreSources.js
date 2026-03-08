import { useCallback } from "react";
import { fetchAgents } from "../api/agents.js";
import { useDashboardCoreData } from "../hooks/useDashboardCoreData.js";
import { relTime } from "../lib/format.js";

export function useDashboardCoreSources({ ui, domain }) {
  const loadAgents = useCallback(async () => {
    domain.setAgentsResp(await fetchAgents());
  }, [domain]);

  const core = useDashboardCoreData({
    setOverview: domain.setOverview,
    setStatusText: ui.setStatusText,
    relTime,
    taskStatus: domain.taskState.taskStatus,
    setTasksResp: domain.taskState.setTasksResp,
    mailStatus: domain.opsState.mailStatus,
    setMailbox: domain.opsState.setMailbox,
    setDigestResp: domain.setDigestResp,
    setHealth: domain.opsState.setHealth,
    setTargets: domain.opsState.setTargets,
    setControlTarget: domain.opsState.setControlTarget,
  });

  return {
    loadAgents,
    ...core,
  };
}
