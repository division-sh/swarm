import { useMemo } from "react";

export function useOverviewController({
  overview,
  digestResp,
  agentsResp,
  incidentsData,
  mailbox,
  tasksResp,
  health,
  funnel,
  holdingData,
  setActiveView,
}) {
  return useMemo(() => ({
    state: {
      overview,
      digestResp,
      agentsResp,
      incidentsData,
      mailbox,
      tasksResp,
      health,
      funnel,
      holdingData,
    },
    actions: {
      openTab: setActiveView,
    },
  }), [
    agentsResp,
    digestResp,
    funnel,
    health,
    holdingData,
    incidentsData,
    mailbox,
    overview,
    setActiveView,
    tasksResp,
  ]);
}
