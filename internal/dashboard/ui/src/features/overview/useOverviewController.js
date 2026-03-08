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
  openView,
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
      openView,
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
    openView,
    tasksResp,
  ]);
}
