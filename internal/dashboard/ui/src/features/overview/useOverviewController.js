import { useMemo } from "react";
import { deriveOverviewState } from "./useOverviewDerivedState.js";

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
  const derived = deriveOverviewState({
    agentsResp,
    incidentsData,
    mailbox,
    tasksResp,
    holdingData,
  });

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
      derived,
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
    derived,
  ]);
}
