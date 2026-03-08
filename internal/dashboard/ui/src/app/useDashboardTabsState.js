import { useDashboardDerivedState } from "./useDashboardDerivedState.js";

export function useDashboardTabsState({
  agentsResp,
  incidentsData,
  flowEvents,
  mailbox,
  funnel,
  holdingData,
}) {
  return useDashboardDerivedState({
    agentsResp,
    incidentsData,
    flowEvents,
    mailbox,
    funnel,
    holdingData,
  });
}
