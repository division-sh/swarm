import { useDashboardDerivedState } from "./useDashboardDerivedState.ts";

type TabsStateInput = {
  agentsResp: Record<string, any>;
  incidentsData: Record<string, any>[];
  flowEvents: Record<string, any>[];
  mailbox: Record<string, any>;
  funnel: Record<string, any>;
  holdingData: Record<string, any>;
};

export function useDashboardTabsState({
  agentsResp,
  incidentsData,
  flowEvents,
  mailbox,
  funnel,
  holdingData,
}: TabsStateInput) {
  return useDashboardDerivedState({
    agentsResp,
    incidentsData,
    flowEvents,
    mailbox,
    funnel,
    holdingData,
  });
}
