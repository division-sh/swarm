import { useMemo } from "react";
import { buildTabBadges, DASHBOARD_TABS } from "./dashboardTabs.ts";

type DerivedStateInput = {
  agentsResp: Record<string, any>;
  incidentsData: Record<string, any>[];
  flowEvents: Record<string, any>[];
  mailbox: Record<string, any>;
  funnel: Record<string, any>;
  holdingData: Record<string, any>;
};

export function useDashboardDerivedState({
  agentsResp,
  incidentsData,
  flowEvents,
  mailbox,
  funnel,
  holdingData,
}: DerivedStateInput) {
  const tabBadges = useMemo(() => buildTabBadges({
    agentsResp,
    mailbox,
    funnel,
    holdingData,
    incidentsData,
    flowEvents,
  }), [agentsResp, mailbox, funnel, holdingData, incidentsData, flowEvents]);

  return {
    tabBadges,
    tabs: [...DASHBOARD_TABS],
  };
}
