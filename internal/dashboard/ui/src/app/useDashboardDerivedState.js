import { useMemo } from "react";
import { buildTabBadges, DASHBOARD_TABS } from "./dashboardTabs.js";

export function useDashboardDerivedState({
  agentsResp,
  incidentsData,
  flowEvents,
  mailbox,
  funnel,
  holdingData,
}) {
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
    tabs: DASHBOARD_TABS,
  };
}
