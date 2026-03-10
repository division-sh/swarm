import { useMemo } from "react";
import type { AgentsResponse, MailboxResponse } from "../types/core.ts";
import type { FunnelResponse, HoldingResponse } from "../types/portfolio.ts";
import type { IncidentRecord } from "../types/runtime.ts";
import type { FlowEventRecord } from "../types/workflow.ts";
import { buildTabBadges, DASHBOARD_TABS } from "./dashboardTabs.ts";

type DerivedStateInput = {
  agentsResp: AgentsResponse;
  incidentsData: IncidentRecord[];
  flowEvents: FlowEventRecord[];
  mailbox: MailboxResponse;
  funnel: FunnelResponse;
  holdingData: HoldingResponse;
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
