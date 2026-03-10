import type { AgentsResponse, MailboxResponse } from "../types/core.ts";
import type { FunnelResponse, HoldingResponse } from "../types/portfolio.ts";
import type { IncidentRecord } from "../types/runtime.ts";
import type { FlowEventRecord } from "../types/workflow.ts";
import { useDashboardDerivedState } from "./useDashboardDerivedState.ts";

type TabsStateInput = {
  agentsResp: AgentsResponse;
  incidentsData: IncidentRecord[];
  flowEvents: FlowEventRecord[];
  mailbox: MailboxResponse;
  funnel: FunnelResponse;
  holdingData: HoldingResponse;
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
