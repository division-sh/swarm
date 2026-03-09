import { useMemo } from "react";
import type {
  AgentsResponse,
  DigestResponse,
  HealthResponse,
  MailboxResponse,
  OverviewResponse,
  TasksResponse,
} from "../../types/core.ts";
import type { FunnelResponse, HoldingResponse } from "../../types/portfolio.ts";
import { deriveOverviewState } from "./useOverviewDerivedState.ts";

type OpenView = (view: string, subview?: string) => void;

type OverviewControllerInput = {
  overview: OverviewResponse;
  digestResp: DigestResponse;
  agentsResp: AgentsResponse;
  incidentsData: Record<string, any>[];
  mailbox: MailboxResponse;
  tasksResp: TasksResponse;
  health: HealthResponse;
  funnel: FunnelResponse;
  holdingData: HoldingResponse;
  openView: OpenView;
};

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
}: OverviewControllerInput) {
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
    derived,
    digestResp,
    funnel,
    health,
    holdingData,
    incidentsData,
    mailbox,
    openView,
    overview,
    tasksResp,
  ]);
}
