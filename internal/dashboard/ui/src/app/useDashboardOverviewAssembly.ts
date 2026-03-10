import type {
  AgentsResponse,
  DigestResponse,
  HealthResponse,
  MailboxResponse,
  OverviewResponse,
  TasksResponse,
} from "../types/core.ts";
import type { FunnelResponse, HoldingResponse } from "../types/portfolio.ts";
import type { IncidentRecord } from "../types/runtime.ts";
import { useOverviewController } from "../features/overview/useOverviewController.ts";

type OpenView = (view: string, subview?: string) => void;

type OverviewAssemblyInput = {
  overview: OverviewResponse;
  digestResp: DigestResponse;
  agentsResp: AgentsResponse;
  incidentsData: IncidentRecord[];
  mailbox: MailboxResponse;
  tasksResp: TasksResponse;
  health: HealthResponse;
  funnel: FunnelResponse;
  holdingData: HoldingResponse;
  openView: OpenView;
};

export function useDashboardOverviewAssembly(input: OverviewAssemblyInput) {
  return useOverviewController(input);
}
