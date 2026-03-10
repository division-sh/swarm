import type { AgentsResponse, MailboxResponse } from "../types/core.ts";
import type { FunnelResponse, HoldingResponse } from "../types/portfolio.ts";
import type { IncidentRecord } from "../types/runtime.ts";
import type { FlowEventRecord } from "../types/workflow.ts";

export const VALID_TABS = ["overview", "agents", "observability", "workflow", "portfolio", "operations", "digest", "events", "logs", "incidents", "flow", "convos", "graph", "control", "tasks", "pipeline", "holding", "health"] as const;

export const DASHBOARD_TABS = [
  ["overview", "Overview"],
  ["agents", "Agents"],
  ["observability", "Observability"],
  ["workflow", "Workflow"],
  ["portfolio", "Portfolio"],
  ["operations", "Operations"],
  ["health", "Health"],
] as const;

type TabBadge = { n: number; type: string } | null;

function num(value: unknown): number {
  return Number(value || 0);
}

export function buildTabBadges({
  agentsResp,
  mailbox,
  funnel,
  holdingData,
  incidentsData,
  flowEvents,
}: {
  agentsResp: AgentsResponse;
  mailbox: MailboxResponse;
  funnel: FunnelResponse;
  holdingData: HoldingResponse;
  incidentsData: IncidentRecord[];
  flowEvents: FlowEventRecord[];
}): Record<string, TabBadge> {
  const stuckAgents = num(agentsResp.states.stuck);
  const pendingMailbox = num(mailbox.summary.pending);
  const driftCount = num(holdingData.workflow_summary.drift);

  const overviewCount = stuckAgents
    + (pendingMailbox > 0 ? 1 : 0)
    + ((incidentsData || []).length > 0 ? 1 : 0)
    + (driftCount > 0 ? 1 : 0);

  return {
    overview: overviewCount > 0 ? { n: Math.min(overviewCount, 99), type: incidentsData.length > 0 || stuckAgents > 0 ? "danger" : "warn" } : null,
    observability: (incidentsData || []).length > 0 ? { n: Math.min((incidentsData || []).length, 99), type: "danger" } : null,
    workflow: (flowEvents || []).length > 0 ? { n: Math.min((flowEvents || []).length, 999), type: "warn" } : null,
    portfolio: Math.max((funnel.stuck || []).length, (holdingData.verticals || []).filter((vertical) => vertical.stage === "ready_for_review").length) > 0
      ? {
          n: Math.min(
            Math.max(
              (funnel.stuck || []).length,
              (holdingData.verticals || []).filter((vertical) => vertical.stage === "ready_for_review").length,
            ),
            99,
          ),
          type: "warn",
        }
      : null,
    agents: stuckAgents > 0 ? { n: stuckAgents, type: "danger" } : null,
    operations: pendingMailbox > 0 ? { n: pendingMailbox, type: "warn" } : null,
    pipeline: (funnel.stuck || []).length > 0 ? { n: funnel.stuck.length, type: "warn" } : null,
    holding: (() => {
      const count = (holdingData.verticals || []).filter((vertical) => vertical.stage === "ready_for_review").length;
      return count > 0 ? { n: count, type: "warn" } : null;
    })(),
    incidents: (incidentsData || []).length > 0 ? { n: incidentsData.length, type: "danger" } : null,
    flow: (flowEvents || []).length > 0 ? { n: Math.min((flowEvents || []).length, 999), type: "warn" } : null,
  };
}
