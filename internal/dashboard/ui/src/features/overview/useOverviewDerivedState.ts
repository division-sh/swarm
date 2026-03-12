import type { AgentsResponse, MailboxResponse, TasksResponse } from "../../types/core.ts";
import type { HoldingResponse } from "../../types/portfolio.ts";
import type { IncidentRecord } from "../../types/runtime.ts";

function clampPriority(n: number): number {
  return Number.isFinite(n) ? n : 0;
}

type UrgentItem = {
  kind: string;
  id: string;
  title: string;
  subtitle: string;
  priority: number;
  route: { view: string; subview: string };
};

export function deriveOverviewState({
  agentsResp,
  incidentsData,
  mailbox,
  tasksResp,
  holdingData,
}: {
  agentsResp: AgentsResponse;
  incidentsData: IncidentRecord[];
  mailbox: MailboxResponse;
  tasksResp: TasksResponse;
  holdingData: HoldingResponse;
}): { urgentNow: UrgentItem[] } {
  const agents = Array.isArray(agentsResp?.agents) ? agentsResp.agents : [];
  const incidents = Array.isArray(incidentsData) ? incidentsData : [];
  const mailboxItems = Array.isArray(mailbox?.items) ? mailbox.items : [];
  const tasks = Array.isArray(tasksResp?.tasks) ? tasksResp.tasks : [];
  const verticals = Array.isArray(holdingData?.verticals) ? holdingData.verticals : [];

  const urgentNow = [
    ...agents
      .filter((agent) => agent.state === "stuck")
      .map((agent) => ({
        kind: "agent",
        id: agent.id,
        title: agent.stuck_reason || `${agent.id} is stuck`,
        subtitle: [agent.role, agent.vertical_slug || "holding", `${agent.pending_events || 0} pending`].filter(Boolean).join(" · "),
        priority: 90 + clampPriority(Number(agent.pending_events)),
        route: { view: "agents", subview: "" },
      })),
    ...incidents.map((incident) => ({
      kind: "incident",
      id: incident.code,
      title: incident.code,
      subtitle: [incident.component, `${incident.count || 0} occurrences`].filter(Boolean).join(" · "),
      priority: 80 + clampPriority(Number(incident.count)),
      route: { view: "observability", subview: "incidents" },
    })),
    ...mailboxItems
      .filter((item) => item.status === "pending")
      .map((item) => ({
        kind: "mailbox",
        id: item.id,
        title: item.summary || item.type || item.id,
        subtitle: [item.from_agent, item.vertical_slug || item.vertical_id, item.priority].filter(Boolean).join(" · "),
        priority: 70 + (String(item.priority || "").toLowerCase() === "critical" ? 20 : 0),
        route: { view: "operations", subview: "queue" },
      })),
    ...tasks
      .filter((task) => ["open", "pending_review", "assigned"].includes(String(task.status || "").toLowerCase()))
      .map((task) => ({
        kind: "task",
        id: task.id,
        title: task.description || task.category || task.id,
        subtitle: [task.requesting_agent, task.vertical_slug, task.priority].filter(Boolean).join(" · "),
        priority: 60 + (String(task.priority || "").toLowerCase() === "p1" ? 20 : 0),
        route: { view: "operations", subview: "queue" },
      })),
    ...verticals
      .filter((vertical) => vertical.workflow_current_state !== vertical.stage || Number(vertical.active_timer_count || 0) > 0)
      .map((vertical) => ({
        kind: "vertical",
        id: vertical.id,
        title: vertical.slug || vertical.name || vertical.id,
        subtitle: [
          vertical.workflow_current_state !== vertical.stage ? "stage drift" : "",
          Number(vertical.active_timer_count || 0) > 0 ? `${vertical.active_timer_count} timers` : "",
        ].filter(Boolean).join(" · "),
        priority: 50 + (vertical.workflow_current_state !== vertical.stage ? 20 : 0) + clampPriority(Number(vertical.active_timer_count)),
        route: { view: "portfolio", subview: "holding" },
      })),
  ].sort((a, b) => b.priority - a.priority).slice(0, 8);

  return { urgentNow };
}
