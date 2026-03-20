import type {
  AgentsResponse,
  HealthResponse,
  MailboxResponse,
  OverviewResponse,
  TasksResponse,
} from "../types/core.ts";
import type { EventRecord, IncidentRecord } from "../types/runtime.ts";

type OverviewInput = {
  agentsResp: AgentsResponse;
  health: HealthResponse;
  mailbox: MailboxResponse;
  tasksResp: TasksResponse;
  events: EventRecord[];
  incidents: IncidentRecord[];
  generatedAt?: string;
};

function countActiveAgents(agentsResp: AgentsResponse): number {
  return (agentsResp.agents || []).filter((agent) => String(agent.state || "").toLowerCase() !== "terminated").length;
}

export function adaptOverview({
  agentsResp,
  health,
  mailbox,
  tasksResp,
  events,
  incidents,
  generatedAt,
}: OverviewInput): OverviewResponse {
  const timestamp = generatedAt || new Date().toISOString();
  return {
    generated_at: timestamp,
    agents_active: countActiveAgents(agentsResp),
    events_24h: Array.isArray(events) ? events.length : 0,
    summary: {
      runtime_running: Boolean(health.runtime?.running),
      incidents_open: Array.isArray(incidents) ? incidents.length : 0,
      mailbox_pending: Number(mailbox.summary?.pending || 0),
      tasks_open: Array.isArray(tasksResp.tasks) ? tasksResp.tasks.length : 0,
    },
  };
}
