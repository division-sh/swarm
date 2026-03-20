import type {
  AgentsResponse,
  DigestResponse,
  HealthResponse,
  MailboxResponse,
  TasksResponse,
} from "../types/core.ts";
import type { IncidentRecord } from "../types/runtime.ts";

type DigestInput = {
  agentsResp: AgentsResponse;
  health: HealthResponse;
  mailbox: MailboxResponse;
  tasksResp: TasksResponse;
  incidents: IncidentRecord[];
  generatedAt?: string;
};

function countAttentionAgents(agentsResp: AgentsResponse): number {
  return (agentsResp.agents || []).filter((agent) => {
    const state = String(agent.state || "").toLowerCase();
    return state === "stuck" || Boolean(agent.near_breaker) || Number(agent.pending_events || 0) > 0;
  }).length;
}

export function adaptDigest({
  agentsResp,
  health,
  mailbox,
  tasksResp,
  incidents,
  generatedAt,
}: DigestInput): DigestResponse {
  const timestamp = generatedAt || new Date().toISOString();
  const pendingMailbox = Number(mailbox.summary?.pending || 0);
  const openTasks = Array.isArray(tasksResp.tasks) ? tasksResp.tasks.length : 0;
  const attentionAgents = countAttentionAgents(agentsResp);
  const incidentCount = Array.isArray(incidents) ? incidents.length : 0;
  const runtimeStatus = health.runtime?.running ? "running" : "stopped";
  const text = [
    `Runtime is ${runtimeStatus}.`,
    `${attentionAgents} agents need attention.`,
    `${pendingMailbox} mailbox items are pending.`,
    `${openTasks} tasks are currently open.`,
    `${incidentCount} incidents are currently visible.`,
  ].join(" ");

  return {
    generated_at: timestamp,
    summary: text,
    current: {
      text,
    },
    last_compiled: {
      at: timestamp,
      payload: {
        runtime_running: Boolean(health.runtime?.running),
        attention_agents: attentionAgents,
        pending_mailbox: pendingMailbox,
        open_tasks: openTasks,
        incidents: incidentCount,
      },
    },
    top: (incidents || []).slice(0, 10).map((incident) => ({
      code: incident.code,
      count: incident.count,
      component: incident.component,
      level: incident.level,
      last_seen: incident.last_seen,
    })),
  };
}
