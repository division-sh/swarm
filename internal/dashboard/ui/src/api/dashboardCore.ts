import { fetchAgents } from "./agents.ts";
import { fetchHealth } from "./health.ts";
import { adaptDigest } from "../adapters/digest.ts";
import { adaptMailbox } from "../adapters/mailbox.ts";
import { adaptOverview } from "../adapters/overview.ts";
import { fetchEvents, fetchIncidents } from "./dashboardRuntime.ts";
import { fetchGenericMailbox } from "./resources/mailbox.ts";
import { fetchJSON } from "./client.ts";
import type {
  AgentsResponse,
  DigestResponse,
  HealthResponse,
  MailboxResponse,
  MailboxSummary,
  OverviewResponse,
  TargetRecord,
  TaskRecord,
  TasksResponse,
  WeeklyBudget,
} from "../types/core.ts";

export async function fetchOverview(): Promise<OverviewResponse> {
  const [agentsResp, health, mailbox, tasksResp, events, incidents] = await Promise.all([
    fetchAgents(),
    fetchHealth(),
    fetchMailbox("all"),
    fetchTasks("open"),
    fetchEvents(),
    fetchIncidents({ sinceHours: 24, mcpOnly: false }),
  ]);
  return adaptOverview({
    agentsResp,
    health,
    mailbox,
    tasksResp,
    events,
    incidents,
  });
}

export async function fetchDigest(top = 10): Promise<DigestResponse> {
  const [agentsResp, health, mailbox, tasksResp, incidents] = await Promise.all([
    fetchAgents(),
    fetchHealth(),
    fetchMailbox("all"),
    fetchTasks("open"),
    fetchIncidents({ sinceHours: 24, mcpOnly: false }),
  ]);
  const digest = adaptDigest({
    agentsResp,
    health,
    mailbox,
    tasksResp,
    incidents,
  });
  if (digest && Array.isArray(digest.top) && digest.top.length > top) {
    digest.top = digest.top.slice(0, top);
  }
  return digest;
}

export async function fetchTasks(status?: string): Promise<TasksResponse> {
  const p = new URLSearchParams();
  p.set("status", status || "open");
  p.set("limit", "250");
  const d = await fetchJSON<{ tasks?: TaskRecord[]; weekly_budget?: WeeklyBudget }>(`/api/tasks?${p.toString()}`);
  return {
    tasks: d.tasks || [],
    weekly_budget: d.weekly_budget || {},
  };
}

export async function fetchTaskStats(): Promise<Record<string, unknown> | null> {
  return (await fetchJSON<Record<string, unknown> | null>("/api/tasks/stats")) || null;
}

export async function fetchMailbox(status?: string): Promise<MailboxResponse> {
  const requestedStatus = status || "all";
  const d = await fetchJSON<{ summary?: MailboxSummary; items?: MailboxResponse["items"] }>(`/api/mailbox?status=${encodeURIComponent(requestedStatus)}&limit=150`);
  if (d.summary) {
    return {
      summary: d.summary || {},
      items: d.items || [],
    };
  }
  const generic = Array.isArray(d.items) ? d.items : await fetchGenericMailbox(requestedStatus, 150);
  return adaptMailbox(generic);
}

export async function fetchTargets(): Promise<TargetRecord[]> {
  const agents = await fetchAgents();
  return (agents.agents || []).map((agent) => ({
    agent_id: String(agent.agent_id || agent.id || ""),
    role: agent.role,
    status: agent.status || agent.state,
    vertical_slug: agent.vertical_slug || agent.vertical_id,
  }));
}

export async function fetchDashboardHealth(): Promise<HealthResponse> {
  return (await fetchHealth()) || {};
}

export async function fetchDashboardAgents(): Promise<AgentsResponse> {
  return fetchAgents();
}
