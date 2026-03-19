import { fetchAgents } from "./agents.ts";
import { fetchJSON } from "./client.ts";
import { fetchHealth } from "./health.ts";
import { adaptMailbox } from "../adapters/mailbox.ts";
import { fetchGenericMailbox } from "./resources/mailbox.ts";
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
  return (await fetchJSON<OverviewResponse>("/dashboard/api/overview")) || {};
}

export async function fetchDigest(top = 10): Promise<DigestResponse> {
  return (await fetchJSON<DigestResponse>(`/dashboard/api/digest?top=${encodeURIComponent(top)}`)) || null;
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
  const d = await fetchJSON<{ targets?: TargetRecord[] }>("/dashboard/api/control/targets");
  return d.targets || [];
}

export async function fetchDashboardHealth(): Promise<HealthResponse> {
  return (await fetchHealth()) || {};
}

export async function fetchDashboardAgents(): Promise<AgentsResponse> {
  return fetchAgents();
}
