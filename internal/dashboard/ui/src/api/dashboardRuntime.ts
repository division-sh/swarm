import { fetchJSON } from "./client.ts";
import type {
  ConversationDetail,
  ConversationRecord,
  EventDetail,
  EventFilter,
  EventRecord,
  IncidentArtifacts,
  IncidentFilter,
  IncidentRecord,
  LogFilter,
  RuntimeLogRecord,
} from "../types/runtime.ts";

export async function fetchEvents(eventsFilter?: EventFilter): Promise<EventRecord[]> {
  const p = new URLSearchParams();
  if (eventsFilter?.type) p.set("type", eventsFilter.type);
  if (eventsFilter?.source) p.set("source", eventsFilter.source);
  if (eventsFilter?.vertical) p.set("vertical", eventsFilter.vertical);
  if (eventsFilter?.subscriber) p.set("subscriber", eventsFilter.subscriber);
  p.set("limit", "200");
  const d = await fetchJSON<{ events?: EventRecord[] }>(`/api/events?${p.toString()}`);
  return d.events || [];
}

export async function fetchRuntimeLogs(eventsFilter?: EventFilter, eventsRuntimeErrorsOnly?: boolean): Promise<RuntimeLogRecord[]> {
  const p = new URLSearchParams();
  if (eventsFilter?.type) p.set("type", eventsFilter.type);
  if (eventsFilter?.subscriber) p.set("source", eventsFilter.subscriber);
  else if (eventsFilter?.source) p.set("source", eventsFilter.source);
  if (eventsFilter?.vertical) p.set("vertical", eventsFilter.vertical);
  if (eventsFilter?.component) p.set("component", eventsFilter.component);
  if (eventsFilter?.level) p.set("level", eventsFilter.level);
  else if (eventsRuntimeErrorsOnly) p.set("level", "error");
  p.set("limit", "200");
  const d = await fetchJSON<{ runtime_logs?: RuntimeLogRecord[] }>(`/api/runtime/logs?${p.toString()}`);
  return d.runtime_logs || [];
}

export async function fetchLogs(logsFilter?: LogFilter, logsOrder?: string, logsRuntimeErrorsOnly?: boolean): Promise<RuntimeLogRecord[]> {
  const p = new URLSearchParams();
  if (logsFilter?.type) p.set("type", logsFilter.type);
  if (logsFilter?.subscriber) p.set("source", logsFilter.subscriber);
  else if (logsFilter?.source) p.set("source", logsFilter.source);
  if (logsFilter?.vertical) p.set("vertical", logsFilter.vertical);
  if (logsFilter?.component) p.set("component", logsFilter.component);
  if (logsFilter?.level) p.set("level", logsFilter.level);
  else if (logsRuntimeErrorsOnly) p.set("level", "error");
  p.set("order", logsOrder || "desc");
  p.set("limit", "200");
  const d = await fetchJSON<{ runtime_logs?: RuntimeLogRecord[] }>(`/api/runtime/logs?${p.toString()}`);
  return d.runtime_logs || [];
}

export async function fetchIncidents(incidentsFilter?: IncidentFilter): Promise<IncidentRecord[]> {
  const p = new URLSearchParams();
  p.set("since_hours", String(Math.max(1, Number(incidentsFilter?.sinceHours || 24))));
  p.set("mcp_only", incidentsFilter?.mcpOnly ? "true" : "false");
  if (incidentsFilter?.level) p.set("level", incidentsFilter.level);
  if (incidentsFilter?.component) p.set("component", incidentsFilter.component);
  p.set("limit", "2000");
  const d = await fetchJSON<{ incidents?: IncidentRecord[] }>(`/api/runtime/incidents?${p.toString()}`);
  return d.incidents || [];
}

export async function fetchIncidentLogs(code?: string): Promise<RuntimeLogRecord[]> {
  const c = String(code || "").trim();
  if (!c) return [];
  const p = new URLSearchParams();
  p.set("error_code", c);
  p.set("order", "desc");
  p.set("limit", "250");
  const d = await fetchJSON<{ runtime_logs?: RuntimeLogRecord[] }>(`/api/runtime/logs?${p.toString()}`);
  return d.runtime_logs || [];
}

export async function fetchIncidentArtifacts(agentID?: string): Promise<IncidentArtifacts | null> {
  const id = String(agentID || "").trim();
  if (!id) return null;
  return fetchJSON<IncidentArtifacts>(`/dashboard/api/conversations/${encodeURIComponent(id)}/artifacts?lines=120`);
}

export async function fetchEventDetail(id?: string): Promise<EventDetail | null> {
  const value = String(id || "").trim();
  if (!value) return null;
  return fetchJSON<EventDetail>(`/api/events/${encodeURIComponent(value)}`);
}

export async function fetchConversations(): Promise<ConversationRecord[]> {
  const d = await fetchJSON<{ conversations?: ConversationRecord[] }>("/dashboard/api/conversations?limit=100");
  return d.conversations || [];
}

export async function fetchConversationDetail(agentID?: string): Promise<ConversationDetail> {
  const id = String(agentID || "").trim();
  if (!id) return { messages: [], turns: [] };
  const d = await fetchJSON<Partial<ConversationDetail>>(`/dashboard/api/conversations/${encodeURIComponent(id)}`);
  return { messages: d.messages || [], turns: d.turns || [] };
}
