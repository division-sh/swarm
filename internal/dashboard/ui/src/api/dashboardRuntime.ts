import { fetchJSON } from "./client.ts";
import { adaptIncidentArtifacts } from "../adapters/incidentArtifacts.ts";
import { adaptConversationDetail, adaptConversationSummaries } from "../adapters/conversations.ts";
import { fetchGenericConversationDetail, fetchGenericConversations } from "./resources/conversations.ts";
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

function trimString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function normalizeEventRecord(row: EventRecord): EventRecord {
  const payload = asRecord(row.payload);
  const entityID = trimString(row.entity_id) || trimString(payload.entity_id);
  const verticalID = trimString(row.vertical_id) || trimString(payload.vertical_id) || entityID;
  const verticalSlug = trimString(row.vertical_slug) || trimString(payload.vertical_slug) || trimString(payload.vertical) || verticalID;
  return {
    ...row,
    entity_id: entityID,
    vertical_id: verticalID,
    vertical_slug: verticalSlug,
  };
}

function normalizeRuntimeLogRecord(row: RuntimeLogRecord): RuntimeLogRecord {
  const entityID = trimString(row.entity_id);
  return {
    ...row,
    entity_id: entityID,
    vertical_id: trimString(row.vertical_id) || entityID,
  };
}

export async function fetchEvents(eventsFilter?: EventFilter): Promise<EventRecord[]> {
  const p = new URLSearchParams();
  if (eventsFilter?.type) p.set("type", eventsFilter.type);
  if (eventsFilter?.source) p.set("source", eventsFilter.source);
  if (eventsFilter?.entity_id) p.set("entity_id", eventsFilter.entity_id);
  else if (eventsFilter?.vertical) p.set("entity_id", eventsFilter.vertical);
  if (eventsFilter?.subscriber) p.set("subscriber", eventsFilter.subscriber);
  p.set("limit", "200");
  const d = await fetchJSON<{ events?: EventRecord[] }>(`/api/events?${p.toString()}`);
  return (d.events || []).map(normalizeEventRecord);
}

export async function fetchRuntimeLogs(eventsFilter?: EventFilter, eventsRuntimeErrorsOnly?: boolean): Promise<RuntimeLogRecord[]> {
  const p = new URLSearchParams();
  if (eventsFilter?.type) p.set("type", eventsFilter.type);
  if (eventsFilter?.subscriber) p.set("source", eventsFilter.subscriber);
  else if (eventsFilter?.source) p.set("source", eventsFilter.source);
  if (eventsFilter?.entity_id) p.set("entity_id", eventsFilter.entity_id);
  else if (eventsFilter?.vertical) p.set("entity_id", eventsFilter.vertical);
  if (eventsFilter?.component) p.set("component", eventsFilter.component);
  if (eventsFilter?.level) p.set("level", eventsFilter.level);
  else if (eventsRuntimeErrorsOnly) p.set("level", "error");
  p.set("limit", "200");
  const d = await fetchJSON<{ runtime_logs?: RuntimeLogRecord[] }>(`/api/runtime/logs?${p.toString()}`);
  return (d.runtime_logs || []).map(normalizeRuntimeLogRecord);
}

export async function fetchLogs(logsFilter?: LogFilter, logsOrder?: string, logsRuntimeErrorsOnly?: boolean): Promise<RuntimeLogRecord[]> {
  const p = new URLSearchParams();
  if (logsFilter?.type) p.set("type", logsFilter.type);
  if (logsFilter?.subscriber) p.set("source", logsFilter.subscriber);
  else if (logsFilter?.source) p.set("source", logsFilter.source);
  if (logsFilter?.entity_id) p.set("entity_id", logsFilter.entity_id);
  else if (logsFilter?.vertical) p.set("entity_id", logsFilter.vertical);
  if (logsFilter?.component) p.set("component", logsFilter.component);
  if (logsFilter?.level) p.set("level", logsFilter.level);
  else if (logsRuntimeErrorsOnly) p.set("level", "error");
  p.set("order", logsOrder || "desc");
  p.set("limit", "200");
  const d = await fetchJSON<{ runtime_logs?: RuntimeLogRecord[] }>(`/api/runtime/logs?${p.toString()}`);
  return (d.runtime_logs || []).map(normalizeRuntimeLogRecord);
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
  const generic = await fetchGenericConversationDetail(id);
  return adaptIncidentArtifacts(generic);
}

export async function fetchEventDetail(id?: string): Promise<EventDetail | null> {
  const value = String(id || "").trim();
  if (!value) return null;
  const detail = await fetchJSON<EventDetail>(`/api/events/${encodeURIComponent(value)}`);
  return normalizeEventRecord(detail) as EventDetail;
}

export async function fetchConversations(): Promise<ConversationRecord[]> {
  const generic = await fetchGenericConversations(100);
  return adaptConversationSummaries(generic);
}

export async function fetchConversationDetail(agentID?: string): Promise<ConversationDetail> {
  const id = String(agentID || "").trim();
  if (!id) return { messages: [], turns: [] };
  const generic = await fetchGenericConversationDetail(id);
  return adaptConversationDetail(generic);
}
