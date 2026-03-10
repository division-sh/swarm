import type { EventRecord, IncidentRecord, RuntimeLogRecord } from "../../types/runtime.ts";

type ObservabilityFocus = {
  agent?: string;
  vertical?: string;
  component?: string;
  incidentCode?: string;
  chips?: string[];
};

type ObservabilityState<T> = {
  state?: T;
};

type EventsState = {
  filteredEvents?: EventRecord[];
  filteredRuntimeLogs?: RuntimeLogRecord[];
};

type LogsState = {
  filteredLogsData?: RuntimeLogRecord[];
};

type IncidentsState = {
  incidentsData?: IncidentRecord[];
};

function hasRuntimeError(item: RuntimeLogRecord | null | undefined): boolean {
  if (!item || typeof item !== "object") return false;
  if ((item.error_code || "").trim() !== "") return true;
  const level = String(item.level || "").toLowerCase();
  if (level === "error") return true;
  return (item.error || "").trim() !== "";
}

function hasEventError(item: EventRecord | null | undefined): boolean {
  if (!item || typeof item !== "object") return false;
  return Number(item.error_count || 0) > 0 || Number(item.dead_count || 0) > 0;
}

function trim(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

export function deriveObservabilityState({
  events,
  logs,
  incidents,
  focus,
}: {
  events: ObservabilityState<EventsState>;
  logs: ObservabilityState<LogsState>;
  incidents: ObservabilityState<IncidentsState>;
  focus: ObservabilityFocus;
}) {
  const filteredEvents = Array.isArray(events?.state?.filteredEvents) ? events.state.filteredEvents : [];
  const filteredRuntimeLogs = Array.isArray(events?.state?.filteredRuntimeLogs) ? events.state.filteredRuntimeLogs : [];
  const filteredLogs = Array.isArray(logs?.state?.filteredLogsData) ? logs.state.filteredLogsData : [];
  const incidentsData = Array.isArray(incidents?.state?.incidentsData) ? incidents.state.incidentsData : [];

  const eventErrors = filteredEvents.filter(hasEventError);
  const runtimeErrors = filteredRuntimeLogs.filter(hasRuntimeError);
  const logErrors = filteredLogs.filter(hasRuntimeError);
  const criticalIncidents = incidentsData.filter((incident) => Number(incident.count || 0) > 0);

  const hotspots = [
    ...criticalIncidents.slice(0, 3).map((incident) => ({
      id: `incident:${incident.code}`,
      kind: "incident",
      title: incident.code,
      subtitle: `${incident.root_cause || "runtime incident"} • ${incident.count || 0} hits`,
      agent: Array.isArray(incident.agents) ? trim(incident.agents[0]) : "",
      vertical: "",
      subview: "incidents",
    })),
    ...logErrors.slice(0, 3).map((log) => ({
      id: `log:${log.id}`,
      kind: "log",
      title: `${log.component || "runtime"}.${log.action || "event"}`,
      subtitle: trim(log.error_code || log.error || log.event_type || "runtime error"),
      agent: trim(log.agent_id),
      vertical: trim(log.vertical_id),
      subview: "logs",
    })),
    ...eventErrors.slice(0, 3).map((event) => ({
      id: `event:${event.id}`,
      kind: "event",
      title: event.type || event.id,
      subtitle: `${event.error_count || 0} errors • ${event.pending_count || 0} pending`,
      agent: trim(event.source_agent),
      vertical: trim(event.vertical_slug || event.vertical_id),
      subview: "events",
    })),
  ].slice(0, 8);

  return {
    summary: {
      filteredEvents: filteredEvents.length,
      runtimeLogs: filteredRuntimeLogs.length,
      logRows: filteredLogs.length,
      incidents: incidentsData.length,
      eventErrors: eventErrors.length,
      runtimeErrors: runtimeErrors.length + logErrors.length,
      criticalIncidents: criticalIncidents.length,
      focusActive: focus?.chips?.length || 0,
    },
    focusSummary: {
      agent: trim(focus?.agent),
      vertical: trim(focus?.vertical),
      component: trim(focus?.component),
      incidentCode: trim(focus?.incidentCode),
      chips: Array.isArray(focus?.chips) ? focus.chips : [],
    },
    hotspots,
  };
}
