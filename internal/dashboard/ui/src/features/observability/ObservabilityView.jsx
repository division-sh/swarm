import React, { useEffect, useState } from "react";
import EventsView from "../events/EventsView.jsx";
import IncidentsView from "../incidents/IncidentsView.jsx";
import LogsView from "../logs/LogsView.jsx";
import {
  DEFAULT_INCIDENTS_FILTER,
  deriveObservabilityFocus,
  EMPTY_EVENT_FILTER,
  EMPTY_LOG_FILTER,
} from "./focus.js";

function routeToSubview(activeView) {
  if (activeView === "events" || activeView === "logs" || activeView === "incidents") {
    return activeView;
  }
  return "";
}

export default function ObservabilityView({
  activeView,
  activeSubview,
  setViewRoute,
  events,
  logs,
  incidents,
}) {
  const routeSubview = routeToSubview(activeView) || activeSubview;
  const [subview, setSubview] = useState(routeSubview || "events");

  useEffect(() => {
    if (!routeSubview) return;
    setSubview(routeSubview);
    if (activeView === "events" || activeView === "logs" || activeView === "incidents") {
      setViewRoute("observability", routeSubview);
    }
  }, [activeView, routeSubview, setViewRoute]);

  function selectSubview(next) {
    setSubview(next);
    setViewRoute("observability", next);
  }

  const eventCount = Array.isArray(events.state.filteredEvents) ? events.state.filteredEvents.length : 0;
  const logCount = Array.isArray(logs.state.filteredLogsData) ? logs.state.filteredLogsData.length : 0;
  const incidentCount = Array.isArray(incidents.state.incidentsData) ? incidents.state.incidentsData.length : 0;
  const focus = deriveObservabilityFocus({ events, logs, incidents });

  function applyAgentFocus(nextSubview) {
    if (!focus.agent) return;
    events.actions.setEventsFilter({ ...EMPTY_EVENT_FILTER, subscriber: focus.agent });
    events.actions.setEventsIncludeRuntime(true);
    events.actions.setEventsRuntimeErrorsOnly(false);
    events.actions.setSelectedEventID("");
    logs.actions.setLogsFilter({ ...EMPTY_LOG_FILTER, source: focus.agent });
    logs.actions.setLogsRuntimeErrorsOnly(false);
    logs.actions.setSelectedLogID(null);
    selectSubview(nextSubview);
  }

  function applyRuntimeErrors() {
    events.actions.setEventsIncludeRuntime(true);
    events.actions.setEventsRuntimeErrorsOnly(true);
    logs.actions.setLogsRuntimeErrorsOnly(true);
    selectSubview("logs");
  }

  function applyMcpIncidents() {
    incidents.actions.setIncidentsFilter({ ...DEFAULT_INCIDENTS_FILTER, mcpOnly: true });
    selectSubview("incidents");
  }

  function clearAll() {
    events.actions.clear();
    events.actions.setSelectedEventID("");
    logs.actions.clear();
    logs.actions.setSelectedLogID(null);
    incidents.actions.resetFilters();
    incidents.actions.setSelectedIncidentCode("");
    incidents.actions.setSelectedIncidentAgent("");
  }

  function openLogsForAgent(agentID) {
    const value = String(agentID || "").trim();
    if (!value) return;
    logs.actions.setLogsFilter({ ...EMPTY_LOG_FILTER, source: value });
    logs.actions.setLogsRuntimeErrorsOnly(false);
    logs.actions.setSelectedLogID(null);
    selectSubview("logs");
  }

  function focusAgentEvents(agentID) {
    const value = String(agentID || "").trim();
    if (!value) return;
    events.actions.setEventsFilter({ ...EMPTY_EVENT_FILTER, subscriber: value });
    events.actions.setEventsIncludeRuntime(true);
    events.actions.setEventsRuntimeErrorsOnly(false);
    events.actions.setSelectedEventID("");
    selectSubview("events");
  }

  function focusEventType(eventType) {
    const value = String(eventType || "").trim();
    if (!value) return;
    events.actions.setEventsFilter((prev) => ({ ...prev, type: value }));
    events.actions.setSelectedEventID("");
  }

  function focusEventVertical(vertical) {
    const value = String(vertical || "").trim();
    if (!value) return;
    events.actions.setEventsFilter({ ...EMPTY_EVENT_FILTER, vertical: value });
    events.actions.setEventsIncludeRuntime(true);
    events.actions.setEventsRuntimeErrorsOnly(false);
    events.actions.setSelectedEventID("");
    selectSubview("events");
  }

  function openEvent(eventID) {
    const value = String(eventID || "").trim();
    if (!value) return;
    events.actions.setEventsFilter(EMPTY_EVENT_FILTER);
    events.actions.setEventsIncludeRuntime(true);
    events.actions.setEventsRuntimeErrorsOnly(false);
    events.actions.setSelectedEventID(value);
    selectSubview("events");
  }

  function focusIncidentComponent(component) {
    const value = String(component || "").trim();
    if (!value) return;
    incidents.actions.setIncidentsFilter((prev) => ({ ...prev, component: value }));
    selectSubview("incidents");
  }

  function openIncidentFocus(log) {
    const component = String(log?.component || "").trim();
    const errorCode = String(log?.error_code || "").trim();
    if (component) {
      incidents.actions.setIncidentsFilter((prev) => ({ ...prev, component }));
    }
    if (errorCode) {
      incidents.actions.setSelectedIncidentCode(errorCode);
    }
    selectSubview("incidents");
  }

  return (
    <div>
      <div className="head">
        <h2>Observability</h2>
        <div className="stack">
          <button className={subview === "events" ? "active" : ""} onClick={() => selectSubview("events")}>
            Event Trace
          </button>
          <button className={subview === "logs" ? "active" : ""} onClick={() => selectSubview("logs")}>
            Runtime Logs
          </button>
          <button className={subview === "incidents" ? "active" : ""} onClick={() => selectSubview("incidents")}>
            Incidents
          </button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Unified runtime tracing, log inspection, and incident triage. {eventCount} filtered events, {logCount} filtered logs, {incidentCount} incidents in the current window.
      </div>
      <div className="health-card" style={{ marginBottom: 10 }}>
        <div className="stack" style={{ justifyContent: "space-between", marginBottom: 8 }}>
          <div>
            <div className="tiny">Focus Context</div>
            <div>{focus.chips.length > 0 ? focus.chips.join(" | ") : "No active observability focus"}</div>
          </div>
          <div className="stack">
            {focus.agent ? (
              <>
                <button className="btn-secondary" onClick={() => applyAgentFocus("events")}>Agent Events</button>
                <button className="btn-secondary" onClick={() => applyAgentFocus("logs")}>Agent Logs</button>
              </>
            ) : null}
            <button className="btn-secondary" onClick={applyRuntimeErrors}>Runtime Errors</button>
            <button className="btn-secondary" onClick={applyMcpIncidents}>MCP Incidents</button>
            <button className="btn-secondary" onClick={clearAll}>Reset Focus</button>
          </div>
        </div>
        <div className="stack tiny">
          <span>Agent: <span className="mono">{focus.agent || "-"}</span></span>
          <span>Vertical: <span className="mono">{focus.vertical || "-"}</span></span>
          <span>Component: <span className="mono">{focus.component || "-"}</span></span>
          <span>Incident: <span className="mono">{focus.incidentCode || "-"}</span></span>
        </div>
      </div>
      {subview === "events" ? (
        <EventsView
          state={events.state}
          actions={{
            ...events.actions,
            openLogsForAgent,
            focusEventType,
            focusEventVertical,
          }}
        />
      ) : null}
      {subview === "logs" ? (
        <LogsView
          state={logs.state}
          actions={{
            ...logs.actions,
            openEvent,
            focusAgentEvents,
            openIncidentFocus,
          }}
        />
      ) : null}
      {subview === "incidents" ? (
        <IncidentsView
          state={incidents.state}
          actions={{
            ...incidents.actions,
            focusAgentEvents,
            focusIncidentComponent,
          }}
        />
      ) : null}
    </div>
  );
}
