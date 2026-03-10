import React, { useEffect, useMemo, useState } from "react";
import {
  DEFAULT_INCIDENTS_FILTER,
  deriveObservabilityFocus,
  EMPTY_EVENT_FILTER,
  EMPTY_LOG_FILTER,
} from "./focus.ts";
import ObservabilityWorkbench from "./ObservabilityWorkbench.tsx";
import { deriveObservabilityState } from "./useObservabilityDerivedState.ts";

function routeToSubview(activeView) {
  if (activeView === "events" || activeView === "logs" || activeView === "incidents" || activeView === "overview") {
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
  actions,
}) {
  const routeSubview = routeToSubview(activeView) || activeSubview;
  const [subview, setSubview] = useState(routeSubview || "overview");

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

  const focus = deriveObservabilityFocus({ events, logs, incidents });
  const derived = useMemo(() => deriveObservabilityState({ events, logs, incidents, focus }), [events, logs, incidents, focus]);

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
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Unified runtime tracing, log inspection, and incident response workspace.
      </div>
      <ObservabilityWorkbench
        subview={subview}
        setViewRoute={setViewRoute}
        derived={derived}
        events={{
          state: events.state,
          actions: {
            ...events.actions,
            openLogsForAgent,
            focusEventType,
            focusEventVertical,
            openAgent: actions.openAgent,
            openWorkflowForVertical: actions.openWorkflowTrace,
            openPortfolioForVertical: actions.openPortfolio,
          },
        }}
        logs={{
          state: logs.state,
          actions: {
            ...logs.actions,
            openEvent,
            focusAgentEvents,
            openIncidentFocus,
            openAgent: actions.openAgent,
            openWorkflowForVertical: actions.openWorkflowTrace,
            openPortfolioForVertical: actions.openPortfolio,
          },
        }}
        incidents={{
          state: incidents.state,
          actions: {
            ...incidents.actions,
            focusAgentEvents,
            focusIncidentComponent,
            openAgent: actions.openAgent,
            openWorkflowForVertical: actions.openWorkflowTrace,
            openPortfolioForVertical: actions.openPortfolio,
          },
        }}
        actions={{
          selectSubview,
          applyAgentFocus,
          applyRuntimeErrors,
          applyMcpIncidents,
          clearAll,
          openAgent: actions.openAgent,
          openWorkflowTrace: actions.openWorkflowTrace,
          openPortfolio: actions.openPortfolio,
        }}
      />
    </div>
  );
}
