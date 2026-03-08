import React, { useEffect, useState } from "react";
import EventsView from "../events/EventsView.jsx";
import IncidentsView from "../incidents/IncidentsView.jsx";
import LogsView from "../logs/LogsView.jsx";

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
      {subview === "events" ? (
        <EventsView state={events.state} actions={events.actions} />
      ) : null}
      {subview === "logs" ? (
        <LogsView state={logs.state} actions={logs.actions} />
      ) : null}
      {subview === "incidents" ? (
        <IncidentsView state={incidents.state} actions={incidents.actions} />
      ) : null}
    </div>
  );
}
