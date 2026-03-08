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
  setActiveView,
  events,
  logs,
  incidents,
}) {
  const routeSubview = routeToSubview(activeView);
  const [subview, setSubview] = useState(routeSubview || "events");

  useEffect(() => {
    if (!routeSubview) return;
    setSubview(routeSubview);
    if (activeView !== "observability") {
      setActiveView("observability");
    }
  }, [activeView, routeSubview, setActiveView]);

  function selectSubview(next) {
    setSubview(next);
    if (activeView !== "observability") {
      setActiveView("observability");
    }
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
            Event Trace{eventCount > 0 ? ` (${eventCount})` : ""}
          </button>
          <button className={subview === "logs" ? "active" : ""} onClick={() => selectSubview("logs")}>
            Runtime Logs{logCount > 0 ? ` (${logCount})` : ""}
          </button>
          <button className={subview === "incidents" ? "active" : ""} onClick={() => selectSubview("incidents")}>
            Incidents{incidentCount > 0 ? ` (${incidentCount})` : ""}
          </button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Unified runtime tracing, log inspection, and incident triage. Legacy event/log/incident routes remain supported and land here.
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
