import { DockviewReact, themeAbyssSpaced } from "dockview";
import React, { useCallback, useEffect, useMemo, useRef } from "react";
import EventsView from "../events/EventsView.tsx";
import IncidentsView from "../incidents/IncidentsView.tsx";
import LogsView from "../logs/LogsView.tsx";

function ObservabilityOverviewPanel(props) {
  const params = props.params || {};
  const derived = params.derived || { summary: {}, focusSummary: {}, hotspots: [] };
  const focus = derived.focusSummary || {};

  return (
    <div className="observability-dock-panel">
      <div className="health-card" style={{ marginBottom: 10 }}>
        <div className="stack" style={{ justifyContent: "space-between", marginBottom: 8 }}>
          <div>
            <div className="tiny">Focus Context</div>
            <div>{focus.chips && focus.chips.length > 0 ? focus.chips.join(" | ") : "No active observability focus"}</div>
          </div>
          <div className="stack">
            {focus.agent ? (
              <>
                <button className="btn-secondary" onClick={() => params.applyAgentFocus?.("events")}>Agent Events</button>
                <button className="btn-secondary" onClick={() => params.applyAgentFocus?.("logs")}>Agent Logs</button>
                <button className="btn-secondary" onClick={() => params.openAgent?.(focus.agent)}>Open Agent</button>
              </>
            ) : null}
            {focus.vertical ? (
              <>
                <button className="btn-secondary" onClick={() => params.openWorkflowTrace?.(focus.vertical)}>Workflow</button>
                <button className="btn-secondary" onClick={() => params.openPortfolio?.(focus.vertical)}>Portfolio</button>
              </>
            ) : null}
            <button className="btn-secondary" onClick={params.applyRuntimeErrors}>Runtime Errors</button>
            <button className="btn-secondary" onClick={params.applyMcpIncidents}>MCP Incidents</button>
            <button className="btn-secondary" onClick={params.clearAll}>Reset Focus</button>
          </div>
        </div>
        <div className="stack tiny">
          <span>Agent: <span className="mono">{focus.agent || "-"}</span></span>
          <span>Vertical: <span className="mono">{focus.vertical || "-"}</span></span>
          <span>Component: <span className="mono">{focus.component || "-"}</span></span>
          <span>Incident: <span className="mono">{focus.incidentCode || "-"}</span></span>
        </div>
      </div>

      <div className="metrics-grid" style={{ marginBottom: 10 }}>
        <div className="stat-card">
          <div className="tiny">Filtered Events</div>
          <div className="stat">{derived.summary.filteredEvents || 0}</div>
        </div>
        <div className="stat-card">
          <div className="tiny">Runtime Errors</div>
          <div className="stat">{derived.summary.runtimeErrors || 0}</div>
        </div>
        <div className="stat-card">
          <div className="tiny">Incidents</div>
          <div className="stat">{derived.summary.criticalIncidents || 0}</div>
        </div>
        <div className="stat-card">
          <div className="tiny">Focused Filters</div>
          <div className="stat">{derived.summary.focusActive || 0}</div>
        </div>
      </div>

      <section>
        <div className="head">
          <h2>Investigation Hotspots</h2>
          <div className="stack">
            <button className="btn-secondary" onClick={() => params.selectSubview?.("events")}>Event Trace</button>
            <button className="btn-secondary" onClick={() => params.selectSubview?.("logs")}>Runtime Logs</button>
            <button className="btn-secondary" onClick={() => params.selectSubview?.("incidents")}>Incidents</button>
          </div>
        </div>
        {derived.hotspots.length === 0 ? (
          <div className="empty-state">No active hotspots for the current filters.</div>
        ) : (
          <table>
            <thead><tr><th>Type</th><th>Signal</th><th>Context</th><th>Action</th></tr></thead>
            <tbody>
              {derived.hotspots.map((item) => (
                <tr key={item.id}>
                  <td>{item.kind}</td>
                  <td>
                    <div>{item.title}</div>
                    <div className="tiny">{item.subtitle}</div>
                  </td>
                  <td className="tiny mono">{item.agent || item.vertical || "-"}</td>
                  <td>
                    <div className="stack">
                      <button className="btn-secondary" onClick={() => params.selectSubview?.(item.subview)}>Open</button>
                      {item.agent ? <button className="btn-secondary" onClick={() => params.openAgent?.(item.agent)}>Agent</button> : null}
                      {item.vertical ? <button className="btn-secondary" onClick={() => params.openWorkflowTrace?.(item.vertical)}>Workflow</button> : null}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}

function ObservabilityEventsPanel(props) {
  const params = props.params || {};
  return (
    <div className="observability-dock-panel">
      <EventsView state={params.events.state} actions={params.events.actions} />
    </div>
  );
}

function ObservabilityLogsPanel(props) {
  const params = props.params || {};
  return (
    <div className="observability-dock-panel">
      <LogsView state={params.logs.state} actions={params.logs.actions} />
    </div>
  );
}

function ObservabilityIncidentsPanel(props) {
  const params = props.params || {};
  return (
    <div className="observability-dock-panel">
      <IncidentsView state={params.incidents.state} actions={params.incidents.actions} />
    </div>
  );
}

export default function ObservabilityWorkbench({
  subview,
  setViewRoute,
  derived,
  events,
  logs,
  incidents,
  actions,
}) {
  const dockApiRef = useRef(null);
  const dockInitRef = useRef(false);
  const dockDisposerRef = useRef(null);

  const selectSubview = useCallback((next) => {
    setViewRoute("observability", next);
    dockApiRef.current?.getPanel(next)?.api?.setActive();
  }, [setViewRoute]);

  const dockComponents = useMemo(() => ({
    overview: ObservabilityOverviewPanel,
    events: ObservabilityEventsPanel,
    logs: ObservabilityLogsPanel,
    incidents: ObservabilityIncidentsPanel,
  }), []);

  const dockParams = useMemo(() => ({
    derived,
    events,
    logs,
    incidents,
    selectSubview,
    ...actions,
  }), [actions, derived, events, incidents, logs, selectSubview]);

  useEffect(() => {
    const api = dockApiRef.current;
    if (!api) return;
    ["overview", "events", "logs", "incidents"].forEach((panelID) => {
      api.getPanel(panelID)?.api?.updateParameters(dockParams);
    });
  }, [dockParams]);

  useEffect(() => {
    const api = dockApiRef.current;
    if (!api) return;
    api.getPanel(subview || "overview")?.api?.setActive();
  }, [subview]);

  useEffect(() => () => {
    dockDisposerRef.current?.dispose?.();
  }, []);

  const handleReady = useCallback((event) => {
    const api = event.api;
    dockApiRef.current = api;
    if (!dockInitRef.current) {
      dockInitRef.current = true;
      const overviewPanel = api.addPanel({
        id: "overview",
        component: "overview",
        title: "Overview",
        params: dockParams,
      });
      const eventsPanel = api.addPanel({
        id: "events",
        component: "events",
        title: "Event Trace",
        params: dockParams,
        position: {
          referencePanel: overviewPanel,
          direction: "right",
        },
      });
      api.addPanel({
        id: "logs",
        component: "logs",
        title: "Runtime Logs",
        params: dockParams,
        position: {
          referencePanel: overviewPanel,
          direction: "below",
        },
      });
      api.addPanel({
        id: "incidents",
        component: "incidents",
        title: "Incidents",
        params: dockParams,
        position: {
          referencePanel: eventsPanel,
          direction: "below",
        },
      });
    }
    dockDisposerRef.current?.dispose?.();
    dockDisposerRef.current = api.onDidActivePanelChange((panel) => {
      const next = panel?.id || "overview";
      setViewRoute("observability", next);
    });
    api.getPanel(subview || "overview")?.api?.setActive();
  }, [dockParams, setViewRoute, subview]);

  return (
    <section className="observability-workbench-shell">
      <div className="head">
        <h2>Workbench</h2>
        <div className="stack">
          <button className={subview === "overview" ? "active" : ""} onClick={() => selectSubview("overview")}>Overview</button>
          <button className={subview === "events" ? "active" : ""} onClick={() => selectSubview("events")}>Event Trace</button>
          <button className={subview === "logs" ? "active" : ""} onClick={() => selectSubview("logs")}>Runtime Logs</button>
          <button className={subview === "incidents" ? "active" : ""} onClick={() => selectSubview("incidents")}>Incidents</button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Investigation workspace for event trace, runtime logs, and incident response. {derived.summary.filteredEvents || 0} filtered events, {derived.summary.logRows || 0} log rows, {derived.summary.incidents || 0} incidents.
      </div>
      <div className="body observability-workbench-body">
        <DockviewReact
          className="observability-dockview"
          theme={themeAbyssSpaced}
          components={dockComponents}
          onReady={handleReady}
          disableFloatingGroups
          dndEdges={false}
          tabComponents={{}}
        />
      </div>
    </section>
  );
}
