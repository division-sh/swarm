import React from "react";
import JsonBlock from "../../components/JsonBlock.jsx";
import { fmtTime, relTime } from "../../lib/format.js";

export default function IncidentsView({ state, actions }) {
  const { incidentsData, selectedIncident, incidentArtifacts, incidentLogs, incidentsFilter, selectedIncidentCode, selectedIncidentAgent } = state;
  const { refresh, resetFilters, openLogs, openConvo, setIncidentsFilter, setSelectedIncidentCode, setSelectedIncidentAgent } = actions;

  return (
    <div className="layout-two">
      <section>
        <div className="head">
          <h2>Incident Response</h2>
          <div className="stack">
            <select
              value={String(incidentsFilter.sinceHours)}
              onChange={(e) => setIncidentsFilter((p) => ({ ...p, sinceHours: Number(e.target.value || "24") }))}
            >
              <option value="1">1h</option>
              <option value="6">6h</option>
              <option value="24">24h</option>
              <option value="72">72h</option>
              <option value="168">7d</option>
            </select>
            <select
              value={incidentsFilter.level}
              onChange={(e) => setIncidentsFilter((p) => ({ ...p, level: e.target.value }))}
            >
              <option value="warn">warn+</option>
              <option value="error">error only</option>
              <option value="info">info+</option>
            </select>
            <input
              placeholder="component"
              value={incidentsFilter.component}
              onChange={(e) => setIncidentsFilter((p) => ({ ...p, component: e.target.value }))}
            />
            <label className="tiny" style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
              <input
                type="checkbox"
                checked={incidentsFilter.mcpOnly}
                onChange={(e) => setIncidentsFilter((p) => ({ ...p, mcpOnly: e.target.checked }))}
              />
              mcp only
            </label>
            <button onClick={() => refresh().catch(() => {})}>Refresh</button>
            <button className="btn-secondary" onClick={resetFilters}>Reset</button>
          </div>
        </div>
        <div className="body scroll">
          {(incidentsData || []).length === 0 ? (
            <div className="empty-state">No incidents for selected filters</div>
          ) : (
            <table>
              <thead><tr><th>Code</th><th>Count</th><th>Last Seen</th><th>Root Cause</th><th>Agents</th></tr></thead>
              <tbody>
                {(incidentsData || []).map((it) => (
                  <tr
                    key={it.code}
                    className={selectedIncidentCode === it.code ? "selected" : ""}
                    onClick={() => setSelectedIncidentCode(it.code)}
                    style={{ cursor: "pointer" }}
                  >
                    <td className="mono">{it.code}</td>
                    <td className="mono">{it.count || 0}</td>
                    <td title={fmtTime(it.last_seen)}>{relTime(it.last_seen)}</td>
                    <td>{it.root_cause || "-"}</td>
                    <td>{Array.isArray(it.agents) ? it.agents.length : 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </section>

      <section>
        <div className="head">
          <h2>Incident Detail</h2>
          <span className="tiny mono">{selectedIncidentCode || "none"}</span>
        </div>
        <div className="body scroll">
          {!selectedIncident ? (
            <div className="empty-state">Select an incident code</div>
          ) : (
            <>
              <div className="health-card" style={{ marginBottom: 10 }}>
                <div className="health-kv"><span>Code</span><span className="mono">{selectedIncident.code}</span></div>
                <div className="health-kv"><span>Count</span><span className="mono">{selectedIncident.count || 0}</span></div>
                <div className="health-kv"><span>First</span><span>{fmtTime(selectedIncident.first_seen)}</span></div>
                <div className="health-kv"><span>Last</span><span>{fmtTime(selectedIncident.last_seen)}</span></div>
                <div className="health-kv"><span>Root Cause</span><span>{selectedIncident.root_cause || "-"}</span></div>
                <div className="health-kv"><span>Components</span><span>{(selectedIncident.components || []).join(", ") || "-"}</span></div>
                <div className="health-kv"><span>Actions</span><span>{(selectedIncident.actions || []).join(", ") || "-"}</span></div>
              </div>

              <div className="tiny" style={{ marginBottom: 6 }}>Impacted Agents</div>
              {(selectedIncident.agents || []).length === 0 ? (
                <div className="empty-state" style={{ marginBottom: 10 }}>No agent IDs found in logs</div>
              ) : (
                <div className="stack" style={{ marginBottom: 10 }}>
                  {(selectedIncident.agents || []).map((agentID) => (
                    <button
                      key={agentID}
                      className={selectedIncidentAgent === agentID ? "" : "btn-secondary"}
                      onClick={() => setSelectedIncidentAgent(agentID)}
                    >
                      {agentID}
                    </button>
                  ))}
                  {selectedIncidentAgent ? (
                    <>
                      <button className="btn-secondary" onClick={() => openLogs(selectedIncidentAgent)}>Open Logs</button>
                      <button className="btn-secondary" onClick={() => openConvo(selectedIncidentAgent)}>Open Convo</button>
                    </>
                  ) : null}
                </div>
              )}

              <div className="tiny" style={{ marginBottom: 6 }}>Session Artifacts</div>
              {incidentArtifacts.loading ? (
                <div className="empty-state">Loading artifacts...</div>
              ) : incidentArtifacts.error ? (
                <div className="health-bad" style={{ marginBottom: 10 }}>{incidentArtifacts.error}</div>
              ) : incidentArtifacts.data ? (
                <details className="holding-artifact-card" open>
                  <summary>{incidentArtifacts.data.agent_id || selectedIncidentAgent || "agent"}</summary>
                  <JsonBlock data={incidentArtifacts.data} defaultOpen={2} />
                </details>
              ) : (
                <div className="empty-state" style={{ marginBottom: 10 }}>Select an impacted agent</div>
              )}

              <div className="tiny" style={{ margin: "10px 0 6px" }}>Recent Runtime Logs</div>
              {(incidentLogs || []).length === 0 ? (
                <div className="empty-state">No runtime logs for selected code</div>
              ) : (
                <table>
                  <thead><tr><th>When</th><th>Agent</th><th>Component</th><th>Error</th></tr></thead>
                  <tbody>
                    {(incidentLogs || []).slice(0, 80).map((rl) => (
                      <tr key={rl.id} style={{ cursor: "pointer" }} onClick={() => setSelectedIncidentAgent(rl.agent_id || "")}>
                        <td title={fmtTime(rl.ts)}>{relTime(rl.ts)}</td>
                        <td className="mono">{rl.agent_id || "-"}</td>
                        <td>{rl.component || "runtime"}.{rl.action || "-"}</td>
                        <td className="tiny mono">{rl.error || rl.event_type || "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </>
          )}
        </div>
      </section>
    </div>
  );
}
