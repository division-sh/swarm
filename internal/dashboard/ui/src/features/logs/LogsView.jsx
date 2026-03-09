import React from "react";
import JsonBlock from "../../components/JsonBlock.jsx";
import { fmtTime, relTime } from "../../lib/format.ts";

export default function LogsView({ state, actions }) {
  const { filteredLogsData, selectedLog, logsFilter, logsRuntimeErrorsOnly, logsOrder, selectedLogID } = state;
  const { refresh, clear, setLogsFilter, setLogsRuntimeErrorsOnly, setLogsOrder, setSelectedLogID } = actions;

  return (
    <div className="layout-two">
      <section>
        <div className="head">
          <h2>Logs</h2>
          <div className="obs-filterbar">
            <div className="obs-filtergroup">
              <div className="obs-filterlabel">Trace</div>
              <input placeholder="event type" value={logsFilter.type} onChange={(e) => setLogsFilter((p) => ({ ...p, type: e.target.value }))} />
              <input placeholder="agent" value={logsFilter.subscriber} onChange={(e) => setLogsFilter((p) => ({ ...p, subscriber: e.target.value }))} />
              <input placeholder="source" value={logsFilter.source} onChange={(e) => setLogsFilter((p) => ({ ...p, source: e.target.value }))} />
              <input placeholder="vertical" value={logsFilter.vertical} onChange={(e) => setLogsFilter((p) => ({ ...p, vertical: e.target.value }))} />
            </div>
            <div className="obs-filtergroup">
              <div className="obs-filterlabel">Runtime</div>
              <input placeholder="component" value={logsFilter.component} onChange={(e) => setLogsFilter((p) => ({ ...p, component: e.target.value }))} />
              <input placeholder="level" value={logsFilter.level} onChange={(e) => setLogsFilter((p) => ({ ...p, level: e.target.value }))} />
              <label className="obs-toggle">
                <input type="checkbox" checked={logsRuntimeErrorsOnly} onChange={(e) => setLogsRuntimeErrorsOnly(e.target.checked)} />
                runtime errors only
              </label>
              <button className="btn-secondary" onClick={() => setLogsOrder((o) => o === "desc" ? "asc" : "desc")}>{logsOrder === "desc" ? "Newest first" : "Oldest first"}</button>
            </div>
            <div className="obs-filtergroup obs-filtergroup-actions">
              <div className="obs-filterlabel">Actions</div>
              <button onClick={() => refresh().catch(() => {})}>Apply</button>
              <button className="btn-secondary" onClick={clear}>Reset</button>
            </div>
          </div>
        </div>
        <div className="body scroll">
          {filteredLogsData.length === 0 ? (
            <div className="empty-state">No runtime logs match the current filters</div>
          ) : filteredLogsData.map((rl) => (
            <div key={rl.id} className={`timeline-item runtime-log-item ${selectedLogID === rl.id ? "selected" : ""}`} onClick={() => setSelectedLogID(rl.id)}>
              <div className="event-type">{rl.component || "runtime"}.{rl.action || "-"}</div>
              <div className="tiny">
                <span className={`runtime-level rl-${(rl.level || "").toLowerCase()}`}>{rl.level || "info"}</span>
                {" | "}
                {rl.agent_id || "-"}
                {" | "}
                <span title={fmtTime(rl.ts)}>{relTime(rl.ts)}</span>
              </div>
              <div className="tiny mono">{rl.event_type || rl.error || "-"}</div>
            </div>
          ))}
        </div>
      </section>
      <section>
        <div className="head"><h2>Log Detail</h2></div>
        <div className="body scroll">
          {!selectedLog ? (
            <div className="empty-state">Select a log entry</div>
          ) : (
            <>
              <div className="stack" style={{ marginBottom: 10 }}>
                <button className="btn-secondary" disabled={!selectedLog.event_id} onClick={() => actions.openEvent?.(selectedLog.event_id)}>Open Event</button>
                <button className="btn-secondary" disabled={!selectedLog.agent_id} onClick={() => actions.openAgent?.(selectedLog.agent_id)}>Agent</button>
                <button className="btn-secondary" disabled={!selectedLog.vertical_id} onClick={() => actions.openWorkflowForVertical?.(selectedLog.vertical_id)}>Workflow</button>
                <button className="btn-secondary" disabled={!selectedLog.vertical_id} onClick={() => actions.openPortfolioForVertical?.(selectedLog.vertical_id)}>Portfolio</button>
                <button className="btn-secondary" disabled={!selectedLog.agent_id} onClick={() => actions.focusAgentEvents?.(selectedLog.agent_id)}>Agent Events</button>
                <button className="btn-secondary" disabled={!selectedLog.error_code && !selectedLog.component} onClick={() => actions.openIncidentFocus?.(selectedLog)}>Related Incidents</button>
              </div>
              <div className="log-detail-grid">
                <span className="log-detail-label">ID</span><span className="log-detail-value mono">{selectedLog.id}</span>
                <span className="log-detail-label">Timestamp</span><span className="log-detail-value">{fmtTime(selectedLog.ts)}</span>
                <span className="log-detail-label">Level</span><span><span className={`runtime-level rl-${(selectedLog.level || "").toLowerCase()}`}>{selectedLog.level || "info"}</span></span>
                <span className="log-detail-label">Component</span><span className="log-detail-value">{selectedLog.component || "-"}</span>
                <span className="log-detail-label">Action</span><span className="log-detail-value">{selectedLog.action || "-"}</span>
                <span className="log-detail-label">Agent</span><span className="log-detail-value mono">{selectedLog.agent_id || "-"}</span>
                <span className="log-detail-label">Event ID</span><span className="log-detail-value mono">{selectedLog.event_id || "-"}</span>
                <span className="log-detail-label">Event Type</span><span className="log-detail-value">{selectedLog.event_type || "-"}</span>
                <span className="log-detail-label">Error Code</span><span className="log-detail-value mono">{selectedLog.error_code || "-"}</span>
                <span className="log-detail-label">Vertical</span><span className="log-detail-value mono">{selectedLog.vertical_id || "-"}</span>
                <span className="log-detail-label">Campaign</span><span className="log-detail-value mono">{selectedLog.campaign_id || "-"}</span>
                <span className="log-detail-label">Scan</span><span className="log-detail-value mono">{selectedLog.scan_id || "-"}</span>
                <span className="log-detail-label">Session</span><span className="log-detail-value mono">{selectedLog.session_id || "-"}</span>
                <span className="log-detail-label">Duration</span><span className="log-detail-value mono">{selectedLog.duration_us != null ? `${(selectedLog.duration_us / 1000).toFixed(1)} ms` : "-"}</span>
              </div>
              {selectedLog.error ? (
                <>
                  <div className="log-detail-label" style={{ marginTop: 10 }}>Error</div>
                  <pre className="log-error-text">{selectedLog.error}</pre>
                </>
              ) : null}
              {selectedLog.detail ? (
                <>
                  <div className="log-detail-label" style={{ marginTop: 10 }}>Detail</div>
                  <JsonBlock data={selectedLog.detail} defaultOpen={2} />
                </>
              ) : null}
            </>
          )}
        </div>
      </section>
    </div>
  );
}
