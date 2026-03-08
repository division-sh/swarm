import React from "react";
import JsonBlock from "../../components/JsonBlock.jsx";
import { fmtTime, relTime } from "../../lib/format.js";

export default function EventsView({ state, actions }) {
  const { filteredEvents, filteredRuntimeLogs, eventDetail, eventsFilter, eventsIncludeRuntime, eventsRuntimeErrorsOnly, selectedEventID } = state;
  const { refresh, clear } = actions;
  const { setEventsFilter, setEventsIncludeRuntime, setEventsRuntimeErrorsOnly, setSelectedEventID } = actions;

  return (
    <section>
      <div className="head">
        <h2>Event Flow</h2>
        <div className="stack">
          <input placeholder="type (prefix*)" value={eventsFilter.type} onChange={(e) => setEventsFilter((p) => ({ ...p, type: e.target.value }))} />
          <input placeholder="source" value={eventsFilter.source} onChange={(e) => setEventsFilter((p) => ({ ...p, source: e.target.value }))} />
          <input placeholder="subscriber (agent)" value={eventsFilter.subscriber} onChange={(e) => setEventsFilter((p) => ({ ...p, subscriber: e.target.value }))} />
          <input placeholder="vertical slug/id" value={eventsFilter.vertical} onChange={(e) => setEventsFilter((p) => ({ ...p, vertical: e.target.value }))} />
          <input placeholder="component (runtime)" value={eventsFilter.component} onChange={(e) => setEventsFilter((p) => ({ ...p, component: e.target.value }))} />
          <input placeholder="level (debug|info|warn|error)" value={eventsFilter.level} onChange={(e) => setEventsFilter((p) => ({ ...p, level: e.target.value }))} />
          <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
            <input type="checkbox" checked={eventsIncludeRuntime} onChange={(e) => setEventsIncludeRuntime(e.target.checked)} />
            include runtime logs
          </label>
          <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
            <input type="checkbox" checked={eventsRuntimeErrorsOnly} onChange={(e) => setEventsRuntimeErrorsOnly(e.target.checked)} />
            runtime errors only
          </label>
          <button onClick={() => refresh().catch(() => {})}>Filter</button>
          <button className="btn-secondary" onClick={clear}>Clear</button>
        </div>
      </div>
      <div className="row3 body">
        <div className="body scroll" style={{ maxHeight: "70vh", padding: 0 }}>
          {filteredEvents.length === 0 ? (
            <div className="empty-state">No events match the current filters</div>
          ) : filteredEvents.map((e) => (
            <div key={e.id} className={`timeline-item ${selectedEventID === e.id ? "selected" : ""}`} onClick={() => setSelectedEventID(e.id)}>
              <div className="event-type">{e.type}</div>
              <div className="tiny">{e.source_agent} | {e.vertical_slug || "-"} | <span title={fmtTime(e.created_at)}>{relTime(e.created_at)}</span></div>
              <div className="tiny">delivered {e.delivery_count} | processed {e.processed_count} | errors {e.error_count} | pending {e.pending_count}</div>
            </div>
          ))}
        </div>
        <div>
          <div className="tiny" style={{ marginBottom: 6 }}>Selected Event</div>
          <div className="tiny">{eventDetail && eventDetail.event ? `${eventDetail.event.type} by ${eventDetail.event.source_agent} at ${fmtTime(eventDetail.event.created_at)}` : "Select event"}</div>
          <JsonBlock data={(eventDetail && eventDetail.payload) || {}} defaultOpen={2} />
          <div className="tiny" style={{ margin: "8px 0 4px" }}>Deliveries</div>
          <div className="body scroll" style={{ maxHeight: 240, padding: 0 }}>
            <table>
              <thead><tr><th>Agent</th><th>Status</th><th>ms</th><th>Error</th></tr></thead>
              <tbody>
                {((eventDetail && eventDetail.deliveries) || []).length === 0 ? (
                  <tr><td colSpan={4} className="empty-state">No deliveries</td></tr>
                ) : ((eventDetail && eventDetail.deliveries) || []).map((d) => (
                  <tr key={`${d.agent_id}-${d.status}-${d.retry_count || 0}`}>
                    <td>{d.agent_id}</td>
                    <td>{d.status}</td>
                    <td>{d.processing_ms || 0}</td>
                    <td>{d.error || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div>
          <div className="tiny" style={{ marginBottom: 6 }}>Runtime Logs</div>
          <div className="body scroll" style={{ maxHeight: "70vh", padding: 0 }}>
            {filteredRuntimeLogs.length === 0 ? (
              <div className="empty-state">No runtime logs match the current filters</div>
            ) : filteredRuntimeLogs.map((rl) => (
              <div key={`${rl.id}-${rl.ts || ""}`} className="timeline-item runtime-log-item">
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
        </div>
      </div>
    </section>
  );
}
