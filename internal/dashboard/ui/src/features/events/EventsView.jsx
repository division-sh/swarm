import React from "react";
import JsonBlock from "../../components/JsonBlock.jsx";
import { fmtTime, relTime } from "../../lib/format.js";

export default function EventsView({ state, actions }) {
  const { filteredEvents, filteredRuntimeLogs, eventDetail, eventsFilter, eventsIncludeRuntime, eventsRuntimeErrorsOnly, selectedEventID } = state;
  const { refresh, clear } = actions;
  const { setEventsFilter, setEventsIncludeRuntime, setEventsRuntimeErrorsOnly, setSelectedEventID } = actions;
  const selectedEvent = eventDetail && eventDetail.event ? eventDetail.event : null;

  return (
    <section>
      <div className="head">
        <h2>Event Flow</h2>
        <div className="obs-filterbar">
          <div className="obs-filtergroup">
            <div className="obs-filterlabel">Trace</div>
            <input placeholder="type prefix" value={eventsFilter.type} onChange={(e) => setEventsFilter((p) => ({ ...p, type: e.target.value }))} />
            <input placeholder="source" value={eventsFilter.source} onChange={(e) => setEventsFilter((p) => ({ ...p, source: e.target.value }))} />
            <input placeholder="agent subscriber" value={eventsFilter.subscriber} onChange={(e) => setEventsFilter((p) => ({ ...p, subscriber: e.target.value }))} />
            <input placeholder="vertical slug/id" value={eventsFilter.vertical} onChange={(e) => setEventsFilter((p) => ({ ...p, vertical: e.target.value }))} />
          </div>
          <div className="obs-filtergroup">
            <div className="obs-filterlabel">Runtime</div>
            <input placeholder="component" value={eventsFilter.component} onChange={(e) => setEventsFilter((p) => ({ ...p, component: e.target.value }))} />
            <input placeholder="level" value={eventsFilter.level} onChange={(e) => setEventsFilter((p) => ({ ...p, level: e.target.value }))} />
            <label className="obs-toggle">
              <input type="checkbox" checked={eventsIncludeRuntime} onChange={(e) => setEventsIncludeRuntime(e.target.checked)} />
              include runtime logs
            </label>
            <label className="obs-toggle">
              <input type="checkbox" checked={eventsRuntimeErrorsOnly} onChange={(e) => setEventsRuntimeErrorsOnly(e.target.checked)} />
              runtime errors only
            </label>
          </div>
          <div className="obs-filtergroup obs-filtergroup-actions">
            <div className="obs-filterlabel">Actions</div>
            <button onClick={() => refresh().catch(() => {})}>Apply</button>
            <button className="btn-secondary" onClick={clear}>Reset</button>
          </div>
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
          <div className="tiny">{selectedEvent ? `${selectedEvent.type} by ${selectedEvent.source_agent} at ${fmtTime(selectedEvent.created_at)}` : "Select event"}</div>
          {selectedEvent ? (
            <div className="stack" style={{ margin: "8px 0" }}>
              <button className="btn-secondary" onClick={() => actions.openLogsForAgent?.(selectedEvent.source_agent || "")}>Open Logs</button>
              <button className="btn-secondary" onClick={() => actions.focusEventType?.(selectedEvent.type || "")}>Same Type</button>
              <button className="btn-secondary" onClick={() => actions.focusEventVertical?.(selectedEvent.vertical_slug || selectedEvent.vertical_id || "")}>Same Vertical</button>
            </div>
          ) : null}
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
