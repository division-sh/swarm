import React from "react";
import { fmtTime, formatDurationMs, relTime } from "../../lib/format.js";

export default function AgentSummaryPanel({ agent, onNavigate }) {
  const creationID = agent.creation_event?.id || "";

  return (
    <>
      <div className="agent-kv tiny"><strong>Agent</strong><span className="mono">{agent.id}</span></div>
      <div className="agent-kv tiny"><strong>Role</strong>{agent.role || "-"}</div>
      <div className="agent-kv tiny"><strong>Vertical</strong>{agent.vertical_slug || agent.vertical_id || "holding"}</div>
      <div className="agent-kv tiny"><strong>Created</strong><span title={fmtTime(agent.started_at)}>{relTime(agent.started_at)}</span></div>
      <div className="agent-kv tiny"><strong>Pending</strong>{agent.pending_events || 0}{(agent.oldest_pending_age_sec || 0) > 0 ? ` (oldest ${formatDurationMs((agent.oldest_pending_age_sec || 0) * 1000)})` : ""}</div>
      <div className="agent-kv tiny"><strong>In-Flight Turn</strong>{agent.in_flight_turn ? `yes (${formatDurationMs((agent.in_flight_seconds || 0) * 1000)})` : "no"}</div>
      <div className="agent-kv tiny"><strong>Session Lease</strong>{agent.lock_owner ? `locked until ${relTime(agent.lock_expires_at)}` : "unlocked"}</div>
      <div className="agent-kv tiny"><strong>Creation Event</strong>{agent.creation_event?.type ? `${agent.creation_event.type} ${relTime(agent.creation_event.created_at)}` : "No source event"}</div>
      <div className="stack" style={{ marginBottom: 8 }}>
        <button className="btn-secondary" disabled={!creationID} onClick={() => onNavigate("events", { eventID: creationID })}>Open Creation Event</button>
        <button className="btn-secondary" onClick={() => onNavigate("agents", { convID: agent.id, agentID: agent.id })}>Open Agent Console</button>
        <button className="btn-secondary" onClick={() => onNavigate("events", { eventsSubscriber: agent.id })}>Open Event Trace</button>
        <button className="btn-secondary" onClick={() => onNavigate("logs", { logsAgent: agent.id })}>Open Runtime Logs</button>
      </div>
    </>
  );
}
