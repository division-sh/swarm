import React from "react";
import { fmtTime, formatDurationMs, relTime } from "../../lib/format.ts";
import StatusDot from "../../components/StatusDot.jsx";

function statusLabel(agent) {
  if (agent.stuck_reason) return agent.stuck_reason;
  if (agent.in_flight_turn) return `in-turn ${formatDurationMs((agent.in_flight_seconds || 0) * 1000)}`;
  if ((agent.oldest_pending_age_sec || 0) > 0) return `oldest pending ${formatDurationMs((agent.oldest_pending_age_sec || 0) * 1000)}`;
  return "healthy enough";
}

export default function AgentSummaryPanel({ agent, promptState, onNavigate, onOpenControl, onNavigateTask, onSelectSection }) {
  const creationID = agent.creation_event?.id || "";
  const currentTaskID = agent.current_task_id || "";
  const lastToolLabel = agent.last_tool?.name ? `${agent.last_tool.name}${agent.last_tool.ok === false ? " (fail)" : ""}` : "-";
  const promptOverrideAge = promptState?.override?.updated_at || promptState?.override?.created_at || "";

  return (
    <>
      <div className="health-card" style={{ marginBottom: 10 }}>
        <div className="stack" style={{ justifyContent: "space-between", marginBottom: 8 }}>
          <div>
            <div className="tiny">Agent Health</div>
            <div className="mono" style={{ fontSize: 15, fontWeight: 700 }}>{agent.id}</div>
            <div className="tiny">{agent.role || "-"} | {agent.vertical_slug || agent.vertical_id || "holding"}</div>
          </div>
          <div className={`badge b-${agent.state || "idle"}`}><StatusDot state={agent.state} />{agent.state || "idle"}</div>
        </div>
        <div className="health-kv"><span>Attention</span><span>{statusLabel(agent)}</span></div>
        <div className="health-kv"><span>Pending</span><span className={(agent.pending_events || 0) > 0 ? "health-warn mono" : "mono"}>{agent.pending_events || 0}</span></div>
        <div className="health-kv"><span>Failures 24h</span><span className={(agent.failures_24h || 0) > 0 ? "health-bad mono" : "mono"}>{agent.failures_24h || 0}</span></div>
        <div className="health-kv"><span>Dead Letters 24h</span><span className={(agent.dead_letters_24h || 0) > 0 ? "health-bad mono" : "mono"}>{agent.dead_letters_24h || 0}</span></div>
        <div className="health-kv"><span>Turns</span><span className={agent.near_breaker ? "health-warn mono" : "mono"}>{agent.turn_count || 0}/{agent.turn_limit || 0}</span></div>
        <div className="health-kv"><span>Tokens 24h</span><span className="mono">{(agent.total_tokens_24h || 0).toLocaleString()}</span></div>
        <div className="health-kv"><span>Last Tool</span><span className={agent.last_tool?.ok === false ? "health-bad" : ""}>{lastToolLabel}</span></div>
        <div className="health-kv"><span>Runtime</span><span className="mono">{agent.runtime_mode || "-"} / {agent.session_id || "-"}</span></div>
        <div className="health-kv"><span>Lease</span><span>{agent.lock_owner ? `locked by ${agent.lock_owner} until ${relTime(agent.lock_expires_at)}` : "unlocked"}</span></div>
        <div className="health-kv"><span>Prompt</span><span>{promptState?.has_override ? `override ${promptOverrideAge ? relTime(promptOverrideAge) : ""}`.trim() : "template"}</span></div>
        <div className="health-kv"><span>Created</span><span title={fmtTime(agent.started_at)}>{relTime(agent.started_at)}</span></div>
        <div className="health-kv"><span>Creation Event</span><span>{agent.creation_event?.type ? `${agent.creation_event.type} ${relTime(agent.creation_event.created_at)}` : "No source event"}</span></div>
      </div>
      <div className="stack" style={{ marginBottom: 8 }}>
        <button className="btn-secondary" onClick={() => onSelectSection("actions")}>Open Actions</button>
        <button className="btn-secondary" onClick={() => onOpenControl(agent.id)}>Open Control</button>
        <button className="btn-secondary" disabled={!currentTaskID} onClick={() => onNavigateTask(currentTaskID)}>Open Task</button>
        <button className="btn-secondary" onClick={() => onNavigate("agents", { convID: agent.id, agentID: agent.id })}>Open Conversation</button>
        <button className="btn-secondary" onClick={() => onNavigate("events", { eventsSubscriber: agent.id })}>Open Event Trace</button>
        <button className="btn-secondary" onClick={() => onNavigate("logs", { logsAgent: agent.id })}>Open Runtime Logs</button>
        <button className="btn-secondary" disabled={!creationID} onClick={() => onNavigate("events", { eventID: creationID })}>Open Creation Event</button>
      </div>
    </>
  );
}
