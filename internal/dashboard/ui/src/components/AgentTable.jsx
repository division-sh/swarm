import React from "react";
import StatusDot from "./StatusDot.jsx";

export default function AgentTable({ agents, selectedAgentID, onSelectAgent, renderDropdown, onNavigateTask, formatDurationMs }) {
  if (!agents || agents.length === 0) {
    return <div className="empty-state">No agents in this group.</div>;
  }
  const stateOrder = { stuck: 0, running: 1, idle: 2, terminated: 3 };
  const sorted = [...agents].sort((a, b) => (stateOrder[a.state] ?? 9) - (stateOrder[b.state] ?? 9) || (a.id || "").localeCompare(b.id || ""));
  return (
    <table>
      <thead>
        <tr>
          <th>Agent</th>
          <th>State</th>
          <th>Task</th>
          <th>Turns</th>
          <th>Tokens 24h</th>
          <th>Pending</th>
          <th>Last Tool</th>
        </tr>
      </thead>
      <tbody>
        {sorted.map((a) => {
          const pct = a.turn_limit > 0 ? Math.min(100, Math.round((a.turn_count / a.turn_limit) * 100)) : 0;
          const fillClass = pct >= 95 ? "turnfill bad" : pct >= 85 ? "turnfill warn" : "turnfill";
          const tool = a.last_tool && a.last_tool.name ? `${a.last_tool.name}${a.last_tool.ok === false ? " (fail)" : ""}` : "-";
          const active = selectedAgentID === a.id;
          return (
            <React.Fragment key={a.id}>
              <tr className={`agent-row ${active ? "active" : ""}`} onClick={() => onSelectAgent(active ? "" : a.id)}>
                <td>
                  <div><strong>{a.id}</strong></div>
                  <div className="tiny">{a.role || "-"}</div>
                </td>
                <td>
                  <span className={`badge b-${a.state}`}><StatusDot state={a.state} />{a.state}</span>
                  {a.stuck_reason ? <div className="tiny" style={{ color: "#f87171" }}>{a.stuck_reason}</div> : null}
                </td>
                <td>{a.current_task_id ? <span className="copy-id mono" style={{ cursor: "pointer", color: "var(--info)" }} onClick={(e) => { e.stopPropagation(); if (onNavigateTask) onNavigateTask(a.current_task_id); }} title="Open in Tasks tab">{a.current_task_id.slice(0, 10)}</span> : <span className="mono">-</span>}</td>
                <td>
                  <div className="turnbar"><div className={fillClass} style={{ width: `${pct}%` }} /></div>
                  <div className="tiny">{a.turn_count}/{a.turn_limit} <span className="mono">({a.turns_24h || 0} in 24h)</span></div>
                </td>
                <td className="mono">{(a.total_tokens_24h || 0).toLocaleString()}</td>
                <td>
                  <div>{a.pending_events || 0}</div>
                  {a.in_flight_turn ? (
                    <div className="tiny mono" title="Agent currently holds an active session lease while processing a turn.">
                      in-turn {formatDurationMs((a.in_flight_seconds || 0) * 1000)}
                    </div>
                  ) : null}
                  {!a.in_flight_turn && (a.oldest_pending_age_sec || 0) > 0 ? (
                    <div className="tiny mono" title="Age of oldest pending delivery for this agent.">
                      oldest {formatDurationMs((a.oldest_pending_age_sec || 0) * 1000)}
                    </div>
                  ) : null}
                </td>
                <td>
                  <div>{tool}</div>
                  <div className="tiny">{(a.last_tool && a.last_tool.result) || "-"}</div>
                </td>
              </tr>
              {active ? (
                <tr>
                  <td className="agent-drop-cell" colSpan={7}>
                    {renderDropdown ? renderDropdown(a) : null}
                  </td>
                </tr>
              ) : null}
            </React.Fragment>
          );
        })}
      </tbody>
    </table>
  );
}
