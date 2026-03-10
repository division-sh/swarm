import React from "react";
import { relTime } from "../../lib/format.ts";

export default function HoldingTeamPanel({ agents }) {
  return (
    <div className="holding-detail-section">
      <div className="tiny" style={{ marginBottom: 6 }}>Team</div>
      {(agents || []).length === 0 ? (
        <div className="empty-state">No agents linked yet</div>
      ) : (
        <table>
          <thead><tr><th>Agent</th><th>Role</th><th>Status</th><th>Mode</th><th>Task</th><th>Last Active</th></tr></thead>
          <tbody>
            {(agents || []).map((agent) => (
              <tr key={agent.id}>
                <td className="mono">{agent.id}</td>
                <td>{agent.role || "-"}</td>
                <td>{agent.status || "-"}</td>
                <td>{agent.mode || "-"}</td>
                <td className="mono">{agent.current_task_id || "-"}</td>
                <td>{relTime(agent.last_active_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
