import React from "react";

export default function AgentTurnsPanel({ turns }) {
  return (
    <>
      <div className="tiny">Recent turns</div>
      <div className="body scroll" style={{ maxHeight: 180, padding: 0 }}>
        <table>
          <thead><tr><th>#</th><th>OK</th><th>Latency</th><th>Result</th></tr></thead>
          <tbody>
            {turns.length === 0 ? (
              <tr><td colSpan={4} className="empty-state">No turns recorded</td></tr>
            ) : turns.slice(0, 8).map((turn, index) => (
              <tr key={`${turn.turn_index || index}-${index}`}>
                <td>{turn.turn_index}</td>
                <td>{turn.parse_ok ? "yes" : "no"}</td>
                <td>{turn.latency_ms}</td>
                <td className="tiny">{turn.tool_result || turn.assistant_text || "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
