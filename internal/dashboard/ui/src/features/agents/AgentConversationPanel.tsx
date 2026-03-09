import React from "react";
import ChatMessages from "../../components/ChatMessages.tsx";
import { fmtTime, relTime } from "../../lib/format.ts";

export default function AgentConversationPanel({ agentID, conversation, onCopyConversation, onOpenMessage }) {
  const messages = conversation?.messages || [];
  const turns = conversation?.turns || [];

  return (
    <div style={{ marginTop: 10 }}>
      <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
        <div className="tiny">Conversation History</div>
        <button className="btn-secondary" onClick={() => onCopyConversation(agentID, messages)}>Copy All</button>
      </div>
      {messages.length === 0 ? (
        <div className="empty-state">No messages recorded</div>
      ) : (
        <ChatMessages messages={messages} agentID={agentID} turns={turns} onOpenMessage={onOpenMessage} fmtTime={fmtTime} relTime={relTime} />
      )}
      <div className="tiny" style={{ marginTop: 10, marginBottom: 6 }}>Turns / Tool Calls</div>
      <div className="body scroll" style={{ maxHeight: "42vh", padding: 0 }}>
        <table>
          <thead><tr><th>#</th><th>OK</th><th>Latency</th><th>Tool Calls</th><th>Result</th></tr></thead>
          <tbody>
            {turns.length === 0 ? (
              <tr><td colSpan={5} className="empty-state">No turns</td></tr>
            ) : turns.map((turn, index) => {
              const text = turn.assistant_text || turn.tool_result || "-";
              return (
                <tr key={`${turn.turn_index || index}-${index}`} style={{ cursor: "pointer" }} onClick={() => onOpenMessage({ role: `Turn ${turn.turn_index != null ? turn.turn_index : index}`, text })}>
                  <td>{turn.turn_index}</td>
                  <td>{turn.parse_ok ? "yes" : "no"}</td>
                  <td>{turn.latency_ms}</td>
                  <td>{((turn.tool_calls || []).map((item) => item.name || "-")).join(", ") || "-"}</td>
                  <td className="tiny" style={{ maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{text}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
