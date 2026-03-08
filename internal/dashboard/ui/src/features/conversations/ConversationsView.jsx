import React from "react";
import ChatMessages from "../../components/ChatMessages.jsx";

export default function ConversationsView({ state, actions }) {
  const { conversations, conversationDetail, selectedConv } = state;
  const { setSelectedConv, openConversation, copyConversation, openMessage } = actions;

  return (
    <section>
      <div className="head">
        <h2>Conversations</h2>
        <div className="stack">
          <select value={selectedConv} onChange={(e) => setSelectedConv(e.target.value)}>
            {(conversations || []).map((c) => <option key={c.agent_id} value={c.agent_id}>{c.agent_id} ({c.role || "-"})</option>)}
          </select>
          <button onClick={() => openConversation(selectedConv).catch(() => {})}>Open</button>
          <button className="btn-secondary" onClick={() => copyConversation(selectedConv, conversationDetail.messages)}>Copy All</button>
        </div>
      </div>
      <div className="row body">
        <div style={{ display: "flex", flexDirection: "column" }}>
          <div className="tiny">Messages ({(conversationDetail.messages || []).length})</div>
          <ChatMessages messages={conversationDetail.messages} agentID={selectedConv} turns={conversationDetail.turns} onOpenMessage={openMessage} />
        </div>
        <div>
          <div className="tiny">Turns / Tool Calls</div>
          <div className="body scroll" style={{ maxHeight: "70vh", padding: 0 }}>
            <table>
              <thead><tr><th>#</th><th>OK</th><th>Latency</th><th>Tool Calls</th><th>Result</th></tr></thead>
              <tbody>
                {(conversationDetail.turns || []).length === 0 ? (
                  <tr><td colSpan={5} className="empty-state">No turns</td></tr>
                ) : (conversationDetail.turns || []).map((t, i) => {
                  const text = t.assistant_text || t.tool_result || "-";
                  return (
                    <tr key={`${t.turn_index || i}-${i}`} style={{ cursor: "pointer" }} onClick={() => openMessage({ role: `Turn ${t.turn_index != null ? t.turn_index : i}`, text })}>
                      <td>{t.turn_index}</td>
                      <td>{t.parse_ok ? "yes" : "no"}</td>
                      <td>{t.latency_ms}</td>
                      <td>{((t.tool_calls || []).map((x) => x.name || "-")).join(", ") || "-"}</td>
                      <td className="tiny" style={{ maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{text}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </section>
  );
}
