import React from "react";

export default function AgentChatPanel({ chat, busy, addToast }) {
  return (
    <>
      <div className="tiny">Direct discussion</div>
      <textarea value={chat.message} onChange={(e) => chat.setMessage(e.target.value)} placeholder="Ask this agent directly..." />
      <div className="stack" style={{ marginTop: 6 }}>
        <select value={chat.mode} onChange={(e) => chat.setMode(e.target.value)}>
          <option value="live">live</option>
          <option value="async">async</option>
        </select>
        <button
          disabled={!!busy || !chat.message.trim()}
          onClick={() => chat.send().catch((err) => addToast(err.message, "error"))}
        >
          {busy === "chat" ? "Sending…" : "Send Chat"}
        </button>
      </div>
    </>
  );
}
