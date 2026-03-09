import React from "react";

function extractToolCallNames(message, turn) {
  const names = [];
  if (turn && Array.isArray(turn.tool_calls)) {
    for (const tc of turn.tool_calls) {
      const name = (tc && tc.name ? String(tc.name) : "").trim();
      if (name) names.push(name);
    }
  }
  const content = message && message.content;
  if (Array.isArray(content)) {
    for (const item of content) {
      if (!item || typeof item !== "object") continue;
      if ((item.type || "").trim() !== "tool_use") continue;
      const name = (item.name ? String(item.name) : "").trim();
      if (name) names.push(name);
    }
  }
  return [...new Set(names)];
}

function chatMsgSummary(message, text, turn) {
  const role = (message && message.role ? String(message.role) : "").trim();
  const toolNames = extractToolCallNames(message, turn);
  if (role === "assistant" && toolNames.length > 0) return `TOOLS: ${toolNames.join(", ")}`;
  const first = text.split("\n").find((l) => l.trim().length > 0) || "";
  if (role === "assistant" && !first) return "NO ASSISTANT TEXT";
  return first.length > 120 ? `${first.slice(0, 120)}\u2026` : first;
}

export default function ChatMessages({ messages, onOpenMessage, agentID, turns, fmtTime, relTime }) {
  if (!messages || messages.length === 0) {
    return <div className="empty-state">No messages recorded</div>;
  }

  const turnTimeMap = new Map();
  const turnMap = new Map();
  if (turns) {
    for (const t of turns) {
      if (t.turn_index != null && t.created_at) {
        turnTimeMap.set(t.turn_index, t.created_at);
        turnMap.set(t.turn_index, t);
      }
    }
  }

  let turnIdx = -1;
  return (
    <div className="chat-messages">
      {messages.map((m, i) => {
        const role = m.role || "unknown";
        if (role === "user") turnIdx++;
        const label = agentID ? (role === "assistant" ? agentID : role === "user" ? "orchestrator" : role) : role;
        const text = typeof m.content === "string"
          ? m.content
          : Array.isArray(m.content)
            ? m.content.map((c) => c.text || c.type || "").join("\n")
            : JSON.stringify(m.content, null, 2);
        const turn = turnMap.get(turnIdx) || null;
        const summary = chatMsgSummary(m, text, turn);
        const isNoOp = summary === "NO ASSISTANT TEXT";
        const ts = turnTimeMap.get(turnIdx);
        return (
          <details key={i} className={`chat-msg chat-${role}${isNoOp ? " chat-noop" : ""}`}>
            <summary className="chat-summary">
              <span className="chat-role">{label}</span>
              <span className="chat-summary-text">{summary}</span>
              {ts ? <span className="chat-time" title={fmtTime(ts)}>{relTime(ts)}</span> : null}
            </summary>
            <pre className="chat-content" onClick={() => onOpenMessage && onOpenMessage({ role, text })}>{text}</pre>
          </details>
        );
      })}
    </div>
  );
}
