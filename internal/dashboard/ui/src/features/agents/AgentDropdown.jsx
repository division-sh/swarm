import React, { useState } from "react";
import AgentChatPanel from "./AgentChatPanel.jsx";
import AgentConversationPanel from "./AgentConversationPanel.jsx";
import AgentDirectivePanel from "./AgentDirectivePanel.jsx";
import AgentPromptPanel from "./AgentPromptPanel.jsx";
import AgentSummaryPanel from "./AgentSummaryPanel.jsx";
import AgentTurnsPanel from "./AgentTurnsPanel.jsx";
import { useAgentConsole } from "./useAgentConsole.js";

export default function AgentDropdown({ agent, addToast, onNavigate, onAction, onOpenMessage, onCopyConversation }) {
  const consoleState = useAgentConsole({ agent, addToast, onAction });
  const [section, setSection] = useState("context");

  return (
    <div className="agent-drop">
      <div className="stack" style={{ marginBottom: 8 }}>
        <button className={section === "context" ? "active" : ""} onClick={() => setSection("context")}>Context</button>
        <button className={section === "conversation" ? "active" : ""} onClick={() => setSection("conversation")}>Conversation</button>
        <button className={section === "actions" ? "active" : ""} onClick={() => setSection("actions")}>Actions</button>
      </div>
      {section === "context" ? (
        <div>
          <AgentSummaryPanel agent={agent} onNavigate={onNavigate} />
          <AgentPromptPanel agent={agent} prompt={consoleState.prompt} busy={consoleState.busy} addToast={addToast} />
          <AgentTurnsPanel turns={consoleState.conversation.turns} />
        </div>
      ) : null}
      {section === "conversation" ? (
        <AgentConversationPanel
          agentID={agent.id}
          conversation={consoleState.conversation}
          onCopyConversation={onCopyConversation}
          onOpenMessage={onOpenMessage}
        />
      ) : null}
      {section === "actions" ? (
        <div className="agent-actions">
          <AgentChatPanel chat={consoleState.chat} busy={consoleState.busy} addToast={addToast} />
          <AgentDirectivePanel
            directive={consoleState.directive}
            quickDirective={consoleState.quickDirective}
            busy={consoleState.busy}
            addToast={addToast}
          />
        </div>
      ) : null}
    </div>
  );
}
