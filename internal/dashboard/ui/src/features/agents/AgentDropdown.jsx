import React from "react";
import AgentChatPanel from "./AgentChatPanel.jsx";
import AgentDirectivePanel from "./AgentDirectivePanel.jsx";
import AgentPromptPanel from "./AgentPromptPanel.jsx";
import AgentSummaryPanel from "./AgentSummaryPanel.jsx";
import AgentTurnsPanel from "./AgentTurnsPanel.jsx";
import { useAgentConsole } from "./useAgentConsole.js";

export default function AgentDropdown({ agent, addToast, onNavigate, onAction }) {
  const consoleState = useAgentConsole({ agent, addToast, onAction });

  return (
    <div className="agent-drop">
      <div className="agent-drop-grid">
        <div>
          <AgentSummaryPanel agent={agent} onNavigate={onNavigate} />
          <AgentPromptPanel agent={agent} prompt={consoleState.prompt} busy={consoleState.busy} addToast={addToast} />
          <AgentTurnsPanel turns={consoleState.turns} />
        </div>
        <div className="agent-actions">
          <AgentChatPanel chat={consoleState.chat} busy={consoleState.busy} addToast={addToast} />
          <AgentDirectivePanel
            directive={consoleState.directive}
            quickDirective={consoleState.quickDirective}
            busy={consoleState.busy}
            addToast={addToast}
          />
        </div>
      </div>
    </div>
  );
}
