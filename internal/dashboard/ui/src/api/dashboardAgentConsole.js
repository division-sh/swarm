import { deleteJSON, fetchJSON, postJSON, putJSON } from "./client.js";

export async function fetchAgentPrompt(agentID) {
  return fetchJSON(`/api/agents/${encodeURIComponent(agentID)}/prompt`);
}

export async function fetchAgentPromptDiff(agentID) {
  return fetchJSON(`/api/agents/${encodeURIComponent(agentID)}/prompt/diff`);
}

export async function saveAgentPromptOverride(agentID, prompt, notes) {
  return putJSON(`/api/agents/${encodeURIComponent(agentID)}/prompt`, {
    prompt,
    source: "dashboard",
    notes: notes || undefined,
  });
}

export async function revertAgentPromptOverride(agentID) {
  return deleteJSON(`/api/agents/${encodeURIComponent(agentID)}/prompt`);
}

export async function sendAgentChat(agentID, mode, message) {
  return postJSON(`/api/chat/${encodeURIComponent(agentID)}`, { mode, message });
}

export async function sendAgentDirective(agentID, message) {
  return postJSON("/dashboard/api/control/directive", { agent_id: agentID, message });
}

export async function restartAgentRuntime(agentID) {
  return postJSON("/dashboard/api/control/agents/restart", { agent_id: agentID });
}

export async function replayAgentRuntime(agentID) {
  return postJSON("/dashboard/api/control/agents/replay", { agent_id: agentID });
}
