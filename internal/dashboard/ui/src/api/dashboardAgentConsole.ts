import { deleteJSON, fetchJSON, postJSON, putJSON } from "./client.ts";

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
  return postJSON(`/api/agents/${encodeURIComponent(agentID)}/actions/directive`, { message });
}

export async function restartAgentRuntime(agentID) {
  return postJSON(`/api/agents/${encodeURIComponent(agentID)}/actions/restart`, {});
}

export async function replayAgentRuntime(agentID) {
  return postJSON(`/api/agents/${encodeURIComponent(agentID)}/actions/replay`, {});
}
