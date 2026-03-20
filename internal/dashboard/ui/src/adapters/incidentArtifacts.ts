import { adaptConversationDetail } from "./conversations.ts";
import type { IncidentArtifacts } from "../types/runtime.ts";
import type { GenericConversationDetail } from "../types/server.ts";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? { ...(value as Record<string, unknown>) } : {};
}

function trimString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

export function adaptIncidentArtifacts(detail: GenericConversationDetail): IncidentArtifacts {
  const runtimeState = asRecord(detail.runtime_state);
  const normalized = adaptConversationDetail(detail);
  const out: IncidentArtifacts = {
    agent_id: trimString(detail.agent_id),
    scope_key: trimString(detail.scope_key),
    scope: trimString(detail.scope),
    runtime_mode: trimString(detail.runtime_mode),
    status: trimString(detail.status),
    updated_at: trimString(detail.updated_at),
    turn_count: detail.turn_count,
    summary: trimString(detail.summary) || trimString(runtimeState.summary),
    runtime_state: runtimeState,
    messages: normalized.messages,
    turns: normalized.turns,
  };
  if (Array.isArray(runtimeState.artifacts)) {
    out.artifacts = runtimeState.artifacts;
  }
  return out;
}
