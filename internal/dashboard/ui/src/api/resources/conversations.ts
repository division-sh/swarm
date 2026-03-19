import { fetchJSON } from "../client.ts";
import type { GenericConversationDetail, GenericConversationSummary } from "../../types/server.ts";

export async function fetchGenericConversations(limit = 100): Promise<GenericConversationSummary[]> {
  const d = await fetchJSON<{ conversations?: GenericConversationSummary[] }>(`/api/conversations?limit=${encodeURIComponent(limit)}`);
  return d.conversations || [];
}

export async function fetchGenericConversationDetail(agentID: string): Promise<GenericConversationDetail> {
  return fetchJSON<GenericConversationDetail>(`/api/conversations/${encodeURIComponent(agentID)}`);
}
