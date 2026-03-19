import { fetchJSON } from "../client.ts";
import type { GenericAgent } from "../../types/server.ts";

export async function fetchGenericAgents(): Promise<GenericAgent[]> {
  const d = await fetchJSON<{ agents?: GenericAgent[] }>("/api/agents");
  return d.agents || [];
}

export async function fetchGenericAgentDetail(agentID: string): Promise<GenericAgent> {
  return fetchJSON<GenericAgent>(`/api/agents/${encodeURIComponent(agentID)}`);
}
