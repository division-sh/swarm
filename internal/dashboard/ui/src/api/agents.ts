import { fetchJSON } from "./client.ts";
import type { AgentsResponse } from "../types/core.ts";

export async function fetchAgents(): Promise<AgentsResponse> {
  const d = await fetchJSON<Record<string, any>>("/dashboard/api/agents");
  return { agents: d.agents || [], states: d.states || {} };
}
