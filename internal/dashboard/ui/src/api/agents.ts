import { adaptAgents } from "../adapters/agents.ts";
import { fetchGenericAgents } from "./resources/agents.ts";
import type { AgentsResponse } from "../types/core.ts";

export async function fetchAgents(): Promise<AgentsResponse> {
  const generic = await fetchGenericAgents();
  return adaptAgents(generic);
}
