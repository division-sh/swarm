import { fetchJSON } from "./client.ts";
import { adaptAgents } from "../adapters/agents.ts";
import { fetchGenericAgents } from "./resources/agents.ts";
import type { AgentsResponse } from "../types/core.ts";

function isGenericEndpointUnavailable(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return error.message === "HTTP 404" || error.message === "HTTP 405" || error.message === "HTTP 501";
}

export async function fetchAgents(): Promise<AgentsResponse> {
  try {
    const generic = await fetchGenericAgents();
    return adaptAgents(generic);
  } catch (err) {
    if (!isGenericEndpointUnavailable(err)) throw err;
  }
  const d = await fetchJSON<Partial<AgentsResponse>>("/dashboard/api/agents");
  return { agents: d.agents || [], states: d.states || {} };
}
