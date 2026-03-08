import { fetchJSON } from "./client.js";

export async function fetchAgents() {
  const d = await fetchJSON("/dashboard/api/agents");
  return { agents: d.agents || [], states: d.states || {} };
}
