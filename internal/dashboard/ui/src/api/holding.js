import { fetchJSON } from "./client.js";

export async function fetchHolding() {
  const d = await fetchJSON("/dashboard/api/holding");
  return {
    campaigns: d.campaigns || [],
    verticals: d.verticals || [],
    agent_counts: d.agent_counts || {},
    summary: d.summary || {},
    workflow_summary: d.workflow_summary || {},
  };
}

export async function fetchHoldingVerticalDetail(verticalID) {
  const id = String(verticalID || "").trim();
  if (!id) return null;
  return fetchJSON(`/dashboard/api/holding/vertical?id=${encodeURIComponent(id)}`);
}
