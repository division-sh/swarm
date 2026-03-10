import { fetchJSON } from "./client.ts";
import type { HoldingResponse, HoldingVerticalDetail } from "../types/portfolio.ts";

export async function fetchHolding(): Promise<HoldingResponse> {
  const d = await fetchJSON<Partial<HoldingResponse>>("/dashboard/api/holding");
  return {
    campaigns: d.campaigns || [],
    verticals: d.verticals || [],
    agent_counts: d.agent_counts || {},
    summary: d.summary || {},
    workflow_summary: d.workflow_summary || {},
  };
}

export async function fetchHoldingVerticalDetail(verticalID: string): Promise<HoldingVerticalDetail | null> {
  const id = String(verticalID || "").trim();
  if (!id) return null;
  return fetchJSON<HoldingVerticalDetail>(`/dashboard/api/holding/vertical?id=${encodeURIComponent(id)}`);
}
