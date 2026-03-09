import { fetchJSON, postJSON } from "./client.ts";
import { fetchHolding, fetchHoldingVerticalDetail } from "./holding.ts";
import type { FunnelResponse } from "../types/portfolio.ts";

export async function fetchFunnel(): Promise<FunnelResponse> {
  const d = await fetchJSON<Record<string, any>>("/dashboard/api/funnel");
  return {
    throughput: d.throughput || {},
    stuck: d.stuck || [],
  };
}

export async function fetchShardScans(): Promise<Record<string, any>[]> {
  const d = await fetchJSON<Record<string, any>>("/dashboard/api/pipeline/shards?limit=30");
  return d.scans || [];
}

export async function fetchShardScanDetail(scanID?: string): Promise<Record<string, any>[]> {
  const id = String(scanID || "").trim();
  if (!id) return [];
  const d = await fetchJSON<Record<string, any>>(`/dashboard/api/pipeline/shards/${encodeURIComponent(id)}`);
  return d.shards || [];
}

export async function fetchTrace(vertical?: string): Promise<Record<string, any>[]> {
  const value = String(vertical || "").trim();
  if (!value) return [];
  const d = await fetchJSON<Record<string, any>>(`/dashboard/api/verticals/${encodeURIComponent(value)}/trace`);
  return d.trace || [];
}

export async function fetchVerticals(): Promise<Record<string, any>[]> {
  const d = await fetchJSON<Record<string, any>>("/api/verticals");
  return d.verticals || [];
}

export async function shardActionRequest(scanID?: string, shardID?: string, action?: string): Promise<{ scanID: string; shardID: string; action?: string } | null> {
  const sid = String(scanID || "").trim();
  const hid = String(shardID || "").trim();
  if (!sid || !hid) return null;
  await postJSON(`/api/pipeline/shards/${encodeURIComponent(hid)}/${encodeURIComponent(action || "")}`, {});
  return { scanID: sid, shardID: hid, action };
}

export {
  fetchHolding,
  fetchHoldingVerticalDetail,
};
