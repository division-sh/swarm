import { fetchJSON, postJSON } from "./client.ts";
import { adaptFunnel } from "../adapters/funnel.ts";
import { adaptTrace } from "../adapters/trace.ts";
import { fetchEvents } from "./dashboardRuntime.ts";
import { fetchGenericInstances } from "./resources/instances.ts";
import { fetchHolding, fetchHoldingVerticalDetail } from "./holding.ts";
import type { FunnelResponse, ShardDetailRecord, ShardScanRecord, TraceRecord, VerticalRecord } from "../types/portfolio.ts";

export async function fetchFunnel(): Promise<FunnelResponse> {
  const instances = await fetchGenericInstances();
  return adaptFunnel(instances);
}

export async function fetchShardScans(): Promise<ShardScanRecord[]> {
  const d = await fetchJSON<{ scans?: ShardScanRecord[] }>("/dashboard/api/pipeline/shards?limit=30");
  return d.scans || [];
}

export async function fetchShardScanDetail(scanID?: string): Promise<ShardDetailRecord[]> {
  const id = String(scanID || "").trim();
  if (!id) return [];
  const d = await fetchJSON<{ shards?: ShardDetailRecord[] }>(`/dashboard/api/pipeline/shards/${encodeURIComponent(id)}`);
  return d.shards || [];
}

export async function fetchTrace(vertical?: string): Promise<TraceRecord[]> {
  const value = String(vertical || "").trim();
  if (!value) return [];
  const events = await fetchEvents({ vertical: value });
  return adaptTrace(events);
}

export async function fetchVerticals(): Promise<VerticalRecord[]> {
  const holding = await fetchHolding();
  return holding.verticals || [];
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
