import { fetchJSON, postJSON } from "./client.ts";
import { adaptFunnel } from "../adapters/funnel.ts";
import { adaptTrace } from "../adapters/trace.ts";
import { fetchEvents } from "./dashboardRuntime.ts";
import { fetchGenericInstances } from "./resources/instances.ts";
import { fetchHolding, fetchHoldingVerticalDetail } from "./holding.ts";
import type { FunnelResponse, ShardDetailRecord, ShardScanRecord, TraceRecord, VerticalRecord } from "../types/portfolio.ts";

function isGenericEndpointUnavailable(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return error.message === "HTTP 404" || error.message === "HTTP 405" || error.message === "HTTP 501";
}

export async function fetchFunnel(): Promise<FunnelResponse> {
  try {
    const instances = await fetchGenericInstances();
    return adaptFunnel(instances);
  } catch (err) {
    if (!isGenericEndpointUnavailable(err)) throw err;
  }
  const d = await fetchJSON<Partial<FunnelResponse>>("/dashboard/api/funnel");
  return {
    throughput: d.throughput || {},
    stuck: d.stuck || [],
  };
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
  try {
    const events = await fetchEvents({ vertical: value });
    return adaptTrace(events);
  } catch (err) {
    if (!isGenericEndpointUnavailable(err)) throw err;
  }
  const d = await fetchJSON<{ trace?: TraceRecord[] }>(`/dashboard/api/verticals/${encodeURIComponent(value)}/trace`);
  return d.trace || [];
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
