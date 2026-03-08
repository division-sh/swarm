import { fetchJSON, postJSON } from "./client.js";
import { fetchHolding, fetchHoldingVerticalDetail } from "./holding.js";

export async function fetchFunnel() {
  const d = await fetchJSON("/dashboard/api/funnel");
  return {
    throughput: d.throughput || {},
    stuck: d.stuck || [],
  };
}

export async function fetchShardScans() {
  const d = await fetchJSON("/dashboard/api/pipeline/shards?limit=30");
  return d.scans || [];
}

export async function fetchShardScanDetail(scanID) {
  const id = String(scanID || "").trim();
  if (!id) return [];
  const d = await fetchJSON(`/dashboard/api/pipeline/shards/${encodeURIComponent(id)}`);
  return d.shards || [];
}

export async function fetchTrace(vertical) {
  const value = String(vertical || "").trim();
  if (!value) return [];
  const d = await fetchJSON(`/dashboard/api/verticals/${encodeURIComponent(value)}/trace`);
  return d.trace || [];
}

export async function fetchVerticals() {
  const d = await fetchJSON("/api/verticals");
  return d.verticals || [];
}

export async function shardActionRequest(scanID, shardID, action) {
  const sid = String(scanID || "").trim();
  const hid = String(shardID || "").trim();
  if (!sid || !hid) return null;
  await postJSON(`/api/pipeline/shards/${encodeURIComponent(hid)}/${encodeURIComponent(action)}`, {});
  return { scanID: sid, shardID: hid, action };
}

export {
  fetchHolding,
  fetchHoldingVerticalDetail,
};
