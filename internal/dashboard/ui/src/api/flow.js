import { fetchJSON } from "./client.js";

export async function fetchPipelineFlow({ flowView, flowVertical, flowStart, flowEnd }) {
  const p = new URLSearchParams();
  p.set("view", flowView || "design");
  p.set("limit", "500");
  if (flowVertical && (flowView === "runtime" || flowView === "replay")) {
    p.set("vertical", flowVertical);
  }
  if (flowView === "replay") {
    if (flowStart) p.set("start", flowStart);
    if (flowEnd) p.set("end", flowEnd);
  }
  const d = await fetchJSON(`/api/pipeline/graph?${p.toString()}`);
  return {
    nodes: d.nodes || [],
    edges: d.edges || [],
    meta: d.meta || {},
    flow_events: d.flow_events || [],
  };
}
