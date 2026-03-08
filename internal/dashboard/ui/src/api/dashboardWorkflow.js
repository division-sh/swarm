import { fetchJSON } from "./client.js";
import { fetchPipelineFlow } from "./flow.js";

export async function fetchGraph({ graphMode, graphVertical }) {
  const p = new URLSearchParams();
  p.set("mode", graphMode || "holding");
  if ((graphMode || "holding") === "opco" && graphVertical) {
    p.set("vertical", graphVertical);
  }
  const d = await fetchJSON(`/api/graph?${p.toString()}`);
  return d || { nodes: [], edges: [] };
}

export async function fetchWorkflowFlow(params) {
  return fetchPipelineFlow(params);
}
