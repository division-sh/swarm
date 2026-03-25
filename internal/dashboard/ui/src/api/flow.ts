import { fetchJSON } from "./client.ts";
import type { WorkflowFlowResponse } from "../types/workflow.ts";

export async function fetchPipelineFlow({
  flowView,
  flowVertical,
  flowStart,
  flowEnd,
}: {
  flowView?: string;
  flowVertical?: string;
  flowStart?: string;
  flowEnd?: string;
}): Promise<WorkflowFlowResponse> {
  const p = new URLSearchParams();
  p.set("view", flowView || "design");
  p.set("limit", "500");
  if (flowVertical && (flowView === "runtime" || flowView === "replay")) {
    p.set("entity_id", flowVertical);
  }
  if (flowView === "replay") {
    if (flowStart) p.set("start", flowStart);
    if (flowEnd) p.set("end", flowEnd);
  }
  const d = await fetchJSON<Partial<WorkflowFlowResponse>>(`/api/pipeline/graph?${p.toString()}`);
  return {
    nodes: d.nodes || [],
    edges: d.edges || [],
    meta: d.meta || {},
    flow_events: d.flow_events || [],
  };
}
