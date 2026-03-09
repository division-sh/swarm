import { fetchJSON } from "./client.ts";
import { fetchPipelineFlow } from "./flow.ts";
import type { GraphResponse, WorkflowFlowResponse } from "../types/workflow.ts";

export async function fetchGraph({
  graphMode,
  graphVertical,
}: {
  graphMode?: string;
  graphVertical?: string;
}): Promise<GraphResponse> {
  const p = new URLSearchParams();
  p.set("mode", graphMode || "holding");
  if ((graphMode || "holding") === "opco" && graphVertical) {
    p.set("vertical", graphVertical);
  }
  const d = await fetchJSON<Record<string, any>>(`/api/graph?${p.toString()}`);
  return (d || { nodes: [], edges: [] }) as GraphResponse;
}

export async function fetchWorkflowFlow(params: {
  flowView?: string;
  flowVertical?: string;
  flowStart?: string;
  flowEnd?: string;
}): Promise<WorkflowFlowResponse> {
  return fetchPipelineFlow(params);
}
