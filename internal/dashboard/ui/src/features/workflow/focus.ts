function trim(value: unknown) {
  return typeof value === "string" ? value.trim() : "";
}

type FocusInput = {
  flow: { state?: Record<string, any> };
  graph: { state?: Record<string, any> };
  subview: string;
};

export function deriveWorkflowFocus({ flow, graph, subview }: FocusInput) {
  const flowView = flow?.state?.flowView || "design";
  const graphMode = graph?.state?.graphMode || "holding";
  const vertical = trim(flow?.state?.flowVertical || (graphMode === "opco" ? graph?.state?.graphVertical : ""));
  const stage = trim(flow?.state?.flowStage || "all");
  const rubric = trim(flow?.state?.flowRubric || "all");
  const selectedFlowNodeID = trim(flow?.state?.selectedFlowNodeID);
  const selectedGraphNodeID = trim(graph?.state?.selectedGraphNodeID);

  const chips = [
    subview === "flow" ? `trace:${flowView}` : `topology:${graphMode}`,
    vertical ? `vertical:${vertical}` : "",
    stage && stage !== "all" ? `stage:${stage}` : "",
    rubric && rubric !== "all" ? `rubric:${rubric}` : "",
    selectedFlowNodeID ? `flow-node:${selectedFlowNodeID}` : "",
    selectedGraphNodeID ? `graph-node:${selectedGraphNodeID}` : "",
  ].filter(Boolean);

  return {
    flowView,
    graphMode,
    vertical,
    stage,
    rubric,
    selectedFlowNodeID,
    selectedGraphNodeID,
    chips,
  };
}
