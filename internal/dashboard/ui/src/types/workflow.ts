export type GraphResponse = {
  nodes: Record<string, any>[];
  edges: Record<string, any>[];
};

export type WorkflowFlowResponse = {
  nodes: Record<string, any>[];
  edges: Record<string, any>[];
  meta: Record<string, any>;
  flow_events: Record<string, any>[];
};
