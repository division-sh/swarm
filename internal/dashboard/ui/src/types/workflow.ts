export type GraphNodeRecord = {
  id: string;
  kind?: string;
  label?: string;
  status?: string;
  group?: string;
  role?: string;
  stage?: string;
  agent_id?: string;
  vertical_id?: string;
  vertical_slug?: string;
  system_prompt?: string;
  metadata?: Record<string, unknown>;
  data?: Record<string, unknown>;
  [key: string]: unknown;
};

export type GraphEdgeRecord = {
  id?: string;
  from: string;
  to: string;
  kind?: string;
  status?: string;
  label?: string;
  timer_ids?: string[];
  transition_ids?: string[];
  event_types?: string[];
  guards?: string[];
  actions?: string[];
  metadata?: Record<string, unknown>;
  [key: string]: unknown;
};

export type GraphResponse = {
  nodes: GraphNodeRecord[];
  edges: GraphEdgeRecord[];
};

export type WorkflowFlowMeta = {
  node_count?: number;
  edge_count?: number;
  workflow_name?: string;
  workflow_version?: string;
  platform_version?: string;
  stages?: string[];
  rubrics?: string[];
  event_stage_map?: Record<string, string[]>;
  [key: string]: unknown;
};

export type FlowEventRecord = {
  event_id: string;
  id?: string;
  type?: string;
  event_type?: string;
  kind?: string;
  stage?: string;
  status?: string;
  source_agent?: string;
  source_node?: string;
  target_nodes?: string[];
  entity_id?: string;
  scope?: string;
  vertical_id?: string;
  vertical_slug?: string;
  subscriber?: string;
  created_at?: string;
  payload?: unknown;
  [key: string]: unknown;
};

export type WorkflowFlowResponse = {
  nodes: GraphNodeRecord[];
  edges: GraphEdgeRecord[];
  meta: WorkflowFlowMeta;
  flow_events: FlowEventRecord[];
};
