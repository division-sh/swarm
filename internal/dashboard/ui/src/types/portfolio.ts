export type FunnelResponse = {
  throughput: Record<string, any>;
  stuck: Record<string, any>[];
};

export type HoldingResponse = {
  campaigns: Record<string, any>[];
  verticals: Record<string, any>[];
  agent_counts: Record<string, any>;
  summary: Record<string, any>;
  workflow_summary: Record<string, any>;
};
