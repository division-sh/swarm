export type FunnelThroughput = {
  total?: number;
  queued?: number;
  processing?: number;
  completed?: number;
  [key: string]: unknown;
};

export type FunnelStuckRecord = {
  id?: string;
  slug?: string;
  name?: string;
  stage?: string;
  current_state?: string;
  workflow_current_state?: string;
  idle_hours?: number;
  [key: string]: unknown;
};

export type FunnelResponse = {
  throughput: FunnelThroughput;
  stuck: FunnelStuckRecord[];
};

export type CampaignRecord = {
  id: string;
  name?: string;
  slug?: string;
  status?: string;
  [key: string]: unknown;
};

export type VerticalRecord = {
  id: string;
  slug?: string;
  name?: string;
  geography?: string;
  stage?: string;
  workflow_current_state?: string;
  stage_entered_at?: string;
  active_timer_count?: number;
  revision_count?: number;
  kill_reason?: string;
  updated_at?: string;
  workflow_version?: string;
  composite_score?: unknown;
  [key: string]: unknown;
};

export type HoldingSummary = {
  total?: number;
  in_pipeline?: number;
  killed?: number;
  [key: string]: unknown;
};

export type HoldingWorkflowSummary = {
  drift?: number;
  active_timers?: number;
  revisioned?: number;
  stage_entered_set?: number;
  timers?: number;
  revisions?: number;
  stale?: number;
  [key: string]: unknown;
};

export type HoldingAgentCount = {
  total?: number;
  active?: number;
};

export type HoldingResponse = {
  campaigns: CampaignRecord[];
  verticals: VerticalRecord[];
  agent_counts: Record<string, HoldingAgentCount>;
  summary: HoldingSummary;
  workflow_summary: HoldingWorkflowSummary;
};

export type ShardScanRecord = {
  scan_id: string;
  progress?: number;
  shards_failed?: number;
  shards_stuck?: number;
  status?: string;
  created_at?: string;
  [key: string]: unknown;
};

export type ShardDetailRecord = {
  shard_id?: string;
  status?: string;
  error?: string;
  vertical_slug?: string;
  [key: string]: unknown;
};

export type TraceRecord = {
  id?: string;
  stage?: string;
  kind?: string;
  event_type?: string;
  created_at?: string;
  [key: string]: unknown;
};

export type HoldingVerticalDetail = {
  vertical?: VerticalRecord & { [key: string]: unknown };
  events?: unknown[];
  mailbox?: unknown[];
  agents?: unknown[];
  team?: unknown[];
  artifacts?: unknown[];
  workflow_state?: Record<string, unknown>;
  workflow_state_error?: string;
  spend?: Record<string, unknown>;
  [key: string]: unknown;
};
