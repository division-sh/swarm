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
  current_stage?: string;
  workflow_current_stage?: string;
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
  workflow_current_stage?: string;
  stage_entered_at?: string;
  active_timer_count?: number;
  revision_count?: number;
  kill_reason?: string;
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
  timers?: number;
  revisions?: number;
  stale?: number;
  [key: string]: unknown;
};

export type HoldingResponse = {
  campaigns: CampaignRecord[];
  verticals: VerticalRecord[];
  agent_counts: Record<string, number>;
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
  team?: unknown[];
  artifacts?: unknown[];
  workflow_state?: Record<string, unknown>;
  [key: string]: unknown;
};
