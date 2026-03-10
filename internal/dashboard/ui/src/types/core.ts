export type LooseRecord = Record<string, unknown>;

export type ControlResult = {
  ok?: boolean;
  message?: string;
  error?: string;
  [key: string]: unknown;
};

export type AgentRecord = {
  id: string;
  agent_id?: string;
  role?: string;
  family?: string;
  state?: string;
  status?: string;
  vertical_id?: string;
  vertical_slug?: string;
  lease_owner?: string;
  lease_expires_at?: string;
  pending_count?: number;
  turn_pressure?: number;
  tokens_24h?: number;
  breaker_ratio?: number;
  tool_failures_24h?: number;
  stuck_reason?: string;
  last_tool_outcome?: string;
  last_runtime_ts?: string;
  session_id?: string;
  runtime_mode?: string;
  prompt_override_exists?: boolean;
  prompt_override_updated_at?: string;
  creation_event_id?: string;
  geography?: string;
  pending_events?: number;
  oldest_pending_age_sec?: number;
  in_flight_turn?: boolean;
  in_flight_seconds?: number;
  near_breaker?: boolean;
  failures_24h?: number;
  dead_letters_24h?: number;
  lock_owner?: string;
  last_tool?: {
    ok?: boolean;
    name?: string;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

export type AgentsResponse = {
  agents: AgentRecord[];
  states: Record<string, unknown>;
};

export type TaskRecord = {
  id: string;
  description?: string;
  category?: string;
  status?: string;
  outcome?: string;
  priority?: string;
  deadline?: string;
  created_at?: string;
  updated_at?: string;
  vertical_id?: string;
  vertical_slug?: string;
  requesting_agent?: string;
  agent_id?: string;
  assignee?: string;
  follow_up_needed?: boolean;
  [key: string]: unknown;
};

export type WeeklyBudget = {
  cap?: number;
  spent?: number;
  remaining?: number;
  approved_this_week?: number;
  max_tasks_per_week?: number;
  reset_day?: string;
  week_start_utc?: string;
  [key: string]: unknown;
};

export type TasksResponse = {
  tasks: TaskRecord[];
  weekly_budget?: WeeklyBudget;
};

export type TaskStats = {
  completed?: number;
  rejected?: number;
  open?: number;
  assigned?: number;
  pending_review?: number;
  approved?: number;
  deferred?: number;
  expired?: number;
  [key: string]: unknown;
};

export type MailboxSummary = {
  pending?: number;
  approved?: number;
  rejected?: number;
  deferred?: number;
  decided?: number;
  [key: string]: unknown;
};

export type MailboxItem = {
  id: string;
  type?: string;
  summary?: string;
  status?: string;
  priority?: string;
  created_at?: string;
  updated_at?: string;
  from_agent?: string;
  vertical_id?: string;
  vertical_slug?: string;
  notes?: string;
  [key: string]: unknown;
};

export type MailboxResponse = {
  summary?: MailboxSummary;
  items: MailboxItem[];
};

export type DigestResponse = (LooseRecord & {
  generated_at?: string;
  summary?: string;
  top?: LooseRecord[];
}) | null;

export type OverviewResponse = LooseRecord & {
  summary?: LooseRecord;
  generated_at?: string;
};

export type HealthVerticalRecord = {
  id?: string;
  slug?: string;
  name?: string;
  health_status?: string;
  deploy_status?: string;
  [key: string]: unknown;
};

export type HealthResponse = LooseRecord & {
  auth?: {
    auth_errors_1h?: number;
    auth_errors_24h?: number;
    [key: string]: unknown;
  };
  workflow_audit?: {
    warnings?: string[];
    [key: string]: unknown;
  };
  vertical_health?: HealthVerticalRecord[];
  spend?: LooseRecord;
  runtime?: LooseRecord;
  contracts?: LooseRecord;
};

export type TargetRecord = {
  agent_id: string;
  role?: string;
  status?: string;
  vertical_slug?: string;
  [key: string]: unknown;
};
