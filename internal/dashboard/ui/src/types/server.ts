export type GenericHealthResponse = {
  ok?: boolean;
  timestamp?: string;
  checks?: Record<string, unknown>;
};

export type GenericConversationSummary = {
  agent_id: string;
  scope_key?: string;
  scope?: string;
  runtime_mode?: string;
  status?: string;
  turn_count?: number;
  summary?: string;
  updated_at?: string;
  metadata?: Record<string, unknown>;
};

export type GenericConversationDetail = GenericConversationSummary & {
  messages?: unknown[];
  runtime_state?: Record<string, unknown>;
};

export type GenericMailboxItem = {
  id: string;
  event_id?: string;
  entity_id?: string;
  from_agent?: string;
  type?: string;
  priority?: string;
  status?: string;
  notified?: boolean;
  summary?: string;
  context?: unknown;
  timeout_at?: string;
  decision?: string;
  decision_notes?: string;
  created_at?: string;
  updated_at?: string;
};

export type GenericAgent = {
  id: string;
  type?: string;
  role?: string;
  mode?: string;
  status?: string;
  state?: string;
  entity_id?: string;
  parent_agent_id?: string;
  coordinator_id?: string;
  hired_by?: string;
  template_version?: string;
  budget_envelope?: number;
  subscriptions?: string[];
  permissions?: string[];
  pending_events?: number;
  oldest_pending_age_sec?: number;
  in_flight_turn?: boolean;
  in_flight_seconds?: number;
  lock_owner?: string;
  lock_expires_at?: string;
  failures_24h?: number;
  dead_letters_24h?: number;
  turn_count?: number;
  turn_limit?: number;
  turns_24h?: number;
  total_tokens_24h?: number;
  near_breaker?: boolean;
  current_task_id?: string;
  last_tool?: Record<string, unknown>;
  started_at?: string;
};
