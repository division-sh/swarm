export type EventFilter = {
  type?: string;
  source?: string;
  entity_id?: string;
  vertical?: string;
  component?: string;
  level?: string;
  subscriber?: string;
};

export type LogFilter = EventFilter;

export type IncidentFilter = {
  sinceHours?: number;
  mcpOnly?: boolean;
  level?: string;
  component?: string;
};

export type EventDeliveryRecord = {
  agent_id?: string;
  status?: string;
  error?: string;
  retry_count?: number;
  [key: string]: unknown;
};

export type EventRecord = {
  id: string;
  event_id?: string;
  type?: string;
  created_at?: string;
  source_agent?: string;
  entity_id?: string;
  scope?: string;
  vertical_id?: string;
  vertical_slug?: string;
  component?: string;
  payload?: unknown;
  deliveries?: EventDeliveryRecord[];
  error_count?: number;
  dead_count?: number;
  pending_count?: number;
  [key: string]: unknown;
};

export type RuntimeLogRecord = {
  id: string;
  ts?: string;
  level?: string;
  component?: string;
  action?: string;
  event_type?: string;
  error?: string;
  error_code?: string;
  agent_id?: string;
  entity_id?: string;
  vertical_id?: string;
  source?: string;
  message?: string;
  [key: string]: unknown;
};

export type IncidentRecord = {
  code: string;
  count?: number;
  root_cause?: string;
  component?: string;
  level?: string;
  agents?: string[];
  first_seen?: string;
  last_seen?: string;
  [key: string]: unknown;
};

export type ConversationMessage = {
  id?: string;
  role?: string;
  text?: string;
  content?: Array<{ text?: string; type?: string; [key: string]: unknown }>;
  created_at?: string;
  [key: string]: unknown;
};

export type ConversationTurn = {
  id?: string;
  stage?: string;
  status?: string;
  created_at?: string;
  tool_calls?: unknown[];
  [key: string]: unknown;
};

export type ConversationRecord = {
  agent_id?: string;
  id?: string;
  vertical_slug?: string;
  updated_at?: string;
  [key: string]: unknown;
};

export type ConversationDetail = {
  messages: ConversationMessage[];
  turns: ConversationTurn[];
};

export type EventDetail = EventRecord & {
  deliveries?: EventDeliveryRecord[];
};

export type IncidentArtifacts = {
  error?: string;
  artifacts?: unknown[];
  [key: string]: unknown;
};
