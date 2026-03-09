export type AgentRecord = Record<string, any>;

export type AgentsResponse = {
  agents: AgentRecord[];
  states: Record<string, unknown>;
};

export type TasksResponse = {
  tasks: Record<string, any>[];
  weekly_budget: Record<string, any>;
};

export type MailboxResponse = {
  summary: Record<string, any>;
  items: Record<string, any>[];
};

export type DigestResponse = Record<string, any> | null;
export type OverviewResponse = Record<string, any>;
export type HealthResponse = Record<string, any>;
export type TargetRecord = Record<string, any>;
