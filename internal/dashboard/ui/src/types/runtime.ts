export type EventFilter = {
  type?: string;
  source?: string;
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

export type EventRecord = Record<string, any>;
export type RuntimeLogRecord = Record<string, any>;
export type IncidentRecord = Record<string, any>;
export type ConversationDetail = {
  messages: Record<string, any>[];
  turns: Record<string, any>[];
};
