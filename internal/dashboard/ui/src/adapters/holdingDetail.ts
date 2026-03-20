import type { EventRecord } from "../types/runtime.ts";
import type { HoldingVerticalDetail, VerticalRecord } from "../types/portfolio.ts";
import type { GenericAgent, GenericInstance, GenericMailboxItem } from "../types/server.ts";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function asNumber(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function activeTimers(instance: GenericInstance): Array<NonNullable<GenericInstance["timer_state"]>[number]> {
  return (instance.timer_state || []).filter((timer) => !timer?.cancelled);
}

function deriveStage(instance: GenericInstance): string {
  const metadata = asRecord(instance.metadata);
  return asString(metadata.stage) || asString(metadata.workflow_stage) || asString(instance.current_state);
}

function buildVertical(instance: GenericInstance): VerticalRecord & { [key: string]: unknown } {
  const metadata = asRecord(instance.metadata);
  return {
    id: asString(instance.instance_id),
    slug: asString(metadata.slug) || undefined,
    name: asString(metadata.name) || undefined,
    geography: asString(metadata.geography) || undefined,
    stage: deriveStage(instance) || undefined,
    workflow_current_state: asString(instance.current_state) || undefined,
    stage_entered_at: asString(instance.entered_stage_at) || undefined,
    active_timer_count: activeTimers(instance).length,
    revision_count: asNumber(metadata.revision_count) || undefined,
    kill_reason: asString(metadata.kill_reason) || undefined,
    workflow_version: asString(instance.workflow_version) || undefined,
    composite_score: metadata.composite_score,
    created_at: asString(instance.created_at) || undefined,
    updated_at: asString(instance.updated_at) || undefined,
    template_version: asString(instance.workflow_version) || undefined,
    ...metadata,
  };
}

function buildWorkflowState(instance: GenericInstance): Record<string, unknown> {
  return {
    instance_id: instance.instance_id,
    workflow_name: instance.workflow_name,
    workflow_version: instance.workflow_version,
    current_state: instance.current_state,
    entered_stage_at: instance.entered_stage_at,
    transition_history: instance.transition_history || [],
    transition_count: Array.isArray(instance.transition_history) ? instance.transition_history.length : 0,
    timer_state: instance.timer_state || [],
    active_timer_count: activeTimers(instance).length,
    state_buckets: instance.state_buckets || {},
    metadata: instance.metadata || {},
    created_at: instance.created_at,
    updated_at: instance.updated_at,
  };
}

function buildTeam(agents: GenericAgent[]): Array<Record<string, unknown>> {
  return agents.map((agent) => ({
    ...agent,
    last_active_at: agent.lock_expires_at || agent.started_at,
  }));
}

function sortByCreatedDesc<T extends { created_at?: string }>(rows: T[]): T[] {
  return [...rows].sort((a, b) => String(b.created_at || "").localeCompare(String(a.created_at || "")));
}

export function adaptHoldingDetail(input: {
  instance: GenericInstance;
  agents?: GenericAgent[];
  events?: EventRecord[];
  mailbox?: GenericMailboxItem[];
}): HoldingVerticalDetail {
  const instance = input.instance;
  return {
    vertical: buildVertical(instance),
    workflow_state: buildWorkflowState(instance),
    workflow_state_error: "",
    agents: buildTeam(input.agents || []),
    team: buildTeam(input.agents || []),
    events: sortByCreatedDesc(input.events || []).slice(0, 20),
    mailbox: sortByCreatedDesc(input.mailbox || []).slice(0, 20),
    artifacts: [],
    spend: {},
  };
}
