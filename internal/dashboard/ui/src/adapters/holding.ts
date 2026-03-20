import type { GenericAgent, GenericInstance } from "../types/server.ts";
import type { HoldingResponse, VerticalRecord } from "../types/portfolio.ts";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function asString(value: unknown): string {
  return String(value ?? "").trim();
}

function asNumber(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function activeTimerCount(instance: GenericInstance): number {
  return (instance.timer_state || []).filter((timer) => !timer.cancelled).length;
}

function deriveStage(instance: GenericInstance): string {
  const metadata = asRecord(instance.metadata);
  return asString(metadata.stage) || asString(metadata.workflow_stage) || asString(instance.current_state);
}

function deriveVertical(instance: GenericInstance): VerticalRecord | null {
  const metadata = asRecord(instance.metadata);
  const slug = asString(metadata.slug);
  const name = asString(metadata.name);
  if (!slug && !name && !asString(instance.instance_id)) return null;
  return {
    id: asString(instance.instance_id),
    slug: slug || undefined,
    name: name || undefined,
    geography: asString(metadata.geography) || undefined,
    stage: deriveStage(instance) || undefined,
    workflow_current_state: asString(instance.current_state) || undefined,
    stage_entered_at: asString(instance.entered_stage_at) || undefined,
    active_timer_count: activeTimerCount(instance),
    revision_count: asNumber(metadata.revision_count) || undefined,
    kill_reason: asString(metadata.kill_reason) || undefined,
    updated_at: asString(instance.updated_at) || undefined,
    workflow_version: asString(instance.workflow_version) || undefined,
    composite_score: metadata.composite_score,
  };
}

function buildAgentCounts(agents: GenericAgent[]): Record<string, { total: number; active: number }> {
  const out: Record<string, { total: number; active: number }> = {};
  for (const agent of agents) {
    const key = asString(agent.entity_id);
    if (!key) continue;
    const bucket = out[key] || { total: 0, active: 0 };
    bucket.total += 1;
    if (asString(agent.state) !== "terminated") bucket.active += 1;
    out[key] = bucket;
  }
  return out;
}

export function adaptHolding(instances: GenericInstance[], agents: GenericAgent[] = []): HoldingResponse {
  const verticals = instances
    .map(deriveVertical)
    .filter((item): item is VerticalRecord => Boolean(item));

  return {
    campaigns: [],
    verticals,
    agent_counts: buildAgentCounts(agents),
    summary: {
      total: verticals.length,
      in_pipeline: verticals.filter((vertical) => asString(vertical.stage) !== "killed").length,
      killed: verticals.filter((vertical) => asString(vertical.stage) === "killed").length,
    },
    workflow_summary: {
      drift: verticals.filter((vertical) => vertical.workflow_current_state && vertical.workflow_current_state !== vertical.stage).length,
      active_timers: verticals.filter((vertical) => asNumber(vertical.active_timer_count) > 0).length,
      revisioned: verticals.filter((vertical) => asNumber(vertical.revision_count) > 0).length,
      stage_entered_set: verticals.filter((vertical) => Boolean(vertical.stage_entered_at)).length,
      timers: verticals.filter((vertical) => asNumber(vertical.active_timer_count) > 0).length,
      revisions: verticals.filter((vertical) => asNumber(vertical.revision_count) > 0).length,
    },
  };
}
