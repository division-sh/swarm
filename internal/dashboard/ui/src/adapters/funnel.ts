import type { GenericInstance } from "../types/server.ts";
import type { FunnelResponse, FunnelStuckRecord } from "../types/portfolio.ts";
import { adaptHolding } from "./holding.ts";

function asString(value: unknown): string {
  return String(value ?? "").trim();
}

function ageHours(timestamp: string | undefined): number {
  if (!timestamp) return 0;
  const value = new Date(timestamp).getTime();
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.floor((Date.now() - value) / 3600000));
}

const APPROVED_STAGES = new Set(["approved", "full_speccing", "building", "pre_launch", "launched", "operating", "expanding"]);

export function adaptFunnel(instances: GenericInstance[]): FunnelResponse {
  const holding = adaptHolding(instances);
  const verticals = holding.verticals || [];
  const recentCutoff = Date.now() - (14 * 24 * 3600 * 1000);
  const recent = verticals.filter((vertical) => {
    const at = new Date(vertical.updated_at || vertical.stage_entered_at || 0).getTime();
    return Number.isFinite(at) && at >= recentCutoff;
  });
  const progressedRecent = recent.filter((vertical) => {
    const stage = asString(vertical.stage);
    return stage !== "" && stage !== "discovered";
  });
  const stuck: FunnelStuckRecord[] = verticals
    .filter((vertical) => (
      (vertical.workflow_current_state && vertical.workflow_current_state !== vertical.stage)
      || Number(vertical.active_timer_count || 0) > 0
    ))
    .map((vertical) => ({
      id: vertical.id,
      slug: vertical.slug,
      name: vertical.name,
      stage: vertical.stage,
      current_state: vertical.stage,
      workflow_current_state: vertical.workflow_current_state,
      idle_hours: ageHours(vertical.stage_entered_at),
    }));

  return {
    throughput: {
      discoveries_14d: recent.length,
      scoring_completion_rate: recent.length > 0 ? progressedRecent.length / recent.length : 0,
      specs_approved_or_live: verticals.filter((vertical) => APPROVED_STAGES.has(asString(vertical.stage))).length,
      specs_killed_total: verticals.filter((vertical) => asString(vertical.stage) === "killed").length,
    },
    stuck,
  };
}
