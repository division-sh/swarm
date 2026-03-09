import { useMemo } from "react";

const STALE_STAGE_HOURS = 72;

function ageHours(timestamp) {
  if (!timestamp) return 0;
  const value = new Date(timestamp).getTime();
  if (!Number.isFinite(value) || Number.isNaN(value)) return 0;
  return Math.max(0, Math.floor((Date.now() - value) / 3600000));
}

function sortVerticals(rows) {
  return [...rows].sort((a, b) => (
    Number(b.active_timer_count || 0) - Number(a.active_timer_count || 0)
      || Number(b.revision_count || 0) - Number(a.revision_count || 0)
      || ageHours(b.stage_entered_at) - ageHours(a.stage_entered_at)
      || `${a.slug || a.name || a.id || ""}`.localeCompare(`${b.slug || b.name || b.id || ""}`)
  ));
}

export function usePortfolioDerivedState({ holdingData, funnel, shardScans }) {
  return useMemo(() => {
    const verticals = Array.isArray(holdingData?.verticals) ? holdingData.verticals : [];
    const stuck = Array.isArray(funnel?.stuck) ? funnel.stuck : [];
    const scans = Array.isArray(shardScans) ? shardScans : [];

    const driftedVerticals = sortVerticals(
      verticals.filter((vertical) => vertical.workflow_current_stage && vertical.workflow_current_stage !== vertical.stage),
    );
    const timerHeavyVerticals = sortVerticals(
      verticals.filter((vertical) => Number(vertical.active_timer_count || 0) > 0),
    );
    const revisionedVerticals = sortVerticals(
      verticals.filter((vertical) => Number(vertical.revision_count || 0) > 0),
    );
    const staleVerticals = sortVerticals(
      verticals.filter((vertical) => vertical.stage !== "killed" && ageHours(vertical.stage_entered_at) >= STALE_STAGE_HOURS),
    ).map((vertical) => ({ ...vertical, stage_age_hours: ageHours(vertical.stage_entered_at) }));
    const humanNeededVerticals = sortVerticals(
      verticals.filter((vertical) => vertical.stage === "ready_for_review"),
    );
    const retryShardScans = [...scans]
      .filter((scan) => Number(scan.shards_failed || 0) > 0 || Number(scan.shards_stuck || 0) > 0)
      .sort((a, b) => (
        (Number(b.shards_failed || 0) + Number(b.shards_stuck || 0)) - (Number(a.shards_failed || 0) + Number(a.shards_stuck || 0))
          || Number(b.progress || 0) - Number(a.progress || 0)
          || `${a.scan_id || ""}`.localeCompare(`${b.scan_id || ""}`)
      ));

    return {
      summary: {
        drift: driftedVerticals.length,
        timers: timerHeavyVerticals.length,
        revisions: revisionedVerticals.length,
        stale: staleVerticals.length,
        humanNeeded: humanNeededVerticals.length,
        stuck: stuck.length,
        retryScans: retryShardScans.length,
      },
      lists: {
        driftedVerticals,
        timerHeavyVerticals,
        revisionedVerticals,
        staleVerticals,
        humanNeededVerticals,
        stuckVerticals: stuck,
        retryShardScans,
      },
    };
  }, [funnel?.stuck, holdingData?.verticals, shardScans]);
}
