import React from "react";
import { relTime } from "../../lib/format.ts";

function verticalKey(vertical) {
  return vertical?.slug || vertical?.id || "";
}

function verticalButton(label, vertical, onFocus, actionLabel, onAction) {
  const key = verticalKey(vertical);
  return (
    <div key={`${label}:${key}`} className="health-card" style={{ marginBottom: 8 }}>
      <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
        <div>
          <div className="tiny">{label}</div>
          <div>{vertical.slug || vertical.name || vertical.id}</div>
        </div>
        <div className="stack">
          <button className="btn-secondary" onClick={() => onFocus(key)}>Focus</button>
          {actionLabel ? <button className="btn-secondary" onClick={() => onAction?.(key, vertical)}>{actionLabel}</button> : null}
        </div>
      </div>
      <div className="stack tiny">
        <span>stage {vertical.workflow_current_stage || vertical.stage || "-"}</span>
        {vertical.geography ? <span>{vertical.geography}</span> : null}
        {vertical.stage_entered_at ? <span>entered {relTime(vertical.stage_entered_at)}</span> : null}
        {(vertical.active_timer_count || 0) > 0 ? <span>timers {vertical.active_timer_count}</span> : null}
        {(vertical.revision_count || 0) > 0 ? <span>rev {vertical.revision_count}</span> : null}
      </div>
    </div>
  );
}

function shardButton(scan, onFocusTrace) {
  return (
    <div key={scan.scan_id} className="health-card" style={{ marginBottom: 8 }}>
      <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
        <div>
          <div className="tiny">Retry Needed</div>
          <div className="mono">{scan.scan_id}</div>
        </div>
        <button className="btn-secondary" onClick={() => onFocusTrace(scan.geography || "")}>Open Funnel</button>
      </div>
      <div className="stack tiny">
        <span>{scan.mode || "-"}</span>
        <span>{scan.geography || "-"}</span>
        <span>failed {scan.shards_failed || 0}</span>
        <span>stuck {scan.shards_stuck || 0}</span>
      </div>
    </div>
  );
}

export default function PortfolioTriageSummary({
  triage,
  onFocusVertical,
  onOpenHolding,
  onOpenPipeline,
  onOpenWorkflowTrace,
  onOpenOperations,
}) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div className="head">
        <h2>Portfolio Triage</h2>
        <div className="stack tiny">
          <span>drift {triage.summary.drift}</span>
          <span>timers {triage.summary.timers}</span>
          <span>revisions {triage.summary.revisions}</span>
          <span>stuck {triage.summary.stuck}</span>
          <span>retry scans {triage.summary.retryScans}</span>
        </div>
      </div>

      <div className="row3" style={{ marginBottom: 10 }}>
        <div className="health-card"><div className="tiny">Drift</div><div className="big-number">{triage.summary.drift}</div><div className="tiny">db vs workflow stage mismatch</div></div>
        <div className="health-card"><div className="tiny">Timers</div><div className="big-number">{triage.summary.timers}</div><div className="tiny">verticals with active workflow timers</div></div>
        <div className="health-card"><div className="tiny">Revisions</div><div className="big-number">{triage.summary.revisions}</div><div className="tiny">verticals with workflow revisions</div></div>
      </div>
      <div className="row3" style={{ marginBottom: 12 }}>
        <div className="health-card"><div className="tiny">Stale Stage</div><div className="big-number">{triage.summary.stale}</div><div className="tiny">in stage longer than 72h</div></div>
        <div className="health-card"><div className="tiny">Human Needed</div><div className="big-number">{triage.summary.humanNeeded}</div><div className="tiny">ready for review</div></div>
        <div className="health-card"><div className="tiny">Retry Scans</div><div className="big-number">{triage.summary.retryScans}</div><div className="tiny">failed or stuck shard scans</div></div>
      </div>

      <div className="row" style={{ gap: 12 }}>
        <div className="holding-detail-section">
          <div className="tiny" style={{ marginBottom: 6 }}>Drifted Verticals</div>
          {triage.lists.driftedVerticals.slice(0, 4).map((vertical) => verticalButton("Drift", vertical, onFocusVertical, "Holding", (_, item) => onOpenHolding(item)))}
          {triage.lists.driftedVerticals.length === 0 ? <div className="empty-state">No drifted verticals</div> : null}
        </div>
        <div className="holding-detail-section">
          <div className="tiny" style={{ marginBottom: 6 }}>Stale / Timer Heavy</div>
          {triage.lists.staleVerticals.slice(0, 2).map((vertical) => verticalButton("Stale", vertical, onFocusVertical, "Workflow", (_, item) => onOpenWorkflowTrace(item)))}
          {triage.lists.timerHeavyVerticals.slice(0, 2).map((vertical) => verticalButton("Timer Heavy", vertical, onFocusVertical, "Workflow", (_, item) => onOpenWorkflowTrace(item)))}
          {triage.lists.staleVerticals.length === 0 && triage.lists.timerHeavyVerticals.length === 0 ? <div className="empty-state">No stale or timer-heavy verticals</div> : null}
        </div>
        <div className="holding-detail-section">
          <div className="tiny" style={{ marginBottom: 6 }}>Human Needed / Retry Scans</div>
          {triage.lists.humanNeededVerticals.slice(0, 2).map((vertical) => verticalButton("Human Needed", vertical, onFocusVertical, "Operations", (_, item) => onOpenOperations?.(item)))}
          {triage.lists.retryShardScans.slice(0, 2).map((scan) => shardButton(scan, onOpenPipeline))}
          {triage.lists.humanNeededVerticals.length === 0 && triage.lists.retryShardScans.length === 0 ? <div className="empty-state">No human-needed items or retry scans</div> : null}
        </div>
      </div>
    </div>
  );
}
