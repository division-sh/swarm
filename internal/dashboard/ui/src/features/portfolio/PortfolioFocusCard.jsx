import React from "react";
import { fmtTime, relTime } from "../../lib/format.js";

export default function PortfolioFocusCard({
  focusSummary,
  subview,
  onSelectSubview,
  onOpenHoldingDetail,
  onOpenWorkflowTrace,
  onOpenWorkflowTopology,
  onOpenFunnelTrace,
  onClearFocus,
}) {
  const vertical = focusSummary.vertical;

  return (
    <div className="health-card" style={{ marginBottom: 10 }}>
      <div className="stack" style={{ justifyContent: "space-between", marginBottom: 8 }}>
        <div>
          <div className="tiny">Portfolio Focus</div>
          <div>{vertical ? `${vertical.slug || vertical.name || vertical.id} | ${vertical.geography || "-"}` : "No focused vertical"}</div>
        </div>
        <div className="stack">
          <button className={subview === "holding" ? "active" : "btn-secondary"} onClick={() => onSelectSubview("holding")}>Holding</button>
          <button className={subview === "pipeline" ? "active" : "btn-secondary"} onClick={() => onSelectSubview("pipeline")}>Funnel</button>
          <button className="btn-secondary" onClick={onClearFocus}>Clear Focus</button>
        </div>
      </div>

      <div className="row" style={{ marginBottom: 8 }}>
        <div className="health-card">
          <div className="health-kv"><span>Vertical</span><span className="mono">{focusSummary.key || "-"}</span></div>
          <div className="health-kv"><span>DB Stage</span><span>{vertical?.stage || "-"}</span></div>
          <div className="health-kv"><span>Workflow Stage</span><span>{vertical?.workflow_current_stage || "-"}</span></div>
        </div>
        <div className="health-card">
          <div className="health-kv"><span>Drift</span><span className={focusSummary.drift ? "health-warn mono" : "mono"}>{focusSummary.drift ? "yes" : "no"}</span></div>
          <div className="health-kv"><span>Timers</span><span className="mono">{focusSummary.activeTimers}</span></div>
          <div className="health-kv"><span>Revisions</span><span className="mono">{focusSummary.revisions}</span></div>
        </div>
        <div className="health-card">
          <div className="health-kv"><span>Latest Trace</span><span title={fmtTime(focusSummary.latestTraceRow?.created_at || focusSummary.latestTraceRow?.timestamp)}>{focusSummary.latestTraceRow ? relTime(focusSummary.latestTraceRow.created_at || focusSummary.latestTraceRow.timestamp) : "-"}</span></div>
          <div className="health-kv"><span>Latest Type</span><span>{focusSummary.latestTraceRow?.type || focusSummary.latestTraceRow?.event_type || "-"}</span></div>
          <div className="health-kv"><span>Trace Rows</span><span className="mono">{focusSummary.traceCount}</span></div>
        </div>
      </div>

      <div className="stack">
        <button className="btn-secondary" disabled={!vertical?.id} onClick={onOpenHoldingDetail}>Open Holding Detail</button>
        <button className="btn-secondary" disabled={!focusSummary.key} onClick={onOpenFunnelTrace}>Open Funnel Trace</button>
        <button className="btn-secondary" disabled={!focusSummary.key} onClick={onOpenWorkflowTrace}>Open Workflow Trace</button>
        <button className="btn-secondary" disabled={!focusSummary.key} onClick={onOpenWorkflowTopology}>Open Workflow Topology</button>
      </div>
    </div>
  );
}
