import React from "react";
import { Panel, useReactFlow } from "@xyflow/react";

export function GraphFlowToolbar({
  collapseEvents,
  setCollapseEvents,
  hideOrphans,
  setHideOrphans,
  q,
  setQ,
  onResetLayout,
  nodeCount,
  edgeCount,
  stuckCount,
  layoutDir,
  setLayoutDir,
  isFullscreen,
  onToggleFullscreen,
}) {
  const rf = useReactFlow();
  return (
    <Panel position="top-left" className="rf-panel tiny">
      <div className="graph-toolbar">
        <input className="mono" style={{ width: 180 }} placeholder="search node..." value={q} onChange={(e) => setQ(e.target.value)} />
        <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
          <input type="checkbox" checked={collapseEvents} onChange={(e) => setCollapseEvents(e.target.checked)} />
          direct links
        </label>
        <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
          <input type="checkbox" checked={hideOrphans} onChange={(e) => setHideOrphans(e.target.checked)} />
          hide idle
        </label>
        <div className="graph-dir-toggle">
          <button className={`graph-dir-btn ${layoutDir === "LR" ? "active" : ""}`} onClick={() => setLayoutDir("LR")} title="Left to Right">&rarr;</button>
          <button className={`graph-dir-btn ${layoutDir === "TB" ? "active" : ""}`} onClick={() => setLayoutDir("TB")} title="Top to Bottom">&darr;</button>
        </div>
        <button className="btn-secondary" onClick={() => rf.zoomIn({ duration: 180 })}>+</button>
        <button className="btn-secondary" onClick={() => rf.zoomOut({ duration: 180 })}>-</button>
        <button className="btn-secondary" onClick={() => rf.fitView({ padding: 0.18, duration: 220 })}>Fit</button>
        <button className="btn-secondary" onClick={onResetLayout}>Reset</button>
        <button className={`btn-secondary graph-fullscreen-btn ${isFullscreen ? "active" : ""}`} onClick={onToggleFullscreen} title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}>{isFullscreen ? "\u2716" : "\u26F6"}</button>
        <span className="graph-stats mono">
          <span>{nodeCount} nodes</span>
          <span className="graph-stats-sep">/</span>
          <span>{edgeCount} edges</span>
          {stuckCount > 0 ? <span className="graph-stats-stuck">{stuckCount} stuck</span> : null}
        </span>
      </div>
    </Panel>
  );
}

export function GraphLegendPanel() {
  return (
    <Panel position="bottom-left" className="rf-panel tiny">
      <div className="graph-legend tiny" style={{ position: "static" }}>
        <span className="legend-chip holding">Holding</span>
        <span className="legend-chip template">Template</span>
        <span className="legend-chip opco">OpCo</span>
        <span className="legend-chip event">Event</span>
        <span className="legend-line mgmt">management</span>
        <span className="legend-line bootstrap">bootstrap</span>
        <span className="legend-line seeded">seeded</span>
        <span className="legend-line discovered">discovered</span>
        <span className="legend-line producer">producer</span>
        <span className="legend-line message">message</span>
        <span className="legend-line mailbox">mailbox</span>
      </div>
    </Panel>
  );
}
