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
  focusMode,
  setFocusMode,
  fadeUnrelated,
  setFadeUnrelated,
  onJumpToSelection,
  onJumpToStuck,
  onJumpToHumans,
  viewSlots,
  onSaveView,
  onLoadView,
  performanceMode,
}) {
  const rf = useReactFlow();
  return (
    <Panel position="top-left" className="rf-panel tiny">
      <div className="graph-toolbar">
        <input className="mono" style={{ width: 180 }} placeholder="search node..." value={q} onChange={(e) => setQ(e.target.value)} />
        <select aria-label="Graph focus mode" value={focusMode} onChange={(e) => setFocusMode(e.target.value)}>
          <option value="all">all</option>
          <option value="selected">selected neighborhood</option>
          <option value="active">active path</option>
          <option value="stuck">stuck only</option>
          <option value="system">system layer</option>
          <option value="humans">human/mailbox</option>
          <option value="disconnected">disconnected</option>
        </select>
        <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
          <input type="checkbox" checked={collapseEvents} onChange={(e) => setCollapseEvents(e.target.checked)} />
          direct links
        </label>
        <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
          <input type="checkbox" checked={hideOrphans} onChange={(e) => setHideOrphans(e.target.checked)} />
          hide idle
        </label>
        <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
          <input type="checkbox" checked={fadeUnrelated} onChange={(e) => setFadeUnrelated(e.target.checked)} />
          fade unrelated
        </label>
        <div className="graph-dir-toggle">
          <button aria-label="Left to right layout" className={`graph-dir-btn ${layoutDir === "LR" ? "active" : ""}`} onClick={() => setLayoutDir("LR")} title="Left to Right">&rarr;</button>
          <button aria-label="Top to bottom layout" className={`graph-dir-btn ${layoutDir === "TB" ? "active" : ""}`} onClick={() => setLayoutDir("TB")} title="Top to Bottom">&darr;</button>
        </div>
        <button aria-label="Zoom in" className="btn-secondary" onClick={() => rf.zoomIn({ duration: 180 })}>+</button>
        <button aria-label="Zoom out" className="btn-secondary" onClick={() => rf.zoomOut({ duration: 180 })}>-</button>
        <button aria-label="Fit graph to view (shortcut F)" className="btn-secondary" onClick={() => rf.fitView({ padding: 0.18, duration: 220 })}>Fit</button>
        <button aria-label="Jump to selected node (shortcut S)" className="btn-secondary" onClick={onJumpToSelection}>Selection</button>
        <button aria-label="Jump to stuck nodes (shortcut K)" className="btn-secondary" onClick={onJumpToStuck}>Stuck</button>
        <button aria-label="Jump to human and mailbox nodes (shortcut H)" className="btn-secondary" onClick={onJumpToHumans}>Humans</button>
        <button aria-label="Reset graph layout (shortcut R)" className="btn-secondary" onClick={onResetLayout}>Reset</button>
        <button className={`btn-secondary graph-fullscreen-btn ${isFullscreen ? "active" : ""}`} onClick={onToggleFullscreen} title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}>{isFullscreen ? "\u2716" : "\u26F6"}</button>
        <div className="graph-view-slots">
          {[1, 2, 3].map((slot) => (
            <button
              key={slot}
              className={`btn-secondary graph-view-slot ${viewSlots[slot] ? "filled" : ""}`}
              title={viewSlots[slot] ? `Load view ${slot}` : `Empty slot ${slot}`}
              onClick={() => onLoadView(slot)}
              onContextMenu={(event) => {
                event.preventDefault();
                onSaveView(slot);
              }}
            >
              V{slot}
            </button>
          ))}
        </div>
        <span className="graph-stats mono">
          <span>{nodeCount} nodes</span>
          <span className="graph-stats-sep">/</span>
          <span>{edgeCount} edges</span>
          {stuckCount > 0 ? <span className="graph-stats-stuck">{stuckCount} stuck</span> : null}
          {performanceMode ? <span className="graph-stats-sep">/</span> : null}
          {performanceMode ? <span>perf mode</span> : null}
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
