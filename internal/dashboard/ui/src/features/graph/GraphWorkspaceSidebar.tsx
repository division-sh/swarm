import React from "react";

export default function GraphWorkspaceSidebar({
  verticals,
  graphMode,
  graphVertical,
  graphNodeCount,
  graphEdgeCount,
  selectedNode,
  selectedEdge,
  selectionVertical,
  incomingEdges,
  outgoingEdges,
  relatedNodes,
  selectedRuntime,
  onChangeMode,
  onChangeVertical,
  onRefresh,
  onOpenTrace,
  onClearSelection,
  onGoToParent,
  onInspectAgent,
}) {
  return (
    <div className="graph-workspace-sidebar">
      <section>
        <div className="head">
          <h2>Investigation</h2>
        </div>
        <div className="body">
          <div className="graph-sidebar-intro">
            <div className="tiny">Workspace</div>
            <div className="graph-sidebar-title">Workflow topology investigation studio</div>
            <div className="tiny">Use the canvas for structural navigation, the inspector for structured detail, and the context drawer for path-level relationships.</div>
          </div>
          <div className="graph-sidebar-stats">
            <div className="health-kv"><span>Mode</span><span>{graphMode}</span></div>
            <div className="health-kv"><span>Scope</span><span className="mono">{graphVertical || "global"}</span></div>
            <div className="health-kv"><span>Nodes</span><span className="mono">{graphNodeCount}</span></div>
            <div className="health-kv"><span>Edges</span><span className="mono">{graphEdgeCount}</span></div>
          </div>
        </div>
      </section>

      <section>
        <div className="head">
          <h2>Scope</h2>
        </div>
        <div className="body">
          <div className="obs-filtergroup">
            <div className="obs-filterlabel">Topology Mode</div>
            <select aria-label="Graph topology mode" value={graphMode} onChange={(e) => onChangeMode(e.target.value)}>
              <option value="holding">holding map</option>
              <option value="template">template map</option>
              <option value="opco">live opco</option>
            </select>
          </div>
          <div className="obs-filtergroup graph-sidebar-group">
            <div className="obs-filterlabel">Vertical Scope</div>
            {graphMode === "opco" ? (
              <select aria-label="Graph vertical scope" value={graphVertical} onChange={(e) => onChangeVertical(e.target.value)}>
                <option value="">select vertical…</option>
                {(verticals || []).map((vertical) => (
                  <option key={vertical.id || vertical.slug} value={vertical.slug || vertical.id}>
                    {(vertical.slug || (vertical.id || "").slice(0, 8))} | {vertical.stage || "-"} | {vertical.geography || "-"}
                  </option>
                ))}
              </select>
            ) : (
              <div className="obs-toggle">Global scope is fixed by the selected map mode.</div>
            )}
          </div>
          <div className="stack graph-sidebar-actions">
            <button onClick={onRefresh}>Refresh</button>
            <button className="btn-secondary" disabled={graphMode !== "opco" || !graphVertical} onClick={() => onOpenTrace(graphVertical, "")}>Open Trace</button>
            <button className="btn-secondary" onClick={onClearSelection}>Clear Selection</button>
          </div>
        </div>
      </section>

      <section>
        <div className="head">
          <h2>Selection</h2>
        </div>
        <div className="body">
          {!selectedNode && !selectedEdge ? (
            <div className="empty-state">Pick a node or edge from the canvas to start an investigation.</div>
          ) : (
            <>
              <div className="graph-sidebar-selection">
                <div className="tiny">Current Focus</div>
                <div className="graph-sidebar-selection-title mono">{selectedNode?.id || selectedEdge?.event_type || selectedEdge?.label || selectedEdge?.kind || "-"}</div>
                <div className="tiny">{selectedNode ? `${selectedNode.kind} node` : `${selectedEdge?.kind || "edge"} connection`}</div>
              </div>
              <div className="graph-sidebar-stats">
                <div className="health-kv"><span>Vertical</span><span className="mono">{selectionVertical || "-"}</span></div>
                <div className="health-kv"><span>Incoming</span><span className="mono">{incomingEdges.length}</span></div>
                <div className="health-kv"><span>Outgoing</span><span className="mono">{outgoingEdges.length}</span></div>
                <div className="health-kv"><span>Neighbors</span><span className="mono">{relatedNodes.length}</span></div>
              </div>
              <div className="stack graph-sidebar-actions">
                {selectionVertical ? <button className="btn-secondary" onClick={() => onOpenTrace(selectionVertical, selectedNode?.id || "")}>Open Vertical Trace</button> : null}
                {selectedNode ? <button className="btn-secondary" onClick={onGoToParent} disabled={!selectedNode.parent_id}>Go To Parent</button> : null}
                {selectedNode ? <button className="btn-secondary" onClick={onInspectAgent} disabled={!selectedRuntime}>Inspect Agent</button> : null}
              </div>
            </>
          )}
        </div>
      </section>
    </div>
  );
}
