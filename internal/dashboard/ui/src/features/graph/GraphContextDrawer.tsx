import React from "react";
import { compactValue, edgeSelectionID, renderTagList } from "./graphInspectorUtils.tsx";

export default function GraphContextDrawer({
  selectedNode,
  selectedEdge,
  selectedRuntime,
  selectedEdgeContract,
  incomingEdges,
  outgoingEdges,
  relatedNodes,
  edgeKinds,
  viewGraphEdges,
  onSelectNode,
  onSelectEdge,
}) {
  return (
    <section>
      <div className="head">
        <h2>Selection Context</h2>
        <div className="tiny mono">{selectedNode?.id || selectedEdge?.kind || "none"}</div>
      </div>
      <div className="body">
        {!selectedNode && !selectedEdge ? (
          <div className="empty-state">Selection context will appear here when you pick a node or connection.</div>
        ) : (
          <div className="graph-drawer-grid">
            <div className="node-detail-card">
              <div className="tiny">Pathways</div>
              <div className="graph-drawer-columns">
                <div>
                  <div className="node-detail-label">Incoming</div>
                  {incomingEdges.length === 0 ? <div className="empty-state">No incoming links</div> : (
                    <div className="stack graph-card-stack">
                      {incomingEdges.slice(0, 6).map((edge, index) => (
                        <button key={`in:${index}`} className="btn-secondary" onClick={() => onSelectEdge(edgeSelectionID(edge, viewGraphEdges))}>
                          {edge.kind} | {edge.from}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                <div>
                  <div className="node-detail-label">Outgoing</div>
                  {outgoingEdges.length === 0 ? <div className="empty-state">No outgoing links</div> : (
                    <div className="stack graph-card-stack">
                      {outgoingEdges.slice(0, 6).map((edge, index) => (
                        <button key={`out:${index}`} className="btn-secondary" onClick={() => onSelectEdge(edgeSelectionID(edge, viewGraphEdges))}>
                          {edge.kind} | {edge.to}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>

            <div className="node-detail-card">
              <div className="tiny">Related Nodes</div>
              {relatedNodes.length === 0 ? (
                <div className="empty-state">No adjacent nodes</div>
              ) : (
                <>
                  <div className="graph-drawer-columns">
                    <div>
                      <div className="node-detail-label">Neighbors</div>
                      {renderTagList(relatedNodes.slice(0, 10).map((node) => `${node.kind}:${node.id}`), "No adjacent nodes")}
                    </div>
                    <div>
                      <div className="node-detail-label">Edge Kinds</div>
                      {renderTagList(edgeKinds, "No edge kinds")}
                    </div>
                  </div>
                  <div className="stack graph-card-stack" style={{ marginTop: 8 }}>
                    {relatedNodes.slice(0, 8).map((node) => (
                      <button key={node.id} className="btn-secondary" onClick={() => onSelectNode(node.id)}>
                        {node.label || node.id}
                      </button>
                    ))}
                  </div>
                </>
              )}
            </div>

            <div className="node-detail-card">
              <div className="tiny">Contracts And Runtime</div>
              {selectedEdgeContract ? (
                <div className="graph-drawer-columns">
                  <div>
                    <div className="node-detail-label">Stages</div>
                    {renderTagList(selectedEdgeContract.stages, "No stage buckets")}
                  </div>
                  <div>
                    <div className="node-detail-label">Transitions</div>
                    {renderTagList(selectedEdgeContract.transitions, "No transitions")}
                  </div>
                  <div>
                    <div className="node-detail-label">Timers</div>
                    {renderTagList(selectedEdgeContract.timers, "No timers")}
                  </div>
                  <div>
                    <div className="node-detail-label">Schema</div>
                    {renderTagList([...selectedEdgeContract.required, ...selectedEdgeContract.properties], "No schema fields")}
                  </div>
                </div>
              ) : selectedRuntime ? (
                <div className="graph-drawer-columns">
                  <div>
                    <div className="node-detail-label">Runtime Pressure</div>
                    <div className="health-kv"><span>State</span><span>{selectedRuntime.state || "-"}</span></div>
                    <div className="health-kv"><span>Pending</span><span className="mono">{selectedRuntime.pending_events || 0}</span></div>
                    <div className="health-kv"><span>Turns 24h</span><span className="mono">{(selectedRuntime.turns_24h || 0).toLocaleString()}</span></div>
                  </div>
                  <div>
                    <div className="node-detail-label">Capabilities</div>
                    {renderTagList((selectedNode?.tools || []).map((tool) => (typeof tool === "string" ? tool : (tool.name || tool.type || compactValue(tool)))), "No tools attached")}
                  </div>
                  <div>
                    <div className="node-detail-label">Subscriptions</div>
                    {renderTagList((selectedNode?.subscriptions || []).map((subscription) => (typeof subscription === "string" ? subscription : (subscription.type || subscription.event_type || compactValue(subscription)))), "No subscriptions")}
                  </div>
                </div>
              ) : (
                <div className="empty-state">Select an edge for contract scope or an agent node for runtime pressure.</div>
              )}
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
