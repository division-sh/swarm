import React from "react";
import JsonBlock from "../../components/JsonBlock.tsx";
import GraphView from "../graph/GraphView.tsx";
import { findEdgeBySelectionID } from "../graph/graphInspectorUtils.tsx";
import { fmtTime, relTime } from "../../lib/format.ts";

export default function FlowView({ state, actions }) {
  const {
    verticals,
    visibleFlowEvents,
    flowEvents,
    flowGraph,
    flowGraphMeta,
    flowActiveEdgeKeys,
    selectedFlowSummary,
    agents,
    flowView,
    flowStage,
    flowStageOptions,
    flowRubric,
    flowRubricOptions,
    flowVertical,
    flowStart,
    flowEnd,
    flowReplaySpeed,
    flowReplayOn,
    selectedFlowNodeID,
    selectedFlowEdgeID,
    flowViewGraph,
    graphFullscreen,
  } = state;
  const {
    setFlowView,
    setFlowStage,
    setFlowRubric,
    setFlowVertical,
    setFlowStart,
    setFlowEnd,
    setFlowReplaySpeed,
    setFlowReplayOn,
    setFlowReplayIndex,
    refresh,
    addToast,
    setSelectedFlowNodeID,
    setSelectedFlowEdgeID,
    setFlowViewGraph,
    setGraphFullscreen,
  } = actions;

  return (
    <div className="layout-graph">
      <section>
        <div className="head">
          <h2>Workflow Trace</h2>
          <div className="obs-filterbar">
            <div className="obs-filtergroup">
              <div className="obs-filterlabel">View</div>
              <select aria-label="Workflow trace view mode" value={flowView} onChange={(e) => { setFlowView(e.target.value); setFlowReplayOn(false); setFlowReplayIndex(0); setSelectedFlowEdgeID(""); }}>
                <option value="design">design map</option>
                <option value="runtime">live runtime</option>
                <option value="replay">historical replay</option>
              </select>
              <select aria-label="Workflow stage filter" value={flowStage} onChange={(e) => { setFlowStage(e.target.value); setSelectedFlowEdgeID(""); }}>
                {flowStageOptions.map((s) => <option key={s} value={s}>{s}</option>)}
              </select>
              <select aria-label="Workflow rubric filter" value={flowRubric} onChange={(e) => { setFlowRubric(e.target.value); setSelectedFlowEdgeID(""); }}>
                {flowRubricOptions.map((r) => <option key={r} value={r}>{r}</option>)}
              </select>
            </div>
            <div className="obs-filtergroup">
              <div className="obs-filterlabel">Scope</div>
              {(flowView === "runtime" || flowView === "replay") ? (
                <select aria-label="Workflow vertical scope" value={flowVertical} onChange={(e) => { setFlowVertical(e.target.value); setSelectedFlowEdgeID(""); }}>
                  <option value="">all verticals</option>
                  {(verticals || []).map((v) => (
                    <option key={v.id || v.slug} value={v.slug || v.id}>
                      {(v.slug || (v.id || "").slice(0, 8))} | {v.stage || "-"} | {v.geography || "-"}
                    </option>
                  ))}
                </select>
              ) : (
                <div className="obs-toggle">Design mode uses workflow contract scope</div>
              )}
              {flowView === "replay" ? (
                <>
                  <input
                    aria-label="Workflow replay start"
                    type="datetime-local"
                    value={flowStart}
                    onChange={(e) => setFlowStart(e.target.value)}
                    title="start (local time)"
                  />
                  <input
                    aria-label="Workflow replay end"
                    type="datetime-local"
                    value={flowEnd}
                    onChange={(e) => setFlowEnd(e.target.value)}
                    title="end (local time)"
                  />
                </>
              ) : null}
            </div>
            <div className="obs-filtergroup">
              <div className="obs-filterlabel">Replay</div>
              {flowView === "replay" ? (
                <>
                  <select aria-label="Workflow replay speed" value={String(flowReplaySpeed)} onChange={(e) => setFlowReplaySpeed(Number(e.target.value || "10"))}>
                    <option value="10">10x</option>
                    <option value="50">50x</option>
                    <option value="100">100x</option>
                  </select>
                  <button
                    className="btn-secondary"
                    onClick={() => setFlowReplayOn((v) => !v)}
                    disabled={visibleFlowEvents.length >= (flowEvents || []).length && flowReplayOn}
                  >
                    {flowReplayOn ? "Pause" : "Play"}
                  </button>
                  <button className="btn-secondary" onClick={() => { setFlowReplayOn(false); setFlowReplayIndex(0); }}>Reset Replay</button>
                </>
              ) : (
                <div className="obs-toggle">Replay controls activate in historical replay mode</div>
              )}
            </div>
            <div className="obs-filtergroup obs-filtergroup-actions">
              <div className="obs-filterlabel">Actions</div>
              <button onClick={() => refresh().catch((err) => addToast(err.message, "error"))}>Refresh</button>
              <button className="btn-secondary" disabled={!flowVertical} onClick={() => actions.openTopologyForVertical?.(flowVertical)}>Open Topology</button>
            </div>
          </div>
        </div>
        <div className="body">
          <GraphView
            graph={flowGraph}
            graphKey={`flow:${flowView}:${flowVertical || "all"}`}
            selectedNodeID={selectedFlowNodeID}
            selectedEdgeID={selectedFlowEdgeID}
            onSelectNode={setSelectedFlowNodeID}
            onSelectEdge={setSelectedFlowEdgeID}
            onDerivedGraph={setFlowViewGraph}
            runtimeAgents={agents}
            isFullscreen={graphFullscreen}
            onToggleFullscreen={() => setGraphFullscreen((p) => !p)}
            activeEdgeKeys={flowActiveEdgeKeys}
            stageFilter={flowStage}
            rubricFilter={flowRubric}
          />
          <div className="tiny" style={{ marginTop: 6 }}>
            Modes: design-time architecture, runtime live overlay, and replay from historical flow events.
          </div>
        </div>
      </section>

      <section>
        <div className="head">
          <h2>Flow Detail</h2>
          <div className="stack tiny mono">
            <span>
              {(flowGraphMeta && flowGraphMeta.node_count) || (flowGraph.nodes || []).length} nodes / {(flowGraphMeta && flowGraphMeta.edge_count) || (flowGraph.edges || []).length} edges
            </span>
            {(flowGraphMeta && (flowGraphMeta.workflow_version || flowGraphMeta.platform_version)) ? (
              <span>
                {(flowGraphMeta.workflow_name || "workflow")} {flowGraphMeta.workflow_version || "-"} | platform {flowGraphMeta.platform_version || "-"}
              </span>
            ) : null}
          </div>
        </div>
        <div className="body scroll">
          {(flowGraphMeta && (flowGraphMeta.workflow_version || flowGraphMeta.platform_version)) ? (
            <>
              <div className="tiny">Design Metadata</div>
              <JsonBlock
                data={{
                  workflow_name: flowGraphMeta.workflow_name || "",
                  workflow_version: flowGraphMeta.workflow_version || "",
                  platform_version: flowGraphMeta.platform_version || "",
                  workflow_stages: flowGraphMeta.workflow_stages || [],
                  timer_events: flowGraphMeta.timer_events || [],
                  sources: flowGraphMeta.sources || [],
                }}
                defaultOpen={2}
              />
            </>
          ) : null}
          {(flowView === "runtime" || flowView === "replay") && flowVertical ? (
            <>
              <div className="tiny" style={{ marginTop: 10 }}>Selected Vertical Flow Summary</div>
              <div className="row">
                <div className="health-card">
                  <div className="health-kv"><span>Vertical</span><span className="mono">{flowVertical}</span></div>
                  <div className="tiny">Event Summary</div>
                  <div className="health-kv"><span>Total Events</span><span className="mono">{selectedFlowSummary.total || 0}</span></div>
                  <div className="health-kv"><span>Latest</span><span title={fmtTime(selectedFlowSummary.last && selectedFlowSummary.last.timestamp)}>{selectedFlowSummary.last ? relTime(selectedFlowSummary.last.timestamp) : "-"}</span></div>
                  <div className="health-kv"><span>Earliest</span><span title={fmtTime(selectedFlowSummary.first && selectedFlowSummary.first.timestamp)}>{selectedFlowSummary.first ? relTime(selectedFlowSummary.first.timestamp) : "-"}</span></div>
                  <div className="stack" style={{ marginTop: 8 }}>
                    <button className="btn-secondary" onClick={() => actions.openTopologyForVertical?.(flowVertical)}>Open Topology</button>
                  </div>
                </div>
                <div className="holding-detail-section">
                  <div className="tiny" style={{ marginBottom: 6 }}>Stage Counts</div>
                  <JsonBlock data={selectedFlowSummary.byStage || {}} defaultOpen={2} />
                </div>
              </div>
            </>
          ) : null}
          {(() => {
            const g = flowViewGraph || flowGraph;
            const node = (g.nodes || []).find((n) => n.id === selectedFlowNodeID) || null;
            const edge = findEdgeBySelectionID(g.edges, selectedFlowEdgeID);
            const nodeEdges = node ? (g.edges || []).filter((e) => e.from === node.id || e.to === node.id) : [];
            return (
              <>
                {edge ? (
                  <>
                    <div className="health-card" style={{ marginBottom: 10 }}>
                      <div className="tiny">Selected Transition</div>
                      <div className="health-kv"><span>Kind</span><span>{edge.kind || "-"}</span></div>
                      <div className="health-kv"><span>From</span><span className="mono">{edge.from || "-"}</span></div>
                      <div className="health-kv"><span>To</span><span className="mono">{edge.to || "-"}</span></div>
                      <div className="health-kv"><span>Label</span><span>{edge.label || "-"}</span></div>
                    </div>
                    <div className="tiny">Selected Edge</div>
                    <JsonBlock data={edge} defaultOpen={2} />
                    {(Array.isArray(edge.transition_details) && edge.transition_details.length > 0) || (Array.isArray(edge.timer_details) && edge.timer_details.length > 0) ? (
                      <div className="row" style={{ marginTop: 10 }}>
                        <div className="holding-detail-section">
                          <div className="tiny" style={{ marginBottom: 6 }}>Workflow Transitions</div>
                          {!Array.isArray(edge.transition_details) || edge.transition_details.length === 0 ? (
                            <div className="empty-state">No workflow transition attached to this event</div>
                          ) : (
                            <table>
                              <thead><tr><th>ID</th><th>From</th><th>To</th><th>Owner</th><th>Guards</th><th>Actions</th></tr></thead>
                              <tbody>
                                {edge.transition_details.map((item) => (
                                  <tr key={item.id}>
                                    <td className="mono">{item.id}</td>
                                    <td>{Array.isArray(item.from) ? item.from.join(", ") : "-"}</td>
                                    <td>{item.to || "-"}</td>
                                    <td className="mono">{item.node || "-"}</td>
                                    <td className="tiny">{Array.isArray(item.guards) ? item.guards.join(", ") || "-" : "-"}</td>
                                    <td className="tiny">{Array.isArray(item.actions) ? item.actions.join(", ") || "-" : "-"}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          )}
                        </div>
                        <div className="holding-detail-section">
                          <div className="tiny" style={{ marginBottom: 6 }}>Timers</div>
                          {!Array.isArray(edge.timer_details) || edge.timer_details.length === 0 ? (
                            <div className="empty-state">No timer metadata attached to this event</div>
                          ) : (
                            <table>
                              <thead><tr><th>ID</th><th>Stage</th><th>Owner</th><th>Recurring</th></tr></thead>
                              <tbody>
                                {edge.timer_details.map((item) => (
                                  <tr key={item.id}>
                                    <td className="mono">{item.id}</td>
                                    <td>{item.stage || "-"}</td>
                                    <td className="mono">{item.owner || "-"}</td>
                                    <td>{item.recurring ? "yes" : "no"}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          )}
                        </div>
                      </div>
                    ) : null}
                  </>
                ) : null}
                {!node ? (
                  !edge ? <div className="empty-state">Click a node or edge to inspect flow details.</div> : null
                ) : (
                  <>
                    <div className="tiny">Node</div>
                    <JsonBlock data={node} defaultOpen={2} />
                    <div className="tiny" style={{ margin: "8px 0 4px" }}>Connected Edges ({nodeEdges.length})</div>
                    {nodeEdges.length === 0 ? (
                      <div className="empty-state">No connected edges</div>
                    ) : (
                      <table>
                        <thead><tr><th>Kind</th><th>From</th><th>To</th><th>Label</th></tr></thead>
                        <tbody>
                          {nodeEdges.slice(0, 80).map((e, i) => (
                            <tr key={`${e.from}-${e.to}-${i}`}>
                              <td>{e.kind}</td>
                              <td className="mono">{e.from}</td>
                              <td className="mono">{e.to}</td>
                              <td>{e.label || "-"}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </>
                )}
              </>
            );
          })()}

          <div className="tiny" style={{ margin: "12px 0 4px" }}>
            Flow Events ({visibleFlowEvents.length}{flowView === "replay" ? `/${(flowEvents || []).length}` : ""})
          </div>
          {visibleFlowEvents.length === 0 ? (
            <div className="empty-state">No flow events loaded</div>
          ) : (
            <table>
              <thead><tr><th>When</th><th>Type</th><th>Source</th><th>Targets</th><th>Flags</th></tr></thead>
              <tbody>
                {visibleFlowEvents.slice(0, 120).map((ev) => (
                  <tr key={ev.event_id}>
                    <td title={fmtTime(ev.timestamp)}>{relTime(ev.timestamp)}</td>
                    <td className="mono">{ev.event_type}</td>
                    <td className="mono">{ev.source_node || "-"}</td>
                    <td className="tiny">{(ev.target_nodes || []).join(", ") || "-"}</td>
                    <td>
                      {ev.intercepted ? <span className="tag tag-warn">intercepted</span> : null}
                      {ev.passthrough ? <span className="tag tag-info" style={{ marginLeft: 4 }}>passthrough</span> : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </section>
    </div>
  );
}
