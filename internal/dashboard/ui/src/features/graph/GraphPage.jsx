import React from "react";
import JsonBlock from "../../components/JsonBlock.jsx";
import StatusDot from "../../components/StatusDot.jsx";
import GraphView from "./GraphView.jsx";
import { isEventLinkedEdgeKind } from "./graphTypes.js";

export default function GraphPage({
  domain,
  controls,
  actions,
  helpers,
}) {
  const { verticals, graph, graphViewGraph, agents } = domain;
  const {
    graphMode,
    setGraphMode,
    graphVertical,
    setGraphVertical,
    selectedGraphNodeID,
    setSelectedGraphNodeID,
    selectedGraphEdgeID,
    setSelectedGraphEdgeID,
    graphFullscreen,
    setGraphFullscreen,
  } = controls;
  const { refreshGraph, setGraphViewGraph, restartAgent, openControl, inspectAgent, navigateToTask } = actions;
  const { fmtTime, relTime } = helpers;

  return (
    <div className="layout-graph">
      <section>
        <div className="head">
          <h2>Org Graph</h2>
          <div className="stack">
            <select value={graphMode} onChange={(e) => { setSelectedGraphNodeID(""); setSelectedGraphEdgeID(""); setGraphMode(e.target.value); }}>
              <option value="holding">holding</option>
              <option value="template">default OpCo template</option>
              <option value="opco">running OpCo</option>
            </select>
            {graphMode === "opco" ? (
              <select value={graphVertical} onChange={(e) => { setSelectedGraphNodeID(""); setSelectedGraphEdgeID(""); setGraphVertical(e.target.value); }}>
                {(verticals || []).map((v) => (
                  <option key={v.id || v.slug} value={v.slug || v.id}>
                    {(v.slug || (v.id || "").slice(0, 8))} | {v.stage || "-"} | {v.geography || "-"}
                  </option>
                ))}
              </select>
            ) : null}
            <button onClick={() => { refreshGraph().catch(() => {}); }}>Refresh</button>
          </div>
        </div>
        <div className="body">
          <GraphView
            graph={graph}
            graphKey={`${graphMode}:${graphMode === "opco" ? graphVertical : ""}:${(graph && graph.template_version) || ""}`}
            selectedNodeID={selectedGraphNodeID}
            selectedEdgeID={selectedGraphEdgeID}
            onSelectNode={setSelectedGraphNodeID}
            onSelectEdge={setSelectedGraphEdgeID}
            onDerivedGraph={setGraphViewGraph}
            runtimeAgents={agents}
            isFullscreen={graphFullscreen}
            onToggleFullscreen={() => setGraphFullscreen((p) => !p)}
          />
          <div className="tiny" style={{ marginTop: 6 }}>
            Bootstrap routes are solid, seeded routes dashed, discovered routes dotted. Message and mailbox edges are rendered separately from EventBus routing.
          </div>
        </div>
      </section>

      <section>
        <div className="head">
          <h2>Node Details</h2>
          <div className="tiny mono">{selectedGraphEdgeID ? "edge selected" : (selectedGraphNodeID || "none")}</div>
        </div>
        <div className="body scroll">
          {(() => {
            const g = graphViewGraph || graph;
            const edge = (g.edges || []).find((e, i) => `${e.kind}:${e.from}->${e.to}:${i}` === selectedGraphEdgeID) || null;
            const node = (g.nodes || []).find((n) => n.id === selectedGraphNodeID) || null;
            if (!node && !edge) return <div className="empty-state">Click a node or edge to inspect details.</div>;
            if (!node && edge) {
              return (
                <div className="node-detail-card">
                  <div className="tiny">Selected Edge</div>
                  <JsonBlock data={edge} defaultOpen={2} />
                </div>
              );
            }
            const rt = node.kind === "agent" ? (agents || []).find((a) => a.id === node.id) : null;
            const nodeEdges = (g.edges || []).filter((e) => e.from === node.id || e.to === node.id);
            return (
              <>
                {edge ? (
                  <div className="node-detail-card">
                    <div className="tiny">Selected Edge</div>
                    <JsonBlock data={edge} defaultOpen={2} />
                  </div>
                ) : null}
                <div className="node-detail-card">
                  <div className="tiny">Identity</div>
                  <div className="node-detail-grid">
                    <span className="node-detail-label">ID</span><span className="mono" style={{ fontSize: 11 }}>{node.id}</span>
                    <span className="node-detail-label">Kind</span><span>{node.kind}</span>
                    <span className="node-detail-label">Group</span><span>{node.group || "-"}</span>
                    <span className="node-detail-label">Role</span><span>{node.role || "-"}</span>
                    {node.mode ? <><span className="node-detail-label">Mode</span><span>{node.mode}</span></> : null}
                    {node.status ? <><span className="node-detail-label">Status</span><span>{node.status}</span></> : null}
                    {node.vertical_slug ? <><span className="node-detail-label">Vertical</span><span>{node.vertical_slug}</span></> : null}
                    {node.parent_id ? <><span className="node-detail-label">Parent</span><span className="mono" style={{ fontSize: 10, cursor: "pointer", color: "var(--info)" }} onClick={() => setSelectedGraphNodeID(node.parent_id)}>{node.parent_id}</span></> : null}
                  </div>
                </div>

                {rt ? (
                  <div className="node-detail-card">
                    <div className="tiny">Runtime</div>
                    <div className="node-detail-grid">
                      <span className="node-detail-label">State</span>
                      <span><StatusDot state={rt.state} />{rt.state}</span>
                      <span className="node-detail-label">Turns</span>
                      <span className="mono">{rt.turn_count}/{rt.turn_limit}{rt.turn_limit > 0 ? ` (${Math.round((rt.turn_count / rt.turn_limit) * 100)}%)` : ""}</span>
                      <span className="node-detail-label">Turns 24h</span>
                      <span className="mono">{(rt.turns_24h || 0).toLocaleString()}</span>
                      <span className="node-detail-label">Tokens 24h</span>
                      <span className="mono">{(rt.total_tokens_24h || 0).toLocaleString()}</span>
                      <span className="node-detail-label">Pending</span>
                      <span className={`mono ${(rt.pending_events || 0) > 0 ? "health-warn" : ""}`}>{rt.pending_events || 0}</span>
                      {rt.current_task_id ? <><span className="node-detail-label">Task</span><span className="mono" style={{ cursor: "pointer", color: "var(--info)" }} onClick={() => navigateToTask(rt.current_task_id)}>{rt.current_task_id.slice(0, 12)}</span></> : null}
                      {rt.stuck_reason ? <><span className="node-detail-label">Stuck</span><span style={{ color: "var(--bad)" }}>{rt.stuck_reason}</span></> : null}
                      {rt.started_at ? <><span className="node-detail-label">Started</span><span title={fmtTime(rt.started_at)}>{relTime(rt.started_at)}</span></> : null}
                    </div>
                    {rt.last_tool && rt.last_tool.name ? (
                      <div style={{ marginTop: 6, display: "flex", gap: 8, alignItems: "baseline" }}>
                        <span className="node-detail-label">Last Tool</span>
                        <span className="mono" style={{ fontSize: 11 }}>{rt.last_tool.name}{rt.last_tool.ok === false ? " (fail)" : ""}</span>
                      </div>
                    ) : null}
                  </div>
                ) : null}

                {(node.tools || []).length > 0 ? (
                  <div className="node-detail-card">
                    <div className="tiny">Tools ({(node.tools || []).length})</div>
                    <div className="node-tools">
                      {(node.tools || []).map((t, i) => (
                        <span key={i} className="node-tool-badge">{typeof t === "string" ? t : (t.name || t.type || JSON.stringify(t))}</span>
                      ))}
                    </div>
                  </div>
                ) : null}

                {(node.subscriptions || []).length > 0 ? (
                  <div className="node-detail-card">
                    <div className="tiny">Subscriptions ({(node.subscriptions || []).length})</div>
                    <div className="node-subs">
                      {(node.subscriptions || []).map((s, i) => (
                        <div key={i} className="node-sub-item mono">{typeof s === "string" ? s : (s.type || s.event_type || JSON.stringify(s))}</div>
                      ))}
                    </div>
                  </div>
                ) : null}

                {nodeEdges.length > 0 ? (
                  <div className="node-detail-card">
                    <div className="tiny">Connections ({nodeEdges.length})</div>
                    <table style={{ fontSize: 11 }}>
                      <thead><tr><th>Kind</th><th>From</th><th>To</th><th>Source</th></tr></thead>
                      <tbody>
                        {nodeEdges.map((e, i) => (
                          <tr key={i}>
                            <td><span className={`badge ${isEventLinkedEdgeKind(e.kind) || e.kind === "message" || e.kind === "mailbox" ? "b-running" : ""}`} style={{ fontSize: 9 }}>{e.kind}</span></td>
                            <td className="mono" style={{ fontSize: 10, cursor: e.from !== node.id ? "pointer" : "default", color: e.from === node.id ? "var(--text-3)" : "var(--info)" }} onClick={() => { if (e.from !== node.id) setSelectedGraphNodeID(e.from); }}>{e.from === node.id ? "self" : e.from}</td>
                            <td className="mono" style={{ fontSize: 10, cursor: e.to !== node.id ? "pointer" : "default", color: e.to === node.id ? "var(--text-3)" : "var(--info)" }} onClick={() => { if (e.to !== node.id) setSelectedGraphNodeID(e.to); }}>{e.to === node.id ? "self" : e.to}</td>
                            <td className="tiny">{e.source || e.label || "-"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : null}

                {rt && node.kind === "agent" ? (
                  <div className="node-detail-card">
                    <div className="tiny">Quick Actions</div>
                    <div className="stack" style={{ marginTop: 4 }}>
                      <button className="btn-secondary" onClick={() => {
                        if (!window.confirm(`Restart agent "${node.id}"?`)) return;
                        restartAgent(node.id).catch(() => {});
                      }}>Restart</button>
                      <button className="btn-secondary" onClick={() => openControl(node.id)}>Control</button>
                      <button className="btn-secondary" onClick={() => inspectAgent(node.id)}>Inspect</button>
                    </div>
                  </div>
                ) : null}

                {node.system_prompt ? (
                  <details className="node-detail-card">
                    <summary className="tiny" style={{ cursor: "pointer" }}>System Prompt</summary>
                    <pre className="json" style={{ whiteSpace: "pre-wrap", maxHeight: "40vh", marginTop: 6 }}>
                      {node.system_prompt}
                    </pre>
                  </details>
                ) : null}

                {node.constraints && Object.keys(node.constraints).length > 0 ? (
                  <div className="node-detail-card">
                    <div className="tiny">Constraints</div>
                    <div className="json" style={{ maxHeight: 120 }}>
                      {JSON.stringify(node.constraints, null, 2)}
                    </div>
                  </div>
                ) : null}
              </>
            );
          })()}
        </div>
      </section>
    </div>
  );
}
