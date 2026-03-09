import React from "react";
import DataTable from "../../components/DataTable.jsx";
import StatusDot from "../../components/StatusDot.jsx";
import { fmtTime, relTime } from "../../lib/format.ts";
import { isEventLinkedEdgeKind } from "./graphTypes.js";
import { compactValue, renderRawDetails, renderTagList } from "./graphInspectorUtils.jsx";

function detailButton(label, onClick, disabled = false) {
  return <button className="btn-secondary" disabled={disabled} onClick={onClick}>{label}</button>;
}

function EdgeInspector({
  edge,
  viewGraphNodes,
  onSelectNode,
}) {
  if (!edge) return null;
  const fromNode = (viewGraphNodes || []).find((item) => item.id === edge.from) || null;
  const toNode = (viewGraphNodes || []).find((item) => item.id === edge.to) || null;
  const transitionIDs = edge.transition_ids || [];
  const timerIDs = edge.timer_ids || [];
  const schemaRequired = edge.schema_required || [];
  const schemaProperties = edge.schema_properties || [];
  const stages = edge.stages || [];

  return (
    <>
      <div className="health-card" style={{ marginBottom: 10 }}>
        <div className="tiny">Selected Connection</div>
        <div className="health-kv"><span>Kind</span><span>{edge.kind || "-"}</span></div>
        <div className="health-kv"><span>Source</span><span>{edge.source || edge.label || "-"}</span></div>
        <div className="health-kv"><span>Event</span><span className="mono">{edge.event_type || edge.label || "-"}</span></div>
        <div className="health-kv"><span>Status</span><span>{edge.status || "-"}</span></div>
        <div className="health-kv"><span>Reason</span><span>{edge.reason || "-"}</span></div>
      </div>

      <div className="node-detail-card">
        <div className="tiny">Participants</div>
        <div className="node-detail-grid">
          <span className="node-detail-label">From</span>
          <span className="mono graph-linklike" onClick={() => edge.from && onSelectNode(edge.from)}>{fromNode?.label || edge.from || "-"}</span>
          <span className="node-detail-label">To</span>
          <span className="mono graph-linklike" onClick={() => edge.to && onSelectNode(edge.to)}>{toNode?.label || edge.to || "-"}</span>
          <span className="node-detail-label">Direction</span>
          <span>{edge.from && edge.to ? `${edge.from} -> ${edge.to}` : "-"}</span>
          <span className="node-detail-label">Label</span>
          <span>{edge.label || "-"}</span>
        </div>
      </div>

      {(stages.length > 0 || transitionIDs.length > 0 || timerIDs.length > 0 || schemaRequired.length > 0 || schemaProperties.length > 0) ? (
        <div className="node-detail-card">
          <div className="tiny">Contract Scope</div>
          {stages.length > 0 ? (
            <>
              <div className="node-detail-label graph-detail-label-spacer">Stages</div>
              {renderTagList(stages, "No stage buckets")}
            </>
          ) : null}
          {transitionIDs.length > 0 ? (
            <>
              <div className="node-detail-label graph-detail-label-spacer">Transitions</div>
              {renderTagList(transitionIDs, "No linked transitions")}
            </>
          ) : null}
          {timerIDs.length > 0 ? (
            <>
              <div className="node-detail-label graph-detail-label-spacer">Timers</div>
              {renderTagList(timerIDs, "No linked timers")}
            </>
          ) : null}
          {schemaRequired.length > 0 ? (
            <>
              <div className="node-detail-label graph-detail-label-spacer">Required Fields</div>
              {renderTagList(schemaRequired, "No required fields")}
            </>
          ) : null}
          {schemaProperties.length > 0 ? (
            <>
              <div className="node-detail-label graph-detail-label-spacer">Schema Properties</div>
              {renderTagList(schemaProperties, "No schema properties")}
            </>
          ) : null}
        </div>
      ) : null}

      {edge.transition_details?.length ? (
        <div className="node-detail-card">
          <div className="tiny">Transition Details</div>
          <div className="stack graph-card-stack">
            {edge.transition_details.map((detail, index) => (
              <div key={`${detail.id || "transition"}:${index}`} className="health-card graph-inner-health-card">
                <div className="health-kv"><span>ID</span><span className="mono">{detail.id || "-"}</span></div>
                <div className="health-kv"><span>Owner</span><span>{detail.owner || "-"}</span></div>
                <div className="health-kv"><span>From</span><span>{detail.from_stage || "-"}</span></div>
                <div className="health-kv"><span>To</span><span>{detail.to_stage || "-"}</span></div>
                <div className="health-kv"><span>Guard</span><span>{compactValue(detail.guard)}</span></div>
                <div className="health-kv"><span>Action</span><span>{compactValue(detail.action)}</span></div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {edge.timer_details?.length ? (
        <div className="node-detail-card">
          <div className="tiny">Timer Details</div>
          <div className="stack graph-card-stack">
            {edge.timer_details.map((detail, index) => (
              <div key={`${detail.id || "timer"}:${index}`} className="health-card graph-inner-health-card">
                <div className="health-kv"><span>ID</span><span className="mono">{detail.id || "-"}</span></div>
                <div className="health-kv"><span>Owner</span><span>{detail.owner || "-"}</span></div>
                <div className="health-kv"><span>Stage</span><span>{detail.stage || "-"}</span></div>
                <div className="health-kv"><span>Action</span><span>{detail.action || "-"}</span></div>
                <div className="health-kv"><span>Delay</span><span>{compactValue([detail.delay_seconds && `${detail.delay_seconds}s`, detail.delay_minutes && `${detail.delay_minutes}m`, detail.delay_hours && `${detail.delay_hours}h`, detail.delay_days && `${detail.delay_days}d`].filter(Boolean))}</span></div>
                <div className="health-kv"><span>Recurring</span><span>{detail.recurring ? "yes" : "no"}</span></div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {renderRawDetails("Raw Edge Data", edge)}
    </>
  );
}

function NodeInspector({
  node,
  selectedEdge,
  selectedRuntime,
  selectedNodeEdges,
  relatedNodes,
  edgeKinds,
  selectionVertical,
  onOpenTrace,
  onInspectAgent,
  onOpenControl,
  onNavigateToTask,
  onSelectNode,
  onRestartAgent,
  onOpenPrompt,
}) {
  if (!node) return null;
  const isSystemNode = node.kind === "system" || node.kind === "human" || node.kind === "mailbox";
  const uniqueRelatedAgents = relatedNodes.filter((item) => item.kind === "agent").map((item) => item.id);
  const connectionRows = selectedNodeEdges.map((edge) => ({
    kind: edge.kind || "-",
    from: edge.from,
    to: edge.to,
    source: edge.source || edge.label || "-",
  }));
  const connectionColumns = [
    {
      accessorKey: "kind",
      header: "Kind",
      cell: ({ row }) => (
        <span className={`badge ${isEventLinkedEdgeKind(row.original.kind) || row.original.kind === "message" || row.original.kind === "mailbox" ? "b-running" : ""}`} style={{ fontSize: 9 }}>
          {row.original.kind}
        </span>
      ),
    },
    {
      accessorKey: "from",
      header: "From",
      cell: ({ row }) => (
        <span
          className={`mono ${row.original.from !== node.id ? "graph-linklike" : ""}`}
          style={{ fontSize: 10 }}
          onClick={() => { if (row.original.from !== node.id) onSelectNode(row.original.from); }}
        >
          {row.original.from === node.id ? "self" : row.original.from}
        </span>
      ),
    },
    {
      accessorKey: "to",
      header: "To",
      cell: ({ row }) => (
        <span
          className={`mono ${row.original.to !== node.id ? "graph-linklike" : ""}`}
          style={{ fontSize: 10 }}
          onClick={() => { if (row.original.to !== node.id) onSelectNode(row.original.to); }}
        >
          {row.original.to === node.id ? "self" : row.original.to}
        </span>
      ),
    },
    {
      accessorKey: "source",
      header: "Source",
      cell: ({ row }) => <span className="tiny">{row.original.source}</span>,
    },
  ];

  return (
    <>
      {selectedEdge ? <EdgeInspector edge={selectedEdge} viewGraphNodes={[node, ...relatedNodes]} onSelectNode={onSelectNode} /> : null}
      <div className="health-card" style={{ marginBottom: 10 }}>
        <div className="tiny">Node Summary</div>
        <div className="health-kv"><span>ID</span><span className="mono">{node.id}</span></div>
        <div className="health-kv"><span>Kind</span><span>{node.kind}</span></div>
        <div className="health-kv"><span>Group</span><span>{node.group || "-"}</span></div>
        <div className="health-kv"><span>Role</span><span>{node.role || "-"}</span></div>
        <div className="health-kv"><span>Vertical</span><span className="mono">{selectionVertical || "-"}</span></div>
        <div className="health-kv"><span>Connections</span><span className="mono">{selectedNodeEdges.length}</span></div>
        {isSystemNode ? <div className="health-kv"><span>Related Agents</span><span className="mono">{uniqueRelatedAgents.length}</span></div> : null}
        {isSystemNode ? <div className="health-kv"><span>Edge Kinds</span><span>{edgeKinds.join(", ") || "-"}</span></div> : null}
        <div className="stack" style={{ marginTop: 8 }}>
          {detailButton("Open Vertical Trace", () => onOpenTrace(selectionVertical, node.id), !selectionVertical)}
          {selectedRuntime ? detailButton("Inspect Agent", onInspectAgent) : null}
          {selectedRuntime ? detailButton("Control", onOpenControl) : null}
        </div>
      </div>

      <div className="node-detail-card">
        <div className="tiny">Identity</div>
        <div className="node-detail-grid">
          <span className="node-detail-label">ID</span><span className="mono graph-link-small">{node.id}</span>
          <span className="node-detail-label">Kind</span><span>{node.kind}</span>
          <span className="node-detail-label">Group</span><span>{node.group || "-"}</span>
          <span className="node-detail-label">Role</span><span>{node.role || "-"}</span>
          {node.mode ? <><span className="node-detail-label">Mode</span><span>{node.mode}</span></> : null}
          {node.status ? <><span className="node-detail-label">Status</span><span>{node.status}</span></> : null}
          {node.vertical_slug ? <><span className="node-detail-label">Vertical</span><span>{node.vertical_slug}</span></> : null}
          {node.parent_id ? <><span className="node-detail-label">Parent</span><span className="mono graph-linklike" onClick={() => onSelectNode(node.parent_id)}>{node.parent_id}</span></> : null}
        </div>
      </div>

      <div className="node-detail-card">
        <div className="tiny">Workflow Pivot</div>
        <div className="stack graph-card-stack">
          {detailButton("Open Vertical Trace", () => onOpenTrace(selectionVertical, node.id), !selectionVertical)}
          {detailButton("Trace Agent", () => onOpenTrace(selectionVertical, node.id), !selectedRuntime)}
        </div>
      </div>

      {isSystemNode ? (
        <div className="node-detail-card">
          <div className="tiny">System Relationships</div>
          {uniqueRelatedAgents.length === 0 ? (
            <div className="empty-state">No directly related agents</div>
          ) : (
            <div className="stack graph-card-stack">
              {uniqueRelatedAgents.slice(0, 12).map((agentID) => (
                <button key={agentID} className="btn-secondary" onClick={() => onSelectNode(agentID)}>
                  {agentID}
                </button>
              ))}
            </div>
          )}
        </div>
      ) : null}

      {selectedRuntime ? (
        <div className="node-detail-card">
          <div className="tiny">Runtime</div>
          <div className="node-detail-grid">
            <span className="node-detail-label">State</span>
            <span><StatusDot state={selectedRuntime.state} />{selectedRuntime.state}</span>
            <span className="node-detail-label">Turns</span>
            <span className="mono">{selectedRuntime.turn_count}/{selectedRuntime.turn_limit}{selectedRuntime.turn_limit > 0 ? ` (${Math.round((selectedRuntime.turn_count / selectedRuntime.turn_limit) * 100)}%)` : ""}</span>
            <span className="node-detail-label">Turns 24h</span>
            <span className="mono">{(selectedRuntime.turns_24h || 0).toLocaleString()}</span>
            <span className="node-detail-label">Tokens 24h</span>
            <span className="mono">{(selectedRuntime.total_tokens_24h || 0).toLocaleString()}</span>
            <span className="node-detail-label">Pending</span>
            <span className={`mono ${(selectedRuntime.pending_events || 0) > 0 ? "health-warn" : ""}`}>{selectedRuntime.pending_events || 0}</span>
            {selectedRuntime.current_task_id ? <><span className="node-detail-label">Task</span><span className="mono graph-linklike" onClick={() => onNavigateToTask(selectedRuntime.current_task_id)}>{selectedRuntime.current_task_id.slice(0, 12)}</span></> : null}
            {selectedRuntime.stuck_reason ? <><span className="node-detail-label">Stuck</span><span style={{ color: "var(--bad)" }}>{selectedRuntime.stuck_reason}</span></> : null}
            {selectedRuntime.started_at ? <><span className="node-detail-label">Started</span><span title={fmtTime(selectedRuntime.started_at)}>{relTime(selectedRuntime.started_at)}</span></> : null}
          </div>
          {selectedRuntime.last_tool?.name ? (
            <div className="graph-inline-detail">
              <span className="node-detail-label">Last Tool</span>
              <span className="mono graph-link-small">{selectedRuntime.last_tool.name}{selectedRuntime.last_tool.ok === false ? " (fail)" : ""}</span>
            </div>
          ) : null}
        </div>
      ) : null}

      {(node.tools || []).length > 0 ? (
        <div className="node-detail-card">
          <div className="tiny">Tools ({(node.tools || []).length})</div>
          {renderTagList((node.tools || []).map((tool) => (typeof tool === "string" ? tool : (tool.name || tool.type || compactValue(tool)))), "No tools attached")}
        </div>
      ) : null}

      {(node.subscriptions || []).length > 0 ? (
        <div className="node-detail-card">
          <div className="tiny">Subscriptions ({(node.subscriptions || []).length})</div>
          {renderTagList((node.subscriptions || []).map((subscription) => (typeof subscription === "string" ? subscription : (subscription.type || subscription.event_type || compactValue(subscription)))), "No subscriptions")}
        </div>
      ) : null}

      {selectedNodeEdges.length > 0 ? (
        <div className="node-detail-card">
          <div className="tiny">Connections ({selectedNodeEdges.length})</div>
          <DataTable
            columns={connectionColumns}
            data={connectionRows}
            initialSorting={[{ id: "kind", desc: false }]}
            className="graph-connections-table"
          />
        </div>
      ) : null}

      {selectedRuntime && node.kind === "agent" ? (
        <div className="node-detail-card">
          <div className="tiny">Quick Actions</div>
          <div className="stack graph-card-stack">
            <button className="btn-secondary" onClick={onRestartAgent}>Restart</button>
            <button className="btn-secondary" onClick={onOpenControl}>Control</button>
            <button className="btn-secondary" onClick={onInspectAgent}>Inspect</button>
          </div>
        </div>
      ) : null}

      {node.system_prompt ? (
        <div className="node-detail-card">
          <div className="tiny">System Prompt</div>
          <div className="health-kv"><span>Length</span><span className="mono">{String(node.system_prompt).length.toLocaleString()} chars</span></div>
          <div className="stack" style={{ marginTop: 8 }}>
            <button className="btn-secondary" onClick={onOpenPrompt}>
              View Prompt YAML
            </button>
          </div>
        </div>
      ) : null}

      {node.constraints && Object.keys(node.constraints).length > 0 ? (
        <div className="node-detail-card">
          <div className="tiny">Constraints</div>
          <div className="node-detail-grid">
            {Object.entries(node.constraints).map(([key, value]) => (
              <React.Fragment key={key}>
                <span className="node-detail-label">{key}</span>
                <span>{compactValue(value)}</span>
              </React.Fragment>
            ))}
          </div>
        </div>
      ) : null}

      {renderRawDetails("Raw Node Data", node)}
    </>
  );
}

export default function GraphInspector({
  graphMode,
  graphVertical,
  graphNodeCount,
  graphEdgeCount,
  selectedNode,
  selectedEdge,
  selectedRuntime,
  selectedNodeEdges,
  relatedNodes,
  edgeKinds,
  selectionVertical,
  viewGraphNodes,
  onOpenTrace,
  onInspectAgent,
  onOpenControl,
  onNavigateToTask,
  onSelectNode,
  onRestartAgent,
  onOpenPrompt,
}) {
  const selectionTitle = selectedEdge ? "Connection Details" : "Node Details";

  return (
    <aside className="graph-workspace-inspector">
      <section>
        <div className="head">
          <h2>{selectionTitle}</h2>
          <div className="tiny mono">{selectedEdge ? "edge selected" : (selectedNode?.id || "none")}</div>
        </div>
        <div className="body scroll">
          <div className="health-card" style={{ marginBottom: 10 }}>
            <div className="tiny">Topology Summary</div>
            <div className="health-kv"><span>Mode</span><span>{graphMode}</span></div>
            <div className="health-kv"><span>Vertical</span><span className="mono">{graphVertical || "-"}</span></div>
            <div className="health-kv"><span>Nodes</span><span className="mono">{graphNodeCount}</span></div>
            <div className="health-kv"><span>Edges</span><span className="mono">{graphEdgeCount}</span></div>
          </div>
          {!selectedNode && !selectedEdge ? (
            <div className="empty-state">Click a node or edge to inspect details.</div>
          ) : selectedNode ? (
            <NodeInspector
              node={selectedNode}
              selectedEdge={selectedEdge}
              selectedRuntime={selectedRuntime}
              selectedNodeEdges={selectedNodeEdges}
              relatedNodes={relatedNodes}
              edgeKinds={edgeKinds}
              selectionVertical={selectionVertical}
              onOpenTrace={onOpenTrace}
              onInspectAgent={onInspectAgent}
              onOpenControl={onOpenControl}
              onNavigateToTask={onNavigateToTask}
              onSelectNode={onSelectNode}
              onRestartAgent={onRestartAgent}
              onOpenPrompt={onOpenPrompt}
            />
          ) : (
            <EdgeInspector edge={selectedEdge} viewGraphNodes={viewGraphNodes} onSelectNode={onSelectNode} />
          )}
        </div>
      </section>
    </aside>
  );
}
