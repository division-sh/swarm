import React, { useMemo } from "react";
import DataTable from "../../components/DataTable.tsx";
import { relTime } from "../../lib/format.ts";
import { findEdgeBySelectionID } from "../graph/graphInspectorUtils.tsx";
import { attentionScore, isAttentionAgent, sortAttentionAgents } from "../agents/triage.ts";

function hasConnectedEdges(nodeID, edges) {
  return (edges || []).some((edge) => edge.from === nodeID || edge.to === nodeID);
}

function deriveIssueState(flow, graph) {
  const flowGraph = flow.state.flowViewGraph || flow.state.flowGraph || { nodes: [], edges: [] };
  const graphGraph = graph.state.graphViewGraph || graph.state.graph || { nodes: [], edges: [] };
  const graphNodes = Array.isArray(graphGraph.nodes) ? graphGraph.nodes : [];
  const graphEdges = Array.isArray(graphGraph.edges) ? graphGraph.edges : [];
  const allEvents = Array.isArray(flow.state.visibleFlowEvents) && flow.state.visibleFlowEvents.length > 0
    ? flow.state.visibleFlowEvents
    : (flow.state.flowEvents || []);
  const agents = Array.isArray(flow.state.agents) && flow.state.agents.length > 0
    ? flow.state.agents
    : (graph.state.agents || []);

  const selectedFlowNodeID = flow.state.selectedFlowNodeID || "";
  const selectedGraphNodeID = graph.state.selectedGraphNodeID || "";
  const selectedNodeID = selectedFlowNodeID || selectedGraphNodeID || "";
  const selectedEdge = findEdgeBySelectionID(flowGraph.edges, flow.state.selectedFlowEdgeID)
    || findEdgeBySelectionID(graphGraph.edges, graph.state.selectedGraphEdgeID)
    || null;
  const selectionVertical = flow.state.flowVertical || graph.state.graphVertical || "";

  const attentionAgents = sortAttentionAgents((agents || []).filter(isAttentionAgent));
  const stuckAgents = attentionAgents.filter((agent) => agent.state === "stuck");
  const interceptedEvents = allEvents.filter((event) => !!event.intercepted);
  const passthroughEvents = allEvents.filter((event) => !!event.passthrough);
  const timerEdges = (flowGraph.edges || []).filter((edge) => Array.isArray(edge.timer_ids) && edge.timer_ids.length > 0);
  const disconnectedNodes = graphNodes.filter((node) => !hasConnectedEdges(node.id, graphEdges));

  const scopedEvents = selectedNodeID
    ? allEvents.filter((event) => event.source_node === selectedNodeID || (event.target_nodes || []).includes(selectedNodeID))
    : (selectionVertical ? allEvents.filter((event) => event.vertical_id === selectionVertical) : allEvents);

  const scopedIssueCount = [
    stuckAgents.length,
    interceptedEvents.filter((event) => !selectionVertical || event.vertical_id === selectionVertical).length,
    disconnectedNodes.filter((node) => !selectionVertical || node.vertical_slug === selectionVertical).length,
  ].reduce((sum, value) => sum + value, 0);

  return {
    flowGraph,
    graphGraph,
    selectedNodeID,
    selectedEdge,
    selectionVertical,
    attentionAgents,
    stuckAgents,
    interceptedEvents,
    passthroughEvents,
    timerEdges,
    disconnectedNodes,
    scopedEvents,
    scopedIssueCount,
  };
}

export default function WorkflowIssuesPanel(props) {
  const params = props.params || {};
  const flow = params.flow;
  const graph = params.graph;
  const flowState = flow?.state;
  const graphState = graph?.state;

  const issues = useMemo(
    () => deriveIssueState({ state: flowState || {} }, { state: graphState || {} }),
    [flowState, graphState],
  );

  const focusLabel = issues.selectedNodeID
    ? `node:${issues.selectedNodeID}`
    : issues.selectedEdge?.event_type
      ? `edge:${issues.selectedEdge.event_type}`
      : (issues.selectionVertical ? `vertical:${issues.selectionVertical}` : "workflow");

  const stuckColumns = useMemo(() => ([
    {
      accessorKey: "id",
      header: "Agent",
      cell: ({ row }) => (
        <button className="btn-secondary mono" onClick={() => graph?.actions?.setSelectedGraphNodeID?.(row.original.id)}>
          {row.original.id}
        </button>
      ),
    },
    {
      accessorKey: "state",
      header: "State",
      cell: ({ row }) => <span>{row.original.state || "-"}</span>,
    },
    {
      accessorKey: "pending_events",
      header: "Pending",
      cell: ({ row }) => <span className="mono">{row.original.pending_events || 0}</span>,
    },
    {
      accessorKey: "oldest_pending_age_sec",
      header: "Age",
      cell: ({ row }) => <span className="mono">{row.original.oldest_pending_age_sec ? `${Math.round(row.original.oldest_pending_age_sec / 60)}m` : "-"}</span>,
    },
    {
      accessorKey: "stuck_reason",
      header: "Reason",
      enableSorting: false,
      cell: ({ row }) => <span className="tiny">{row.original.stuck_reason || "-"}</span>,
    },
  ]), [graph]);

  const flowEventColumns = useMemo(() => ([
    {
      accessorKey: "timestamp",
      header: "When",
      cell: ({ row }) => <span title={row.original.timestamp}>{relTime(row.original.timestamp)}</span>,
    },
    {
      accessorKey: "event_type",
      header: "Event",
      cell: ({ row }) => <span className="mono">{row.original.event_type}</span>,
    },
    {
      accessorKey: "source_node",
      header: "Source",
      cell: ({ row }) => (
        <button className="btn-secondary mono" onClick={() => flow?.actions?.setSelectedFlowNodeID?.(row.original.source_node || "")}>
          {row.original.source_node || "-"}
        </button>
      ),
    },
    {
      accessorKey: "vertical_id",
      header: "Vertical",
      cell: ({ row }) => <span className="mono">{row.original.vertical_id || "-"}</span>,
    },
    {
      accessorKey: "flag",
      header: "Flag",
      enableSorting: false,
      cell: ({ row }) => (
        <span className="tiny">
          {row.original.intercepted ? "intercepted" : ""}
          {row.original.intercepted && row.original.passthrough ? " | " : ""}
          {row.original.passthrough ? "passthrough" : ""}
        </span>
      ),
    },
  ]), [flow]);

  const disconnectedColumns = useMemo(() => ([
    {
      accessorKey: "id",
      header: "Node",
      cell: ({ row }) => (
        <button className="btn-secondary mono" onClick={() => graph?.actions?.setSelectedGraphNodeID?.(row.original.id)}>
          {row.original.id}
        </button>
      ),
    },
    {
      accessorKey: "kind",
      header: "Kind",
      cell: ({ row }) => <span>{row.original.kind || "-"}</span>,
    },
    {
      accessorKey: "group",
      header: "Group",
      cell: ({ row }) => <span>{row.original.group || "-"}</span>,
    },
    {
      accessorKey: "vertical_slug",
      header: "Vertical",
      cell: ({ row }) => <span className="mono">{row.original.vertical_slug || "-"}</span>,
    },
  ]), [graph]);

  if (!flow || !graph) return null;

  return (
    <div className="workflow-dock-panel">
      <div className="head">
        <h2>Issues</h2>
        <div className="tiny mono">{focusLabel}</div>
      </div>
      <div className="body scroll">
        <div className="quad-grid" style={{ marginBottom: 10 }}>
          <div className="health-card">
            <div className="tiny">Attention Agents</div>
            <div className="big-number">{issues.attentionAgents.length}</div>
            <div className="tiny">stuck, pending, breaker, failures</div>
          </div>
          <div className="health-card">
            <div className="tiny">Intercepted Events</div>
            <div className="big-number">{issues.interceptedEvents.length}</div>
            <div className="tiny">workflow handoff or blocked transitions</div>
          </div>
          <div className="health-card">
            <div className="tiny">Disconnected Nodes</div>
            <div className="big-number">{issues.disconnectedNodes.length}</div>
            <div className="tiny">topology elements with no edges</div>
          </div>
          <div className="health-card">
            <div className="tiny">Selection Scope</div>
            <div className="big-number">{issues.scopedIssueCount}</div>
            <div className="tiny">{focusLabel}</div>
          </div>
        </div>

        <div className="health-card" style={{ marginBottom: 10 }}>
          <div className="tiny">Workflow Risk Summary</div>
          <div className="health-kv"><span>Focus</span><span className="mono">{focusLabel}</span></div>
          <div className="health-kv"><span>Vertical</span><span className="mono">{issues.selectionVertical || "-"}</span></div>
          <div className="health-kv"><span>Scoped Events</span><span className="mono">{issues.scopedEvents.length}</span></div>
          <div className="health-kv"><span>Passthrough Events</span><span className="mono">{issues.passthroughEvents.length}</span></div>
          <div className="health-kv"><span>Timer-backed Edges</span><span className="mono">{issues.timerEdges.length}</span></div>
          <div className="health-kv"><span>Top Attention Score</span><span className="mono">{issues.attentionAgents[0] ? Math.round(attentionScore(issues.attentionAgents[0])) : 0}</span></div>
        </div>

        <div className="node-detail-card">
          <div className="tiny">Stuck And Attention Agents</div>
          <DataTable
            columns={stuckColumns}
            data={issues.attentionAgents.slice(0, 12)}
            emptyLabel="No workflow-facing agent issues right now."
            initialSorting={[{ id: "pending_events", desc: true }]}
          />
        </div>

        <div className="node-detail-card">
          <div className="tiny">Intercepted And Passthrough Flow Events</div>
          <DataTable
            columns={flowEventColumns}
            data={[...issues.interceptedEvents, ...issues.passthroughEvents.filter((event) => !event.intercepted)].slice(0, 20)}
            emptyLabel="No intercepted or passthrough flow events in the current workflow scope."
            initialSorting={[{ id: "timestamp", desc: true }]}
          />
        </div>

        <div className="node-detail-card">
          <div className="tiny">Disconnected Topology Nodes</div>
          <DataTable
            columns={disconnectedColumns}
            data={issues.disconnectedNodes.slice(0, 20)}
            emptyLabel="No disconnected nodes in the current topology."
            initialSorting={[{ id: "id", desc: false }]}
          />
        </div>
      </div>
    </div>
  );
}
