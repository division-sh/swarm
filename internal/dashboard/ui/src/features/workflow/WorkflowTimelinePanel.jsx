import React, { useMemo } from "react";
import DataTable from "../../components/DataTable.jsx";
import { fmtTime, relTime } from "../../lib/format.js";
import { findEdgeBySelectionID } from "../graph/graphInspectorUtils.jsx";

export default function WorkflowTimelinePanel(props) {
  const params = props.params || {};
  const flow = params.flow;
  const graph = params.graph;
  if (!flow || !graph) return null;

  const flowGraph = flow.state.flowViewGraph || flow.state.flowGraph || { nodes: [], edges: [] };
  const graphGraph = graph.state.graphViewGraph || graph.state.graph || { nodes: [], edges: [] };
  const selectedFlowNodeID = flow.state.selectedFlowNodeID || "";
  const selectedGraphNodeID = graph.state.selectedGraphNodeID || "";
  const selectedFlowEdge = findEdgeBySelectionID(flowGraph.edges, flow.state.selectedFlowEdgeID);
  const selectedGraphEdge = findEdgeBySelectionID(graphGraph.edges, graph.state.selectedGraphEdgeID);
  const activeNodeID = selectedFlowNodeID || selectedGraphNodeID || "";
  const activeEdge = selectedFlowEdge || selectedGraphEdge;
  const allEvents = flow.state.visibleFlowEvents || flow.state.flowEvents || [];

  const filteredEvents = useMemo(() => {
    let rows = [...allEvents];
    if (activeNodeID) {
      rows = rows.filter((row) => row.source_node === activeNodeID || (row.target_nodes || []).includes(activeNodeID));
    } else if (activeEdge?.event_type) {
      rows = rows.filter((row) => row.event_type === activeEdge.event_type);
    }
    return rows;
  }, [activeEdge?.event_type, activeNodeID, allEvents]);

  const focusLabel = activeNodeID
    ? `node:${activeNodeID}`
    : activeEdge?.event_type
      ? `event:${activeEdge.event_type}`
      : (flow.state.flowVertical ? `vertical:${flow.state.flowVertical}` : "global");

  const columns = useMemo(() => ([
    {
      accessorKey: "timestamp",
      header: "When",
      cell: ({ row }) => <span title={fmtTime(row.original.timestamp)}>{relTime(row.original.timestamp)}</span>,
    },
    {
      accessorKey: "event_type",
      header: "Type",
      cell: ({ row }) => <span className="mono">{row.original.event_type}</span>,
    },
    {
      accessorKey: "source_node",
      header: "Source",
      cell: ({ row }) => <span className="mono">{row.original.source_node || "-"}</span>,
    },
    {
      accessorKey: "target_nodes",
      header: "Targets",
      cell: ({ row }) => <span className="tiny">{(row.original.target_nodes || []).join(", ") || "-"}</span>,
    },
    {
      accessorKey: "task_id",
      header: "Task",
      cell: ({ row }) => <span className="mono">{row.original.task_id || "-"}</span>,
    },
    {
      accessorKey: "flags",
      header: "Flags",
      enableSorting: false,
      cell: ({ row }) => (
        <span className="tiny">
          {row.original.intercepted ? "intercepted " : ""}
          {row.original.passthrough ? "passthrough" : ""}
          {!row.original.intercepted && !row.original.passthrough ? "-" : ""}
        </span>
      ),
    },
  ]), []);

  return (
    <div className="workflow-dock-panel">
      <div className="head">
        <h2>Timeline</h2>
        <div className="tiny mono">{focusLabel}</div>
      </div>
      <div className="body scroll">
        <div className="health-card" style={{ marginBottom: 10 }}>
          <div className="tiny">Execution Timeline</div>
          <div className="health-kv"><span>Focus</span><span className="mono">{focusLabel}</span></div>
          <div className="health-kv"><span>Rows</span><span className="mono">{filteredEvents.length}</span></div>
          <div className="health-kv"><span>View</span><span>{flow.state.flowView}</span></div>
          <div className="health-kv"><span>Vertical</span><span className="mono">{flow.state.flowVertical || "-"}</span></div>
        </div>
        <DataTable
          columns={columns}
          data={filteredEvents}
          emptyLabel="No timeline rows for the current workflow focus."
          initialSorting={[{ id: "timestamp", desc: true }]}
        />
      </div>
    </div>
  );
}
