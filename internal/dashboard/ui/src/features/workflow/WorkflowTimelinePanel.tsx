import React, { useMemo } from "react";
import DataTable from "../../components/DataTable.tsx";
import { fmtTime, relTime } from "../../lib/format.ts";
import { findEdgeBySelectionID } from "../graph/graphInspectorUtils.tsx";

export default function WorkflowTimelinePanel(props) {
  const params = props.params || {};
  const flow = params.flow;
  const graph = params.graph;
  const flowState = flow?.state || {};
  const graphState = graph?.state || {};

  const flowGraph = flowState.flowViewGraph || flowState.flowGraph || { nodes: [], edges: [] };
  const graphGraph = graphState.graphViewGraph || graphState.graph || { nodes: [], edges: [] };
  const selectedFlowNodeID = flowState.selectedFlowNodeID || "";
  const selectedGraphNodeID = graphState.selectedGraphNodeID || "";
  const selectedFlowEdge = findEdgeBySelectionID(flowGraph.edges, flowState.selectedFlowEdgeID);
  const selectedGraphEdge = findEdgeBySelectionID(graphGraph.edges, graphState.selectedGraphEdgeID);
  const activeNodeID = selectedFlowNodeID || selectedGraphNodeID || "";
  const activeEdge = selectedFlowEdge || selectedGraphEdge;

  const filteredEvents = useMemo(() => {
    const allEvents = flowState.visibleFlowEvents || flowState.flowEvents || [];
    let rows = [...allEvents];
    if (activeNodeID) {
      rows = rows.filter((row) => row.source_node === activeNodeID || (row.target_nodes || []).includes(activeNodeID));
    } else if (activeEdge?.event_type) {
      rows = rows.filter((row) => row.event_type === activeEdge.event_type);
    }
    return rows;
  }, [activeEdge?.event_type, activeNodeID, flowState.flowEvents, flowState.visibleFlowEvents]);

  const focusLabel = activeNodeID
    ? `node:${activeNodeID}`
    : activeEdge?.event_type
      ? `event:${activeEdge.event_type}`
      : (flowState.flowVertical ? `vertical:${flowState.flowVertical}` : "global");

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

  if (!flow || !graph) return null;

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
          <div className="health-kv"><span>View</span><span>{flowState.flowView}</span></div>
          <div className="health-kv"><span>Vertical</span><span className="mono">{flowState.flowVertical || "-"}</span></div>
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
