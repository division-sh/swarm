import React, { useMemo } from "react";
import CodeViewer, { DiffCodeViewer } from "../../components/CodeViewer.jsx";
import { findEdgeBySelectionID, toYamlBlock } from "../graph/graphInspectorUtils.jsx";

function jsonArtifact(value) {
  return JSON.stringify(value || {}, null, 2);
}

function compareNodeID(flow, graph) {
  return flow.state.selectedFlowNodeID || graph.state.selectedGraphNodeID || "";
}

function compactNodeSnapshot(node) {
  if (!node) return {};
  return {
    id: node.id,
    kind: node.kind,
    group: node.group,
    role: node.role,
    status: node.status,
    mode: node.mode,
    vertical_slug: node.vertical_slug,
    parent_id: node.parent_id,
    has_system_prompt: !!node.system_prompt,
  };
}

function compactEdgeSnapshot(edge) {
  if (!edge) return {};
  return {
    kind: edge.kind,
    from: edge.from,
    to: edge.to,
    event_type: edge.event_type,
    status: edge.status,
    reason: edge.reason,
    stages: edge.stages || [],
    transition_ids: edge.transition_ids || [],
    timer_ids: edge.timer_ids || [],
  };
}

export default function WorkflowComparePanel(props) {
  const params = props.params || {};
  const flow = params.flow;
  const graph = params.graph;
  if (!flow || !graph) return null;

  const comparison = useMemo(() => {
    const flowGraph = flow.state.flowViewGraph || flow.state.flowGraph || { nodes: [], edges: [] };
    const graphGraph = graph.state.graphViewGraph || graph.state.graph || { nodes: [], edges: [] };
    const nodeID = compareNodeID(flow, graph);
    const flowNode = (flowGraph.nodes || []).find((node) => node.id === nodeID) || null;
    const graphNode = (graphGraph.nodes || []).find((node) => node.id === nodeID) || null;
    const flowEdge = findEdgeBySelectionID(flowGraph.edges, flow.state.selectedFlowEdgeID);
    const graphEdge = findEdgeBySelectionID(graphGraph.edges, graph.state.selectedGraphEdgeID);
    const activeEdge = flowEdge || graphEdge;
    const selectedVertical = flow.state.flowVertical || graph.state.graphVertical || "";
    return {
      flowGraph,
      graphGraph,
      nodeID,
      flowNode,
      graphNode,
      flowEdge,
      graphEdge,
      activeEdge,
      selectedVertical,
      flowNodeArtifact: jsonArtifact(compactNodeSnapshot(flowNode)),
      graphNodeArtifact: jsonArtifact(compactNodeSnapshot(graphNode)),
      flowEdgeArtifact: jsonArtifact(compactEdgeSnapshot(flowEdge)),
      graphEdgeArtifact: jsonArtifact(compactEdgeSnapshot(graphEdge)),
      flowPromptYaml: flowNode?.system_prompt ? toYamlBlock("system_prompt", flowNode.system_prompt) : "",
      graphPromptYaml: graphNode?.system_prompt ? toYamlBlock("system_prompt", graphNode.system_prompt) : "",
      workflowMeta: jsonArtifact({
        trace_view: flow.state.flowView,
        topology_mode: graph.state.graphMode,
        workflow_name: flow.state.flowGraphMeta?.workflow_name || "",
        workflow_version: flow.state.flowGraphMeta?.workflow_version || "",
        platform_version: flow.state.flowGraphMeta?.platform_version || "",
        vertical: selectedVertical,
        trace_nodes: (flowGraph.nodes || []).length,
        trace_edges: (flowGraph.edges || []).length,
        topology_nodes: (graphGraph.nodes || []).length,
        topology_edges: (graphGraph.edges || []).length,
      }),
    };
  }, [flow, graph]);

  const focusLabel = comparison.nodeID
    ? `node:${comparison.nodeID}`
    : comparison.activeEdge?.event_type
      ? `edge:${comparison.activeEdge.event_type}`
      : (comparison.selectedVertical ? `vertical:${comparison.selectedVertical}` : "workflow");

  return (
    <div className="workflow-dock-panel">
      <div className="head">
        <h2>Compare</h2>
        <div className="tiny mono">{focusLabel}</div>
      </div>
      <div className="body scroll">
        <div className="quad-grid" style={{ marginBottom: 10 }}>
          <div className="health-card">
            <div className="tiny">Trace Graph</div>
            <div className="big-number">{(comparison.flowGraph.nodes || []).length}</div>
            <div className="tiny">{(comparison.flowGraph.edges || []).length} edges</div>
          </div>
          <div className="health-card">
            <div className="tiny">Topology Graph</div>
            <div className="big-number">{(comparison.graphGraph.nodes || []).length}</div>
            <div className="tiny">{(comparison.graphGraph.edges || []).length} edges</div>
          </div>
          <div className="health-card">
            <div className="tiny">Node Presence</div>
            <div className="big-number">{comparison.nodeID ? `${comparison.flowNode ? 1 : 0}/${comparison.graphNode ? 1 : 0}` : "-"}</div>
            <div className="tiny">trace / topology</div>
          </div>
          <div className="health-card">
            <div className="tiny">Edge Presence</div>
            <div className="big-number">{comparison.flowEdge || comparison.graphEdge ? `${comparison.flowEdge ? 1 : 0}/${comparison.graphEdge ? 1 : 0}` : "-"}</div>
            <div className="tiny">trace / topology</div>
          </div>
        </div>

        <div className="health-card" style={{ marginBottom: 10 }}>
          <div className="tiny">Comparison Context</div>
          <div className="health-kv"><span>Focus</span><span className="mono">{focusLabel}</span></div>
          <div className="health-kv"><span>Vertical</span><span className="mono">{comparison.selectedVertical || "-"}</span></div>
          <div className="health-kv"><span>Trace View</span><span>{flow.state.flowView}</span></div>
          <div className="health-kv"><span>Topology Mode</span><span>{graph.state.graphMode}</span></div>
        </div>

        <div className="node-detail-card">
          <div className="tiny">Workflow Scope</div>
          <CodeViewer language="json" value={comparison.workflowMeta} height={220} compact />
        </div>

        {comparison.nodeID ? (
          <div className="node-detail-card">
            <div className="tiny">Selected Node Diff</div>
            <DiffCodeViewer
              language="json"
              original={comparison.flowNodeArtifact}
              modified={comparison.graphNodeArtifact}
              height={320}
              compact
            />
          </div>
        ) : null}

        {(comparison.flowEdge || comparison.graphEdge) ? (
          <div className="node-detail-card">
            <div className="tiny">Selected Edge Diff</div>
            <DiffCodeViewer
              language="json"
              original={comparison.flowEdgeArtifact}
              modified={comparison.graphEdgeArtifact}
              height={320}
              compact
            />
          </div>
        ) : null}

        {(comparison.flowPromptYaml || comparison.graphPromptYaml) ? (
          <div className="node-detail-card">
            <div className="tiny">Prompt YAML Diff</div>
            <DiffCodeViewer
              language="yaml"
              original={comparison.flowPromptYaml}
              modified={comparison.graphPromptYaml}
              height={320}
              compact
            />
          </div>
        ) : null}

        {!comparison.nodeID && !comparison.flowEdge && !comparison.graphEdge ? (
          <div className="empty-state">
            Select a workflow node or connection to compare trace and topology representations.
          </div>
        ) : null}
      </div>
    </div>
  );
}
