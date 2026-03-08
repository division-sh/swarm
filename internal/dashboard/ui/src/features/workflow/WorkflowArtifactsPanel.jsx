import React, { useMemo } from "react";
import CodeViewer from "../../components/CodeViewer.jsx";
import { findEdgeBySelectionID, toYamlBlock } from "../graph/graphInspectorUtils.jsx";

export default function WorkflowArtifactsPanel(props) {
  const params = props.params || {};
  const flow = params.flow;
  const graph = params.graph;
  if (!flow || !graph) return null;

  const flowGraph = flow.state.flowViewGraph || flow.state.flowGraph || { nodes: [], edges: [] };
  const graphGraph = graph.state.graphViewGraph || graph.state.graph || { nodes: [], edges: [] };
  const selectedFlowNode = (flowGraph.nodes || []).find((node) => node.id === flow.state.selectedFlowNodeID) || null;
  const selectedGraphNode = (graphGraph.nodes || []).find((node) => node.id === graph.state.selectedGraphNodeID) || null;
  const selectedFlowEdge = findEdgeBySelectionID(flowGraph.edges, flow.state.selectedFlowEdgeID);
  const selectedGraphEdge = findEdgeBySelectionID(graphGraph.edges, graph.state.selectedGraphEdgeID);
  const activeNode = selectedGraphNode || selectedFlowNode;
  const activeEdge = selectedGraphEdge || selectedFlowEdge;
  const promptYaml = activeNode?.system_prompt ? toYamlBlock("system_prompt", activeNode.system_prompt) : "";

  const metadataArtifact = useMemo(() => JSON.stringify({
    workflow_name: flow.state.flowGraphMeta?.workflow_name || "",
    workflow_version: flow.state.flowGraphMeta?.workflow_version || "",
    platform_version: flow.state.flowGraphMeta?.platform_version || "",
    workflow_stages: flow.state.flowGraphMeta?.workflow_stages || [],
    timer_events: flow.state.flowGraphMeta?.timer_events || [],
    sources: flow.state.flowGraphMeta?.sources || [],
  }, null, 2), [flow.state.flowGraphMeta]);

  return (
    <div className="workflow-dock-panel">
      <div className="head">
        <h2>Artifacts</h2>
        <div className="tiny mono">{activeNode?.id || activeEdge?.event_type || "workflow"}</div>
      </div>
      <div className="body scroll">
        <div className="health-card" style={{ marginBottom: 10 }}>
          <div className="tiny">Artifact Context</div>
          <div className="health-kv"><span>Selection</span><span className="mono">{activeNode?.id || activeEdge?.event_type || activeEdge?.kind || "-"}</span></div>
          <div className="health-kv"><span>Trace View</span><span>{flow.state.flowView}</span></div>
          <div className="health-kv"><span>Topology Mode</span><span>{graph.state.graphMode}</span></div>
          <div className="health-kv"><span>Vertical</span><span className="mono">{flow.state.flowVertical || graph.state.graphVertical || "-"}</span></div>
        </div>

        <div className="node-detail-card">
          <div className="tiny">Workflow Metadata</div>
          <CodeViewer language="json" value={metadataArtifact} height={220} compact />
        </div>

        {promptYaml ? (
          <div className="node-detail-card">
            <div className="tiny">System Prompt YAML</div>
            <CodeViewer language="yaml" value={promptYaml} height={260} compact />
          </div>
        ) : null}

        {activeEdge ? (
          <div className="node-detail-card">
            <div className="tiny">Selected Edge Artifact</div>
            <CodeViewer language="json" value={JSON.stringify(activeEdge, null, 2)} height={300} compact />
          </div>
        ) : null}

        {activeNode ? (
          <div className="node-detail-card">
            <div className="tiny">Selected Node Artifact</div>
            <CodeViewer language="json" value={JSON.stringify(activeNode, null, 2)} height={300} compact />
          </div>
        ) : null}

        {!activeNode && !activeEdge ? (
          <div className="empty-state">Select a workflow node or connection to inspect prompt/config artifacts.</div>
        ) : null}
      </div>
    </div>
  );
}
