import React, { useMemo } from "react";
import CodeViewer from "../../components/CodeViewer.tsx";
import { findEdgeBySelectionID, toYamlBlock } from "../graph/graphInspectorUtils.tsx";

export default function WorkflowArtifactsPanel(props) {
  const params = props.params || {};
  const flow = params.flow;
  const graph = params.graph;
  const flowState = flow?.state || {};
  const graphState = graph?.state || {};

  const flowGraph = flowState.flowViewGraph || flowState.flowGraph || { nodes: [], edges: [] };
  const graphGraph = graphState.graphViewGraph || graphState.graph || { nodes: [], edges: [] };
  const selectedFlowNode = (flowGraph.nodes || []).find((node) => node.id === flowState.selectedFlowNodeID) || null;
  const selectedGraphNode = (graphGraph.nodes || []).find((node) => node.id === graphState.selectedGraphNodeID) || null;
  const selectedFlowEdge = findEdgeBySelectionID(flowGraph.edges, flowState.selectedFlowEdgeID);
  const selectedGraphEdge = findEdgeBySelectionID(graphGraph.edges, graphState.selectedGraphEdgeID);
  const activeNode = selectedGraphNode || selectedFlowNode;
  const activeEdge = selectedGraphEdge || selectedFlowEdge;
  const promptYaml = activeNode?.system_prompt ? toYamlBlock("system_prompt", activeNode.system_prompt) : "";

  const metadataArtifact = useMemo(() => JSON.stringify({
    workflow_name: flowState.flowGraphMeta?.workflow_name || "",
    workflow_version: flowState.flowGraphMeta?.workflow_version || "",
    platform_version: flowState.flowGraphMeta?.platform_version || "",
    workflow_stages: flowState.flowGraphMeta?.workflow_stages || [],
    timer_events: flowState.flowGraphMeta?.timer_events || [],
    sources: flowState.flowGraphMeta?.sources || [],
  }, null, 2), [flowState.flowGraphMeta]);

  if (!flow || !graph) return null;

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
          <div className="health-kv"><span>Trace View</span><span>{flowState.flowView}</span></div>
          <div className="health-kv"><span>Topology Mode</span><span>{graphState.graphMode}</span></div>
          <div className="health-kv"><span>Vertical</span><span className="mono">{flowState.flowVertical || graphState.graphVertical || "-"}</span></div>
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
