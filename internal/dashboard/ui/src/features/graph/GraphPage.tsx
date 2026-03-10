import React, { useMemo, useState } from "react";
import CodeViewer from "../../components/CodeViewer.tsx";
import Modal from "../../components/Modal.tsx";
import GraphContextDrawer from "./GraphContextDrawer.tsx";
import GraphInspector from "./GraphInspector.tsx";
import { findEdgeBySelectionID, toYamlBlock } from "./graphInspectorUtils.tsx";
import GraphView from "./GraphView.tsx";
import GraphWorkspaceSidebar from "./GraphWorkspaceSidebar.tsx";

export default function GraphPage({ state, actions }) {
  const [promptModal, setPromptModal] = useState(null);
  const {
    verticals,
    graph,
    graphViewGraph,
    agents,
    graphMode,
    graphVertical,
    selectedGraphNodeID,
    selectedGraphEdgeID,
    graphFullscreen,
  } = state;
  const {
    setGraphMode,
    setGraphVertical,
    setSelectedGraphNodeID,
    setSelectedGraphEdgeID,
    setGraphFullscreen,
    refreshGraph,
    setGraphViewGraph,
    restartAgent,
    openControl,
    inspectAgent,
    navigateToTask,
  } = actions;

  const viewGraph = graphViewGraph || graph || { nodes: [], edges: [] };
  const graphNodeCount = (graph?.nodes || []).length;
  const graphEdgeCount = (graph?.edges || []).length;

  const selectedEdge = useMemo(
    () => findEdgeBySelectionID(viewGraph.edges, selectedGraphEdgeID),
    [selectedGraphEdgeID, viewGraph.edges],
  );
  const selectedNode = useMemo(
    () => (viewGraph.nodes || []).find((node) => node.id === selectedGraphNodeID) || null,
    [selectedGraphNodeID, viewGraph.nodes],
  );
  const selectedRuntime = useMemo(
    () => (selectedNode?.kind === "agent" ? (agents || []).find((agent) => agent.id === selectedNode.id) || null : null),
    [agents, selectedNode],
  );
  const selectedNodeEdges = useMemo(
    () => (selectedNode ? (viewGraph.edges || []).filter((edge) => edge.from === selectedNode.id || edge.to === selectedNode.id) : []),
    [selectedNode, viewGraph.edges],
  );
  const selectionVertical = selectedNode?.vertical_slug || (selectedRuntime && (selectedRuntime.vertical_slug || selectedRuntime.vertical_id)) || "";
  const relatedNodeIDs = useMemo(() => {
    if (!selectedNode) return [];
    return Array.from(new Set(selectedNodeEdges.map((edge) => (edge.from === selectedNode.id ? edge.to : edge.from)).filter(Boolean)));
  }, [selectedNode, selectedNodeEdges]);
  const relatedNodes = useMemo(() => {
    if (!selectedNode) return [];
    return relatedNodeIDs
      .map((id) => (viewGraph.nodes || []).find((node) => node.id === id))
      .filter(Boolean);
  }, [relatedNodeIDs, selectedNode, viewGraph.nodes]);
  const incomingEdges = useMemo(
    () => selectedNodeEdges.filter((edge) => edge.to === selectedNode?.id),
    [selectedNode?.id, selectedNodeEdges],
  );
  const outgoingEdges = useMemo(
    () => selectedNodeEdges.filter((edge) => edge.from === selectedNode?.id),
    [selectedNode?.id, selectedNodeEdges],
  );
  const edgeKinds = useMemo(
    () => Array.from(new Set(selectedNodeEdges.map((item) => item.kind).filter(Boolean))).sort(),
    [selectedNodeEdges],
  );
  const selectedEdgeContract = useMemo(() => (
    selectedEdge ? {
      stages: selectedEdge.stages || [],
      transitions: selectedEdge.transition_ids || [],
      timers: selectedEdge.timer_ids || [],
      required: selectedEdge.schema_required || [],
      properties: selectedEdge.schema_properties || [],
    } : null
  ), [selectedEdge]);
  const promptYaml = useMemo(
    () => (promptModal?.prompt ? toYamlBlock("system_prompt", promptModal.prompt) : ""),
    [promptModal],
  );

  function openVerticalTrace(vertical, nodeID) {
    actions.openFlowForVertical?.(vertical, nodeID);
  }

  function clearSelection() {
    setSelectedGraphNodeID("");
    setSelectedGraphEdgeID("");
  }

  function selectGraphEdge(edgeID) {
    setSelectedGraphNodeID("");
    setSelectedGraphEdgeID(edgeID);
  }

  function changeMode(nextMode) {
    clearSelection();
    setGraphMode(nextMode);
  }

  function changeVertical(nextVertical) {
    clearSelection();
    setGraphVertical(nextVertical);
  }

  function restartSelectedAgent() {
    if (!selectedNode) return;
    if (!window.confirm(`Restart agent "${selectedNode.id}"?`)) return;
    restartAgent(selectedNode.id).catch(() => {});
  }

  return (
    <div className="graph-workspace">
      <GraphWorkspaceSidebar
        verticals={verticals}
        graphMode={graphMode}
        graphVertical={graphVertical}
        graphNodeCount={graphNodeCount}
        graphEdgeCount={graphEdgeCount}
        selectedNode={selectedNode}
        selectedEdge={selectedEdge}
        selectionVertical={selectionVertical}
        incomingEdges={incomingEdges}
        outgoingEdges={outgoingEdges}
        relatedNodes={relatedNodes}
        selectedRuntime={selectedRuntime}
        onChangeMode={changeMode}
        onChangeVertical={changeVertical}
        onRefresh={() => { refreshGraph().catch(() => {}); }}
        onOpenTrace={openVerticalTrace}
        onClearSelection={clearSelection}
        onGoToParent={() => setSelectedGraphNodeID(selectedNode?.parent_id || "")}
        onInspectAgent={() => selectedNode && inspectAgent(selectedNode.id)}
      />

      <div className="graph-workspace-main">
        <section>
          <div className="head">
            <h2>Workflow Topology</h2>
            <div className="tiny mono">{graphMode}:{graphVertical || "global"}</div>
          </div>
          <div className="body">
            <div className="graph-canvas-summary">
              <div>
                <div className="tiny">Canvas</div>
                <div className="graph-sidebar-title">Topology map with focus, saved views, overlays, and keyboard navigation</div>
              </div>
              <div className="graph-inline-actions">
                <span className="tiny">Bootstrap routes are solid, seeded routes dashed, discovered routes dotted.</span>
              </div>
            </div>
            <GraphView
              graph={graph}
              graphKey={`${graphMode}:${graphMode === "opco" ? graphVertical : ""}:${(graph && graph.template_version) || ""}`}
              mode={graphMode}
              selectedNodeID={selectedGraphNodeID}
              selectedEdgeID={selectedGraphEdgeID}
              onSelectNode={setSelectedGraphNodeID}
              onSelectEdge={selectGraphEdge}
              onDerivedGraph={setGraphViewGraph}
              runtimeAgents={agents}
              isFullscreen={graphFullscreen}
              onToggleFullscreen={() => setGraphFullscreen((previous) => !previous)}
              activeEdgeKeys={new Set()}
            />
          </div>
        </section>

        <GraphContextDrawer
          selectedNode={selectedNode}
          selectedEdge={selectedEdge}
          selectedRuntime={selectedRuntime}
          selectedEdgeContract={selectedEdgeContract}
          incomingEdges={incomingEdges}
          outgoingEdges={outgoingEdges}
          relatedNodes={relatedNodes}
          edgeKinds={edgeKinds}
          viewGraphEdges={viewGraph.edges || []}
          onSelectNode={setSelectedGraphNodeID}
          onSelectEdge={selectGraphEdge}
        />
      </div>

      <GraphInspector
        graphMode={graphMode}
        graphVertical={graphVertical}
        graphNodeCount={graphNodeCount}
        graphEdgeCount={graphEdgeCount}
        selectedNode={selectedNode}
        selectedEdge={selectedEdge}
        selectedRuntime={selectedRuntime}
        selectedNodeEdges={selectedNodeEdges}
        relatedNodes={relatedNodes}
        edgeKinds={edgeKinds}
        selectionVertical={selectionVertical}
        viewGraphNodes={viewGraph.nodes || []}
        onOpenTrace={openVerticalTrace}
        onInspectAgent={() => selectedNode && inspectAgent(selectedNode.id)}
        onOpenControl={() => selectedNode && openControl(selectedNode.id)}
        onNavigateToTask={navigateToTask}
        onSelectNode={setSelectedGraphNodeID}
        onRestartAgent={restartSelectedAgent}
        onOpenPrompt={() => selectedNode?.system_prompt && setPromptModal({ nodeID: selectedNode.id, prompt: selectedNode.system_prompt })}
      />

      {promptModal ? (
        <Modal
          title={`System Prompt YAML — ${promptModal.nodeID}`}
          onClose={() => setPromptModal(null)}
          copyText={promptYaml}
          className="modal-wide"
        >
          <CodeViewer
            language="yaml"
            value={promptYaml}
            height="70vh"
          />
        </Modal>
      ) : null}
    </div>
  );
}
