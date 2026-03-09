import { DockviewReact, themeAbyssSpaced } from "dockview";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import FlowView from "../flow/FlowView.tsx";
import GraphPage from "../graph/GraphPage.tsx";
import { deriveWorkflowFocus } from "./focus.ts";
import WorkflowArtifactsPanel from "./WorkflowArtifactsPanel.tsx";
import WorkflowComparePanel from "./WorkflowComparePanel.tsx";
import WorkflowIssuesPanel from "./WorkflowIssuesPanel.tsx";
import WorkflowRunsPanel from "./WorkflowRunsPanel.tsx";
import WorkflowTimelinePanel from "./WorkflowTimelinePanel.tsx";

function routeToSubview(activeView) {
  if (activeView === "flow" || activeView === "graph" || activeView === "timeline" || activeView === "artifacts" || activeView === "issues" || activeView === "compare" || activeView === "runs") return activeView;
  return "";
}

function FlowDockPanel(props) {
  const params = props.params || {};
  const flow = params.flow;
  if (!flow) return null;
  return (
    <div className="workflow-dock-panel">
      <FlowView
        state={flow.state}
        actions={{
          ...flow.actions,
          openTopologyForVertical: params.openTopologyForVertical,
        }}
      />
    </div>
  );
}

function GraphDockPanel(props) {
  const params = props.params || {};
  const graph = params.graph;
  if (!graph) return null;
  return (
    <div className="workflow-dock-panel">
      <GraphPage
        state={graph.state}
        actions={{
          ...graph.actions,
          openFlowForVertical: params.openFlowForVertical,
        }}
      />
    </div>
  );
}

function TimelineDockPanel(props) {
  return <WorkflowTimelinePanel {...props} />;
}

function ArtifactsDockPanel(props) {
  return <WorkflowArtifactsPanel {...props} />;
}

function IssuesDockPanel(props) {
  return <WorkflowIssuesPanel {...props} />;
}

function CompareDockPanel(props) {
  return <WorkflowComparePanel {...props} />;
}

function RunsDockPanel(props) {
  return <WorkflowRunsPanel {...props} />;
}

export default function WorkflowView({
  activeView,
  activeSubview,
  setViewRoute,
  flow,
  graph,
}) {
  const routeSubview = routeToSubview(activeView) || activeSubview;
  const [subview, setSubview] = useState(routeSubview || "flow");
  const dockApiRef = useRef(null);
  const dockInitRef = useRef(false);
  const dockDisposerRef = useRef(null);

  const selectSubview = useCallback((next) => {
    setSubview(next);
    setViewRoute("workflow", next);
    const panel = dockApiRef.current?.getPanel(next);
    panel?.api?.setActive();
  }, [setViewRoute]);

  const flowCount = Array.isArray(flow.state.flowEvents) ? flow.state.flowEvents.length : 0;
  const graphCount = Array.isArray(graph.state.graph?.nodes) ? graph.state.graph.nodes.length : 0;
  const focus = deriveWorkflowFocus({ flow, graph, subview });

  function resetFocus() {
    flow.actions.setFlowStage("all");
    flow.actions.setFlowRubric("all");
    flow.actions.setFlowVertical("");
    flow.actions.setSelectedFlowNodeID("");
    flow.actions.setSelectedFlowEdgeID("");
    graph.actions.setSelectedGraphNodeID("");
    graph.actions.setSelectedGraphEdgeID("");
    graph.actions.setGraphMode("holding");
    graph.actions.setGraphVertical("");
  }

  function openTopologyForVertical(vertical) {
    const value = String(vertical || "").trim();
    if (!value) return;
    graph.actions.setGraphMode("opco");
    graph.actions.setGraphVertical(value);
    graph.actions.setSelectedGraphNodeID("");
    graph.actions.setSelectedGraphEdgeID("");
    selectSubview("graph");
  }

  const openFlowForVertical = useCallback((vertical, nodeID) => {
    const value = String(vertical || "").trim();
    flow.actions.setFlowView("runtime");
    flow.actions.setFlowVertical(value);
    flow.actions.setSelectedFlowEdgeID("");
    flow.actions.setSelectedFlowNodeID(String(nodeID || "").trim());
    selectSubview("flow");
  }, [flow.actions, selectSubview]);

  const openTopologyForVerticalCb = useCallback((vertical) => {
    const value = String(vertical || "").trim();
    if (!value) return;
    graph.actions.setGraphMode("opco");
    graph.actions.setGraphVertical(value);
    graph.actions.setSelectedGraphNodeID("");
    graph.actions.setSelectedGraphEdgeID("");
    selectSubview("graph");
  }, [graph.actions, selectSubview]);

  const dockComponents = useMemo(() => ({
    flow: FlowDockPanel,
    graph: GraphDockPanel,
    timeline: TimelineDockPanel,
    artifacts: ArtifactsDockPanel,
    issues: IssuesDockPanel,
    compare: CompareDockPanel,
    runs: RunsDockPanel,
  }), []);

  const dockParams = useMemo(() => ({
    flow,
    graph,
    openTopologyForVertical: openTopologyForVerticalCb,
    openFlowForVertical,
  }), [flow, graph, openFlowForVertical, openTopologyForVerticalCb]);

  useEffect(() => {
    if (!routeSubview) return;
    setSubview(routeSubview);
    if (activeView === "flow" || activeView === "graph") {
      setViewRoute("workflow", routeSubview);
    }
    const panel = dockApiRef.current?.getPanel(routeSubview);
    panel?.api?.setActive();
  }, [activeView, routeSubview, setViewRoute]);

  useEffect(() => {
    const api = dockApiRef.current;
    if (!api) return;
    ["flow", "graph", "timeline", "artifacts", "issues", "compare", "runs"].forEach((panelID) => {
      const panel = api.getPanel(panelID);
      panel?.api?.updateParameters(dockParams);
    });
  }, [dockParams]);

  useEffect(() => () => {
    dockDisposerRef.current?.dispose?.();
  }, []);

  const handleReady = useCallback((event) => {
    const api = event.api;
    dockApiRef.current = api;
    if (!dockInitRef.current) {
      dockInitRef.current = true;
      const graphPanel = api.addPanel({
        id: "graph",
        component: "graph",
        title: "Topology",
        params: dockParams,
      });
      api.addPanel({
        id: "runs",
        component: "runs",
        title: "Runs",
        params: dockParams,
        position: {
          referencePanel: graphPanel,
          direction: "left",
        },
      });
      const flowPanel = api.addPanel({
        id: "flow",
        component: "flow",
        title: "Trace",
        params: dockParams,
        position: {
          referencePanel: graphPanel,
          direction: "right",
        },
      });
      api.addPanel({
        id: "timeline",
        component: "timeline",
        title: "Timeline",
        params: dockParams,
        position: {
          referencePanel: graphPanel,
          direction: "below",
        },
      });
      api.addPanel({
        id: "artifacts",
        component: "artifacts",
        title: "Artifacts",
        params: dockParams,
        position: {
          referencePanel: flowPanel,
          direction: "below",
        },
      });
      api.addPanel({
        id: "issues",
        component: "issues",
        title: "Issues",
        params: dockParams,
        position: {
          referencePanel: api.getPanel("timeline"),
          direction: "within",
        },
      });
      api.addPanel({
        id: "compare",
        component: "compare",
        title: "Compare",
        params: dockParams,
        position: {
          referencePanel: api.getPanel("artifacts"),
          direction: "within",
        },
      });
    }
    dockDisposerRef.current?.dispose?.();
    dockDisposerRef.current = api.onDidActivePanelChange((panel) => {
      const next = panel?.id || "flow";
      setSubview(next);
      setViewRoute("workflow", next);
    });
    const activeTarget = routeSubview || subview || "flow";
    api.getPanel(activeTarget)?.api?.setActive();
  }, [dockParams, routeSubview, setViewRoute, subview]);

  return (
    <div>
      <div className="head">
        <h2>Workflow</h2>
        <div className="stack" data-testid="workflow-subview-nav">
          <button className={subview === "flow" ? "active" : ""} onClick={() => selectSubview("flow")}>
            Trace
          </button>
          <button className={subview === "graph" ? "active" : ""} onClick={() => selectSubview("graph")}>
            Topology
          </button>
          <button className={subview === "timeline" ? "active" : ""} onClick={() => selectSubview("timeline")}>
            Timeline
          </button>
          <button className={subview === "artifacts" ? "active" : ""} onClick={() => selectSubview("artifacts")}>
            Artifacts
          </button>
          <button className={subview === "issues" ? "active" : ""} onClick={() => selectSubview("issues")}>
            Issues
          </button>
          <button className={subview === "compare" ? "active" : ""} onClick={() => selectSubview("compare")}>
            Compare
          </button>
          <button className={subview === "runs" ? "active" : ""} onClick={() => selectSubview("runs")}>
            Runs
          </button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Unified workflow design, runtime trace, replay inspection, and topology. {flowCount} flow events loaded, {graphCount} topology nodes available.
      </div>
      <div className="health-card" style={{ marginBottom: 10 }}>
        <div className="stack" style={{ justifyContent: "space-between", marginBottom: 8 }}>
          <div>
            <div className="tiny">Workflow Focus</div>
            <div>{focus.chips.length > 0 ? focus.chips.join(" | ") : "No active workflow focus"}</div>
          </div>
          <div className="stack">
            {focus.vertical ? (
              <>
                <button className="btn-secondary" onClick={() => openFlowForVertical(focus.vertical, focus.selectedFlowNodeID)}>Trace Vertical</button>
                <button className="btn-secondary" onClick={() => openTopologyForVertical(focus.vertical)}>Topology View</button>
              </>
            ) : null}
            <button className="btn-secondary" onClick={() => { flow.actions.setFlowView("runtime"); selectSubview("flow"); }}>Runtime Trace</button>
            <button className="btn-secondary" onClick={() => { graph.actions.setGraphMode("holding"); selectSubview("graph"); }}>Holding Topology</button>
            <button className="btn-secondary" onClick={resetFocus}>Reset Focus</button>
          </div>
        </div>
        <div className="stack tiny">
          <span>Trace Mode: <span className="mono">{focus.flowView}</span></span>
          <span>Topology Mode: <span className="mono">{focus.graphMode}</span></span>
          <span>Vertical: <span className="mono">{focus.vertical || "-"}</span></span>
          <span>Stage: <span className="mono">{focus.stage || "all"}</span></span>
          <span>Rubric: <span className="mono">{focus.rubric || "all"}</span></span>
        </div>
      </div>
      <section className="workflow-workbench-shell">
        <div className="head">
          <h2>Workbench</h2>
          <div className="tiny mono">{subview} active</div>
        </div>
        <div className="body workflow-workbench-body">
          <DockviewReact
            className="workflow-dockview"
            theme={themeAbyssSpaced}
            components={dockComponents}
            onReady={handleReady}
            disableFloatingGroups
            dndEdges={false}
            tabComponents={{}}
          />
        </div>
      </section>
    </div>
  );
}
