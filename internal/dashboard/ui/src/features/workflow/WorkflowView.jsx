import React, { useEffect, useState } from "react";
import FlowView from "../flow/FlowView.jsx";
import GraphPage from "../graph/GraphPage.jsx";

function routeToSubview(activeView) {
  if (activeView === "flow" || activeView === "graph") return activeView;
  return "";
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

  useEffect(() => {
    if (!routeSubview) return;
    setSubview(routeSubview);
    if (activeView === "flow" || activeView === "graph") {
      setViewRoute("workflow", routeSubview);
    }
  }, [activeView, routeSubview, setViewRoute]);

  function selectSubview(next) {
    setSubview(next);
    setViewRoute("workflow", next);
  }

  const flowCount = Array.isArray(flow.state.flowEvents) ? flow.state.flowEvents.length : 0;
  const graphCount = Array.isArray(graph.state.graph?.nodes) ? graph.state.graph.nodes.length : 0;

  return (
    <div>
      <div className="head">
        <h2>Workflow</h2>
        <div className="stack">
          <button className={subview === "flow" ? "active" : ""} onClick={() => selectSubview("flow")}>
            Flow
          </button>
          <button className={subview === "graph" ? "active" : ""} onClick={() => selectSubview("graph")}>
            Topology
          </button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Unified workflow design, runtime flow, replay inspection, and org topology. {flowCount} flow events loaded, {graphCount} topology nodes available.
      </div>
      {subview === "flow" ? <FlowView state={flow.state} actions={flow.actions} /> : null}
      {subview === "graph" ? <GraphPage state={graph.state} actions={graph.actions} /> : null}
    </div>
  );
}
