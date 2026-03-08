import React, { useEffect, useState } from "react";
import FlowView from "../flow/FlowView.jsx";
import GraphPage from "../graph/GraphPage.jsx";

function routeToSubview(activeView) {
  if (activeView === "flow" || activeView === "graph") return activeView;
  return "";
}

export default function WorkflowView({
  activeView,
  setActiveView,
  flow,
  graph,
}) {
  const routeSubview = routeToSubview(activeView);
  const [subview, setSubview] = useState(routeSubview || "flow");

  useEffect(() => {
    if (!routeSubview) return;
    setSubview(routeSubview);
    if (activeView !== "workflow") {
      setActiveView("workflow");
    }
  }, [activeView, routeSubview, setActiveView]);

  function selectSubview(next) {
    setSubview(next);
    if (activeView !== "workflow") {
      setActiveView("workflow");
    }
  }

  const flowCount = Array.isArray(flow.state.flowEvents) ? flow.state.flowEvents.length : 0;
  const graphCount = Array.isArray(graph.state.graph?.nodes) ? graph.state.graph.nodes.length : 0;

  return (
    <div>
      <div className="head">
        <h2>Workflow</h2>
        <div className="stack">
          <button className={subview === "flow" ? "active" : ""} onClick={() => selectSubview("flow")}>
            Flow{flowCount > 0 ? ` (${flowCount})` : ""}
          </button>
          <button className={subview === "graph" ? "active" : ""} onClick={() => selectSubview("graph")}>
            Topology{graphCount > 0 ? ` (${graphCount})` : ""}
          </button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Unified workflow design, runtime flow, replay inspection, and org topology. Legacy `flow` and `graph` routes still land here.
      </div>
      {subview === "flow" ? <FlowView state={flow.state} actions={flow.actions} /> : null}
      {subview === "graph" ? <GraphPage state={graph.state} actions={graph.actions} /> : null}
    </div>
  );
}
