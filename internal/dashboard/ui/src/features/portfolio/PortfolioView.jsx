import React, { useEffect, useState } from "react";
import HoldingView from "../holding/HoldingView.jsx";
import PipelineView from "../pipeline/PipelineView.jsx";

function routeToSubview(activeView) {
  if (activeView === "pipeline" || activeView === "holding") return activeView;
  return "";
}

export default function PortfolioView({
  activeView,
  setActiveView,
  pipeline,
  holding,
}) {
  const routeSubview = routeToSubview(activeView);
  const [subview, setSubview] = useState(routeSubview || "holding");

  useEffect(() => {
    if (!routeSubview) return;
    setSubview(routeSubview);
    if (activeView !== "portfolio") {
      setActiveView("portfolio");
    }
  }, [activeView, routeSubview, setActiveView]);

  function selectSubview(next) {
    setSubview(next);
    if (activeView !== "portfolio") {
      setActiveView("portfolio");
    }
  }

  const holdingCount = Array.isArray(holding.state.holdingVisibleVerticals) ? holding.state.holdingVisibleVerticals.length : 0;
  const shardCount = Array.isArray(pipeline.state.shardScans) ? pipeline.state.shardScans.length : 0;

  return (
    <div>
      <div className="head">
        <h2>Portfolio</h2>
        <div className="stack">
          <button className={subview === "holding" ? "active" : ""} onClick={() => selectSubview("holding")}>
            Holding Board{holdingCount > 0 ? ` (${holdingCount})` : ""}
          </button>
          <button className={subview === "pipeline" ? "active" : ""} onClick={() => selectSubview("pipeline")}>
            Funnel + Shards{shardCount > 0 ? ` (${shardCount})` : ""}
          </button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Unified funnel, shard scan, and holding triage surface. Legacy `pipeline` and `holding` routes still land here.
      </div>
      {subview === "holding" ? <HoldingView state={holding.state} actions={holding.actions} /> : null}
      {subview === "pipeline" ? <PipelineView state={pipeline.state} actions={pipeline.actions} /> : null}
    </div>
  );
}
