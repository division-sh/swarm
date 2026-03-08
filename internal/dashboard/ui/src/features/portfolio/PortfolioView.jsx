import React, { useEffect, useState } from "react";
import HoldingView from "../holding/HoldingView.jsx";
import PipelineView from "../pipeline/PipelineView.jsx";

function routeToSubview(activeView) {
  if (activeView === "pipeline" || activeView === "holding") return activeView;
  return "";
}

export default function PortfolioView({
  activeView,
  activeSubview,
  setViewRoute,
  pipeline,
  holding,
}) {
  const routeSubview = routeToSubview(activeView) || activeSubview;
  const [subview, setSubview] = useState(routeSubview || "holding");

  useEffect(() => {
    if (!routeSubview) return;
    setSubview(routeSubview);
    if (activeView === "pipeline" || activeView === "holding") {
      setViewRoute("portfolio", routeSubview);
    }
  }, [activeView, routeSubview, setViewRoute]);

  function selectSubview(next) {
    setSubview(next);
    setViewRoute("portfolio", next);
  }

  const holdingCount = Array.isArray(holding.state.holdingVisibleVerticals) ? holding.state.holdingVisibleVerticals.length : 0;
  const shardCount = Array.isArray(pipeline.state.shardScans) ? pipeline.state.shardScans.length : 0;
  const stuckCount = Array.isArray(pipeline.state.funnel?.stuck) ? pipeline.state.funnel.stuck.length : 0;

  return (
    <div>
      <div className="head">
        <h2>Portfolio</h2>
        <div className="stack">
          <button className={subview === "holding" ? "active" : ""} onClick={() => selectSubview("holding")}>
            Holding Board
          </button>
          <button className={subview === "pipeline" ? "active" : ""} onClick={() => selectSubview("pipeline")}>
            Funnel + Shards
          </button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Unified funnel, shard scan, and holding triage surface. {holdingCount} visible verticals, {shardCount} shard scans, {stuckCount} stuck funnel items.
      </div>
      {subview === "holding" ? <HoldingView state={holding.state} actions={holding.actions} /> : null}
      {subview === "pipeline" ? <PipelineView state={pipeline.state} actions={pipeline.actions} /> : null}
    </div>
  );
}
