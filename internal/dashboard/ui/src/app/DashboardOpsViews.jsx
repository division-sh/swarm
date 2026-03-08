import React from "react";
import HealthView from "../features/health/HealthView.jsx";
import OperationsView from "../features/operations/OperationsView.jsx";
import PortfolioView from "../features/portfolio/PortfolioView.jsx";

export default function DashboardOpsViews({ activeView, activeSubview, setActiveView, setViewRoute, ops, pipeline }) {
  return (
    <>
      {activeView === "operations" || activeView === "control" || activeView === "tasks" ? (
        <OperationsView
          activeView={activeView}
          activeSubview={activeSubview}
          setViewRoute={setViewRoute}
          control={ops.control}
          tasks={ops.tasks}
        />
      ) : null}

      {activeView === "portfolio" || activeView === "pipeline" || activeView === "holding" ? (
        <PortfolioView
          activeView={activeView}
          activeSubview={activeSubview}
          setViewRoute={setViewRoute}
          pipeline={pipeline.pipeline}
          holding={pipeline.holding}
        />
      ) : null}

      {activeView === "health" ? (
        <HealthView state={ops.health.state} />
      ) : null}
    </>
  );
}
