import React from "react";
import HealthView from "../features/health/HealthView.jsx";
import OperationsView from "../features/operations/OperationsView.jsx";
import PortfolioView from "../features/portfolio/PortfolioView.jsx";

export default function DashboardOpsViews({ activeView, setActiveView, ops, pipeline }) {
  return (
    <>
      {activeView === "operations" || activeView === "control" || activeView === "tasks" ? (
        <OperationsView
          activeView={activeView}
          setActiveView={setActiveView}
          control={ops.control}
          tasks={ops.tasks}
        />
      ) : null}

      {activeView === "portfolio" || activeView === "pipeline" || activeView === "holding" ? (
        <PortfolioView
          activeView={activeView}
          setActiveView={setActiveView}
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
