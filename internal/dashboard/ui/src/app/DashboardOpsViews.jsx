import React from "react";
import HealthView from "../features/health/HealthView.jsx";
import OperationsView from "../features/operations/OperationsView.jsx";
import PortfolioView from "../features/portfolio/PortfolioView.jsx";

export default function DashboardOpsViews({ activeView, activeSubview, setActiveView, setViewRoute, runtime, ops, pipeline }) {
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
          runtime={runtime}
          ops={ops}
          pipeline={pipeline.pipeline}
          holding={pipeline.holding}
          flow={pipeline.flow}
          graph={pipeline.graph}
        />
      ) : null}

      {activeView === "health" ? (
        <HealthView state={ops.health.state} />
      ) : null}
    </>
  );
}
