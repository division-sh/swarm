import React from "react";
import type { HealthViewState } from "../features/health/HealthView.tsx";
import HealthView from "../features/health/HealthView.tsx";
import OperationsView from "../features/operations/OperationsView.tsx";
import PortfolioView from "../features/portfolio/PortfolioView.tsx";
import type { VerticalRecord } from "../types/portfolio.ts";

type RuntimeControllerShape = Parameters<typeof PortfolioView>[0]["runtime"];
type OpsControllerShape = {
  control: Parameters<typeof OperationsView>[0]["control"];
  tasks: Parameters<typeof OperationsView>[0]["tasks"];
  health: {
    state: HealthViewState;
    actions: NonNullable<Parameters<typeof HealthView>[0]["actions"]>;
  };
};
type PipelineControllerShape = {
  pipeline: Parameters<typeof PortfolioView>[0]["pipeline"];
  flow: Parameters<typeof PortfolioView>[0]["flow"];
  graph: Parameters<typeof PortfolioView>[0]["graph"];
  holding: {
    state: { holdingData?: { verticals?: VerticalRecord[] } };
    actions: { openHoldingVerticalDetail: (verticalID: string) => Promise<void> | void };
  };
};

type DashboardOpsViewsProps = {
  activeView: string;
  activeSubview: string;
  setActiveView: (value: string) => void;
  setViewRoute: (view: string, subview?: string) => void;
  runtime: RuntimeControllerShape;
  ops: OpsControllerShape;
  pipeline: PipelineControllerShape;
};

export default function DashboardOpsViews({
  activeView,
  activeSubview,
  setActiveView: _setActiveView,
  setViewRoute,
  runtime,
  ops,
  pipeline,
}: DashboardOpsViewsProps) {
  function openWorkflowTraceForVertical(vertical: string) {
    const value = String(vertical || "").trim();
    if (!value) {
      setViewRoute("workflow", "flow");
      return;
    }
    pipeline.flow.actions.setFlowView("runtime");
    pipeline.flow.actions.setFlowVertical(value);
    pipeline.flow.actions.setSelectedFlowNodeID("");
    pipeline.flow.actions.setSelectedFlowEdgeID("");
    setViewRoute("workflow", "flow");
  }

  function openPortfolioForVertical(vertical: string) {
    const value = String(vertical || "").trim();
    const knownVerticals = Array.isArray(pipeline.holding.state.holdingData?.verticals)
      ? pipeline.holding.state.holdingData.verticals
      : [];
    const match = knownVerticals.find((item) => item.slug === value || item.id === value);
    if (match?.id) {
      pipeline.holding.actions.openHoldingVerticalDetail(match.id);
    }
    setViewRoute("portfolio", "holding");
  }

  return (
    <>
      {activeView === "operations" || activeView === "control" || activeView === "tasks" ? (
        <OperationsView
          activeView={activeView}
          activeSubview={activeSubview}
          setViewRoute={setViewRoute}
          control={ops.control}
          tasks={ops.tasks}
          pipeline={pipeline}
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
        <HealthView
          state={ops.health.state}
          actions={{
            ...ops.health.actions,
            openWorkflowTraceForVertical,
            openPortfolioForVertical,
          }}
        />
      ) : null}
    </>
  );
}
