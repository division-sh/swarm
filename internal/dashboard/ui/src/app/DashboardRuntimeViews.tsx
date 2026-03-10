import React, { useEffect } from "react";
import AgentsView from "../features/agents/AgentsView.tsx";
import DigestView from "../features/digest/DigestView.tsx";
import ObservabilityView from "../features/observability/ObservabilityView.tsx";
import WorkflowView from "../features/workflow/WorkflowView.tsx";
import type { VerticalRecord } from "../types/portfolio.ts";

type RuntimeControllerShape = {
  agents: Parameters<typeof AgentsView>[0];
  conversations: { state: { selectedConv: string } };
  digest: Parameters<typeof DigestView>[0];
  events: Parameters<typeof ObservabilityView>[0]["events"];
  logs: Parameters<typeof ObservabilityView>[0]["logs"];
  incidents: Parameters<typeof ObservabilityView>[0]["incidents"];
};

type PipelineControllerShape = {
  flow: Parameters<typeof WorkflowView>[0]["flow"];
  graph: Parameters<typeof WorkflowView>[0]["graph"];
  holding: {
    state: { holdingData?: { verticals?: VerticalRecord[] } };
    actions: { openHoldingVerticalDetail: (verticalID: string) => Promise<void> | void };
  };
};

type DashboardRuntimeViewsProps = {
  activeView: string;
  activeSubview: string;
  setActiveView: (value: string) => void;
  setViewRoute: (view: string, subview?: string) => void;
  runtime: RuntimeControllerShape;
  pipeline: PipelineControllerShape;
};

export default function DashboardRuntimeViews({
  activeView,
  activeSubview,
  setActiveView: _setActiveView,
  setViewRoute,
  runtime,
  pipeline,
}: DashboardRuntimeViewsProps) {
  useEffect(() => {
    if (activeView !== "convos") return;
    const legacyAgentID = runtime.conversations.state.selectedConv;
    if (legacyAgentID) {
      runtime.agents.actions.setSelectedAgentID(legacyAgentID);
    }
    setViewRoute("agents");
  }, [activeView, runtime, setViewRoute]);

  function openAgent(agentID: string) {
    const value = String(agentID || "").trim();
    if (!value) return;
    runtime.agents.actions.setSelectedAgentID(value);
    setViewRoute("agents");
  }

  function openWorkflowTrace(vertical: string) {
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

  function openPortfolio(vertical: string) {
    const value = String(vertical || "").trim();
    if (!value) {
      setViewRoute("portfolio", "holding");
      return;
    }
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
      {activeView === "agents" || activeView === "convos" ? (
        <AgentsView state={runtime.agents.state} actions={runtime.agents.actions} />
      ) : null}

      {activeView === "digest" ? (
        <DigestView state={runtime.digest.state} actions={runtime.digest.actions} />
      ) : null}

      {activeView === "observability" || activeView === "events" || activeView === "logs" || activeView === "incidents" ? (
        <ObservabilityView
          activeView={activeView}
          activeSubview={activeSubview}
          setViewRoute={setViewRoute}
          events={runtime.events}
          logs={runtime.logs}
          incidents={runtime.incidents}
          actions={{
            openAgent,
            openWorkflowTrace,
            openPortfolio,
          }}
        />
      ) : null}

      {activeView === "workflow" || activeView === "flow" || activeView === "graph" ? (
        <WorkflowView
          activeView={activeView}
          activeSubview={activeSubview}
          setViewRoute={setViewRoute}
          flow={pipeline.flow}
          graph={pipeline.graph}
        />
      ) : null}
    </>
  );
}
