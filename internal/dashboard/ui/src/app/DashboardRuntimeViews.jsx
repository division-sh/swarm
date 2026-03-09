import React, { useEffect } from "react";
import AgentsView from "../features/agents/AgentsView.jsx";
import DigestView from "../features/digest/DigestView.jsx";
import ObservabilityView from "../features/observability/ObservabilityView.jsx";
import WorkflowView from "../features/workflow/WorkflowView.jsx";

export default function DashboardRuntimeViews({ activeView, activeSubview, setActiveView, setViewRoute, runtime, pipeline }) {
  useEffect(() => {
    if (activeView !== "convos") return;
    const legacyAgentID = runtime.conversations.state.selectedConv;
    if (legacyAgentID) {
      runtime.agents.actions.setSelectedAgentID(legacyAgentID);
    }
    setViewRoute("agents");
  }, [activeView, runtime, setViewRoute]);

  function openAgent(agentID) {
    const value = String(agentID || "").trim();
    if (!value) return;
    runtime.agents.actions.setSelectedAgentID(value);
    setViewRoute("agents");
  }

  function openWorkflowTrace(vertical) {
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

  function openPortfolio(vertical) {
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
