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
