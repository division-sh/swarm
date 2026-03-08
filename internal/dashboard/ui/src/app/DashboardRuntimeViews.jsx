import React, { useEffect } from "react";
import AgentsView from "../features/agents/AgentsView.jsx";
import DigestView from "../features/digest/DigestView.jsx";
import ObservabilityView from "../features/observability/ObservabilityView.jsx";
import WorkflowView from "../features/workflow/WorkflowView.jsx";

export default function DashboardRuntimeViews({ activeView, setActiveView, runtime, pipeline }) {
  useEffect(() => {
    if (activeView !== "convos") return;
    const legacyAgentID = runtime.conversations.state.selectedConv;
    if (legacyAgentID) {
      runtime.agents.actions.setSelectedAgentID(legacyAgentID);
    }
    setActiveView("agents");
  }, [activeView, runtime, setActiveView]);

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
          setActiveView={setActiveView}
          events={runtime.events}
          logs={runtime.logs}
          incidents={runtime.incidents}
        />
      ) : null}

      {activeView === "workflow" || activeView === "flow" || activeView === "graph" ? (
        <WorkflowView
          activeView={activeView}
          setActiveView={setActiveView}
          flow={pipeline.flow}
          graph={pipeline.graph}
        />
      ) : null}
    </>
  );
}
