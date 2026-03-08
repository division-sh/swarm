import React from "react";
import AgentsView from "../features/agents/AgentsView.jsx";
import ConversationsView from "../features/conversations/ConversationsView.jsx";
import DigestView from "../features/digest/DigestView.jsx";
import EventsView from "../features/events/EventsView.jsx";
import FlowView from "../features/flow/FlowView.jsx";
import GraphPage from "../features/graph/GraphPage.jsx";
import IncidentsView from "../features/incidents/IncidentsView.jsx";
import LogsView from "../features/logs/LogsView.jsx";

export default function DashboardRuntimeViews({ activeView, runtime, pipeline }) {
  return (
    <>
      {activeView === "agents" ? (
        <AgentsView state={runtime.agents.state} actions={runtime.agents.actions} />
      ) : null}

      {activeView === "digest" ? (
        <DigestView state={runtime.digest.state} actions={runtime.digest.actions} />
      ) : null}

      {activeView === "events" ? (
        <EventsView state={runtime.events.state} actions={runtime.events.actions} />
      ) : null}

      {activeView === "logs" ? (
        <LogsView state={runtime.logs.state} actions={runtime.logs.actions} />
      ) : null}

      {activeView === "incidents" ? (
        <IncidentsView state={runtime.incidents.state} actions={runtime.incidents.actions} />
      ) : null}

      {activeView === "flow" ? (
        <FlowView state={pipeline.flow.state} actions={pipeline.flow.actions} />
      ) : null}

      {activeView === "convos" ? (
        <ConversationsView state={runtime.conversations.state} actions={runtime.conversations.actions} />
      ) : null}

      {activeView === "graph" ? (
        <GraphPage state={pipeline.graph.state} actions={pipeline.graph.actions} />
      ) : null}
    </>
  );
}
