import React from "react";
import AgentTable from "../components/AgentTable.jsx";
import ChatMessages from "../components/ChatMessages.jsx";
import AgentsView from "../features/agents/AgentsView.jsx";
import ConversationsView from "../features/conversations/ConversationsView.jsx";
import DigestView from "../features/digest/DigestView.jsx";
import EventsView from "../features/events/EventsView.jsx";
import FlowView from "../features/flow/FlowView.jsx";
import GraphPage from "../features/graph/GraphPage.jsx";
import IncidentsView from "../features/incidents/IncidentsView.jsx";
import LogsView from "../features/logs/LogsView.jsx";
import { fmtTime, relTime } from "../lib/format.js";

export default function DashboardRuntimeViews({ app }) {
  return (
    <>
      {app.activeView === "agents" ? (
        <AgentsView
          domain={{ agentsResp: app.agentsResp, groupedAgents: app.groupedAgents }}
          controls={{ agentSearch: app.agentSearch, setAgentSearch: app.setAgentSearch, selectedAgentID: app.selectedAgentID, setSelectedAgentID: app.setSelectedAgentID }}
          actions={{ renderAgentDropdown: app.renderAgentDropdown, navigateToTask: app.navigateToTask }}
          AgentTable={AgentTable}
        />
      ) : null}

      {app.activeView === "digest" ? (
        <DigestView digestResp={app.digestResp} onRefresh={() => app.loadDigest().catch((err) => app.addToast(err.message, "error"))} />
      ) : null}

      {app.activeView === "events" ? (
        <EventsView
          domain={{ filteredEvents: app.filteredEvents, filteredRuntimeLogs: app.filteredRuntimeLogs, eventDetail: app.eventDetail }}
          controls={{
            eventsFilter: app.eventsFilter,
            setEventsFilter: app.setEventsFilter,
            eventsIncludeRuntime: app.eventsIncludeRuntime,
            setEventsIncludeRuntime: app.setEventsIncludeRuntime,
            eventsRuntimeErrorsOnly: app.eventsRuntimeErrorsOnly,
            setEventsRuntimeErrorsOnly: app.setEventsRuntimeErrorsOnly,
            selectedEventID: app.selectedEventID,
            setSelectedEventID: app.setSelectedEventID,
          }}
          actions={{
            refresh: () => Promise.all([app.loadEvents(), app.loadRuntimeLogs()]),
            clear: () => {
              app.setEventsFilter({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
              app.setEventsIncludeRuntime(true);
              app.setEventsRuntimeErrorsOnly(false);
            },
          }}
          helpers={{ fmtTime, relTime }}
        />
      ) : null}

      {app.activeView === "logs" ? (
        <LogsView
          domain={{ filteredLogsData: app.filteredLogsData, selectedLog: app.selectedLog }}
          controls={{
            logsFilter: app.logsFilter,
            setLogsFilter: app.setLogsFilter,
            logsRuntimeErrorsOnly: app.logsRuntimeErrorsOnly,
            setLogsRuntimeErrorsOnly: app.setLogsRuntimeErrorsOnly,
            logsOrder: app.logsOrder,
            setLogsOrder: app.setLogsOrder,
            selectedLogID: app.selectedLogID,
            setSelectedLogID: app.setSelectedLogID,
          }}
          actions={{
            loadLogs: app.loadLogs,
            clear: () => {
              app.setLogsFilter({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
              app.setLogsOrder("desc");
              app.setLogsRuntimeErrorsOnly(false);
            },
          }}
          helpers={{ fmtTime, relTime }}
        />
      ) : null}

      {app.activeView === "incidents" ? (
        <IncidentsView
          domain={{ incidentsData: app.incidentsData, selectedIncident: app.selectedIncident, incidentArtifacts: app.incidentArtifacts, incidentLogs: app.incidentLogs }}
          controls={{
            incidentsFilter: app.incidentsFilter,
            setIncidentsFilter: app.setIncidentsFilter,
            selectedIncidentCode: app.selectedIncidentCode,
            setSelectedIncidentCode: app.setSelectedIncidentCode,
            selectedIncidentAgent: app.selectedIncidentAgent,
            setSelectedIncidentAgent: app.setSelectedIncidentAgent,
          }}
          actions={{
            refreshIncidents: app.loadIncidents,
            resetFilters: () => app.setIncidentsFilter({ sinceHours: 24, mcpOnly: true, level: "warn", component: "" }),
            openLogs: app.openLogsForAgent,
            openConvo: app.openConvoForAgent,
          }}
          helpers={{ fmtTime, relTime }}
        />
      ) : null}

      {app.activeView === "flow" ? (
        <FlowView
          domain={{
            verticals: app.verticals,
            visibleFlowEvents: app.visibleFlowEvents,
            flowEvents: app.flowEvents,
            flowGraph: app.flowGraph,
            flowGraphMeta: app.flowGraphMeta,
            flowActiveEdgeKeys: app.flowActiveEdgeKeys,
            selectedFlowSummary: app.selectedFlowSummary,
            agents: app.agentsResp.agents,
          }}
          controls={{
            flowView: app.flowView,
            setFlowView: app.setFlowView,
            flowStage: app.flowStage,
            setFlowStage: app.setFlowStage,
            flowStageOptions: app.flowStageOptions,
            flowRubric: app.flowRubric,
            setFlowRubric: app.setFlowRubric,
            flowRubricOptions: app.flowRubricOptions,
            flowVertical: app.flowVertical,
            setFlowVertical: app.setFlowVertical,
            flowStart: app.flowStart,
            setFlowStart: app.setFlowStart,
            flowEnd: app.flowEnd,
            setFlowEnd: app.setFlowEnd,
            flowReplaySpeed: app.flowReplaySpeed,
            setFlowReplaySpeed: app.setFlowReplaySpeed,
            flowReplayOn: app.flowReplayOn,
            setFlowReplayOn: app.setFlowReplayOn,
            flowReplayIndex: app.flowReplayIndex,
            setFlowReplayIndex: app.setFlowReplayIndex,
          }}
          loadPipelineFlow={app.loadPipelineFlow}
          addToast={app.addToast}
          inspector={{
            selectedFlowNodeID: app.selectedFlowNodeID,
            setSelectedFlowNodeID: app.setSelectedFlowNodeID,
            selectedFlowEdgeID: app.selectedFlowEdgeID,
            setSelectedFlowEdgeID: app.setSelectedFlowEdgeID,
            flowViewGraph: app.flowViewGraph,
            setFlowViewGraph: app.setFlowViewGraph,
            graphFullscreen: app.graphFullscreen,
            setGraphFullscreen: app.setGraphFullscreen,
          }}
          fmtTime={fmtTime}
          relTime={relTime}
        />
      ) : null}

      {app.activeView === "convos" ? (
        <ConversationsView
          domain={{ conversations: app.conversations, conversationDetail: app.conversationDetail }}
          controls={{ selectedConv: app.selectedConv, setSelectedConv: app.setSelectedConv }}
          actions={{
            openConversation: app.loadConversationDetail,
            copyConversation: app.copyConversation,
            openMessage: (message) => app.setModalContent({ title: `Message — ${message.role}`, text: message.text }),
          }}
          ChatMessages={ChatMessages}
        />
      ) : null}

      {app.activeView === "graph" ? (
        <GraphPage
          domain={{ verticals: app.verticals, graph: app.graph, graphViewGraph: app.graphViewGraph, agents: app.agentsResp.agents }}
          controls={{
            graphMode: app.graphMode,
            setGraphMode: app.setGraphMode,
            graphVertical: app.graphVertical,
            setGraphVertical: app.setGraphVertical,
            selectedGraphNodeID: app.selectedGraphNodeID,
            setSelectedGraphNodeID: app.setSelectedGraphNodeID,
            selectedGraphEdgeID: app.selectedGraphEdgeID,
            setSelectedGraphEdgeID: app.setSelectedGraphEdgeID,
            graphFullscreen: app.graphFullscreen,
            setGraphFullscreen: app.setGraphFullscreen,
          }}
          actions={{
            refreshGraph: () => Promise.all([app.loadVerticals(), app.loadGraph()]),
            setGraphViewGraph: app.setGraphViewGraph,
            restartAgent: app.restartAgent,
            openControl: app.openControl,
            inspectAgent: app.inspectAgent,
            navigateToTask: app.navigateToTask,
          }}
          helpers={{ fmtTime, relTime }}
        />
      ) : null}
    </>
  );
}
