import type { AgentsResponse, DigestResponse } from "../types/core.ts";
import { useGroupedAgents } from "../features/agents/useGroupedAgents.ts";
import { useEventsState } from "../features/events/useEventsState.ts";
import { useIncidentState } from "../features/incidents/useIncidentState.ts";
import { useLogsState } from "../features/logs/useLogsState.ts";
import { useDashboardRuntimeController } from "./useDashboardRuntimeController.ts";

type RuntimeAssemblyInput = {
  agentsResp: AgentsResponse;
  digestResp: DigestResponse;
  ui: {
    agentSearch: string;
    setAgentSearch: (value: string) => void;
    setModalContent: (value: Record<string, any>) => void;
  };
  runtimeState: Record<string, any>;
  runtimeData: Record<string, any>;
  opsState: Record<string, any>;
  loaders: {
    loadDigest: () => Promise<unknown>;
    loadEvents: () => Promise<unknown>;
    loadRuntimeLogs: () => Promise<unknown>;
    loadLogs: () => Promise<unknown>;
    loadIncidents: () => Promise<unknown>;
    loadConversationDetail: (conversationID: string) => Promise<unknown>;
  };
  navigationActions: Record<string, any>;
  addToast: (message: string, type?: string) => void;
};

export function useDashboardRuntimeAssembly({
  agentsResp,
  digestResp,
  ui,
  runtimeState,
  runtimeData,
  opsState,
  loaders,
  navigationActions,
  addToast,
}: RuntimeAssemblyInput) {
  const groupedAgents = useGroupedAgents({
    agents: agentsResp.agents,
    agentSearch: ui.agentSearch,
    selectedAgentID: opsState.selectedAgentID,
    setSelectedAgentID: opsState.setSelectedAgentID,
  });

  const { filteredEvents, filteredRuntimeLogs } = useEventsState({
    events: runtimeData.events,
    runtimeLogs: runtimeData.runtimeLogs,
    eventsRuntimeErrorsOnly: runtimeState.eventsRuntimeErrorsOnly,
  });

  const { filteredLogsData, selectedLog } = useLogsState({
    logsData: runtimeData.logsData,
    logsRuntimeErrorsOnly: runtimeState.logsRuntimeErrorsOnly,
    selectedLogID: runtimeState.selectedLogID,
    setSelectedLogID: runtimeState.setSelectedLogID,
  });

  const selectedIncident = useIncidentState({
    incidentsData: runtimeData.incidentsData,
    selectedIncidentCode: runtimeState.selectedIncidentCode,
  });

  return useDashboardRuntimeController({
    agentsResp,
    groupedAgents,
    agentSearch: ui.agentSearch,
    setAgentSearch: ui.setAgentSearch,
    selectedAgentID: opsState.selectedAgentID,
    setSelectedAgentID: opsState.setSelectedAgentID,
    renderAgentDropdown: navigationActions.renderAgentDropdown,
    navigateToTask: navigationActions.navigateToTask,
    digestResp,
    loadDigest: loaders.loadDigest,
    addToast,
    filteredEvents,
    filteredRuntimeLogs,
    eventDetail: runtimeData.eventDetail,
    eventsFilter: runtimeState.eventsFilter,
    setEventsFilter: runtimeState.setEventsFilter,
    eventsIncludeRuntime: runtimeState.eventsIncludeRuntime,
    setEventsIncludeRuntime: runtimeState.setEventsIncludeRuntime,
    eventsRuntimeErrorsOnly: runtimeState.eventsRuntimeErrorsOnly,
    setEventsRuntimeErrorsOnly: runtimeState.setEventsRuntimeErrorsOnly,
    selectedEventID: runtimeState.selectedEventID,
    setSelectedEventID: runtimeState.setSelectedEventID,
    loadEvents: loaders.loadEvents,
    loadRuntimeLogs: loaders.loadRuntimeLogs,
    filteredLogsData,
    selectedLog,
    logsFilter: runtimeState.logsFilter,
    setLogsFilter: runtimeState.setLogsFilter,
    logsRuntimeErrorsOnly: runtimeState.logsRuntimeErrorsOnly,
    setLogsRuntimeErrorsOnly: runtimeState.setLogsRuntimeErrorsOnly,
    logsOrder: runtimeState.logsOrder,
    setLogsOrder: runtimeState.setLogsOrder,
    selectedLogID: runtimeState.selectedLogID,
    setSelectedLogID: runtimeState.setSelectedLogID,
    loadLogs: loaders.loadLogs,
    incidentsData: runtimeData.incidentsData,
    selectedIncident,
    incidentArtifacts: runtimeData.incidentArtifacts,
    incidentLogs: runtimeData.incidentLogs,
    incidentsFilter: runtimeState.incidentsFilter,
    setIncidentsFilter: runtimeState.setIncidentsFilter,
    selectedIncidentCode: runtimeState.selectedIncidentCode,
    setSelectedIncidentCode: runtimeState.setSelectedIncidentCode,
    selectedIncidentAgent: runtimeState.selectedIncidentAgent,
    setSelectedIncidentAgent: runtimeState.setSelectedIncidentAgent,
    loadIncidents: loaders.loadIncidents,
    openLogsForAgent: navigationActions.openLogsForAgent,
    openConvoForAgent: navigationActions.openConvoForAgent,
    conversations: runtimeData.conversations,
    conversationDetail: runtimeData.conversationDetail,
    selectedConv: runtimeState.selectedConv,
    setSelectedConv: runtimeState.setSelectedConv,
    loadConversationDetail: loaders.loadConversationDetail,
    copyConversation: navigationActions.copyConversation,
    setModalContent: ui.setModalContent,
  });
}
