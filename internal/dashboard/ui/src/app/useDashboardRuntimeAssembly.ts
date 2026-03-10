import type { ReactNode } from "react";
import type { AgentsResponse, DigestResponse } from "../types/core.ts";
import type { ConversationDetail, ConversationRecord, EventDetail, EventFilter, EventRecord, IncidentArtifacts, IncidentFilter, IncidentRecord, LogFilter, RuntimeLogRecord } from "../types/runtime.ts";
import { useGroupedAgents } from "../features/agents/useGroupedAgents.ts";
import { useEventsState } from "../features/events/useEventsState.ts";
import { useIncidentState } from "../features/incidents/useIncidentState.ts";
import { useLogsState } from "../features/logs/useLogsState.ts";
import { useDashboardRuntimeController } from "./useDashboardRuntimeController.ts";
import type { ModalContent } from "./useDashboardUIState.ts";

type StringSetter = (value: string) => void;
type NullableStringSetter = (value: string | null) => void;
type FilterSetter<T> = (value: T | ((prev: T) => T)) => void;

type RuntimeUIState = {
  agentSearch: string;
  setAgentSearch: StringSetter;
  setModalContent: (value: ModalContent) => void;
};

type RuntimeState = {
  eventsRuntimeErrorsOnly: boolean;
  eventsFilter: EventFilter;
  setEventsFilter: FilterSetter<EventFilter>;
  eventsIncludeRuntime: boolean;
  setEventsIncludeRuntime: (value: boolean) => void;
  setEventsRuntimeErrorsOnly: (value: boolean) => void;
  selectedEventID: string;
  setSelectedEventID: StringSetter;
  logsRuntimeErrorsOnly: boolean;
  logsFilter: LogFilter;
  setLogsFilter: FilterSetter<LogFilter>;
  setLogsRuntimeErrorsOnly: (value: boolean) => void;
  logsOrder: string;
  setLogsOrder: StringSetter;
  selectedLogID: string | null;
  setSelectedLogID: NullableStringSetter;
  incidentsFilter: IncidentFilter;
  setIncidentsFilter: FilterSetter<IncidentFilter>;
  selectedIncidentCode: string;
  setSelectedIncidentCode: StringSetter;
  selectedIncidentAgent: string;
  setSelectedIncidentAgent: StringSetter;
  selectedConv: string;
  setSelectedConv: StringSetter;
};

type RuntimeData = {
  events: EventRecord[];
  runtimeLogs: RuntimeLogRecord[];
  eventDetail: EventDetail | null;
  logsData: RuntimeLogRecord[];
  incidentsData: IncidentRecord[];
  incidentArtifacts: IncidentArtifacts | null;
  incidentLogs: RuntimeLogRecord[];
  conversations: ConversationRecord[];
  conversationDetail: ConversationDetail;
};

type RuntimeOpsState = {
  selectedAgentID: string;
  setSelectedAgentID: StringSetter;
};

type RuntimeNavigationActions = {
  renderAgentDropdown: (agent: AgentsResponse["agents"][number]) => ReactNode;
  navigateToTask: (taskID: string) => void;
  openLogsForAgent: (agentID: string) => void;
  openConvoForAgent: (agentID: string) => void;
  copyConversation: (agentID: string, messages: ConversationDetail["messages"]) => void;
};

type RuntimeAssemblyInput = {
  agentsResp: AgentsResponse;
  digestResp: DigestResponse;
  ui: RuntimeUIState;
  runtimeState: RuntimeState;
  runtimeData: RuntimeData;
  opsState: RuntimeOpsState;
  loaders: {
    loadDigest: () => Promise<unknown>;
    loadEvents: () => Promise<unknown>;
    loadRuntimeLogs: () => Promise<unknown>;
    loadLogs: () => Promise<unknown>;
    loadIncidents: () => Promise<unknown>;
    loadConversationDetail: (conversationID: string) => Promise<unknown>;
  };
  navigationActions: RuntimeNavigationActions;
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
