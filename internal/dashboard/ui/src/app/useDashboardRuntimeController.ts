import { useMemo } from "react";
import type { ReactNode } from "react";
import type { AgentRecord, AgentsResponse, DigestResponse } from "../types/core.ts";
import type {
  ConversationDetail,
  ConversationMessage,
  ConversationRecord,
  EventDetail,
  EventFilter,
  EventRecord,
  IncidentArtifacts,
  IncidentFilter,
  IncidentRecord,
  LogFilter,
  RuntimeLogRecord,
} from "../types/runtime.ts";
import { useAgentsController } from "../features/agents/useAgentsController.ts";
import type { ModalContent } from "./useDashboardUIState.ts";

type AsyncAction = () => Promise<unknown>;
type StringSetter = (value: string) => void;
type NullableStringSetter = (value: string | null) => void;
type FilterSetter<T> = (value: T | ((prev: T) => T)) => void;
type GroupedAgents = {
  holding: AgentRecord[];
  opcos: Array<{ slug?: string; agents: AgentRecord[] }>;
};

type DashboardRuntimeControllerInput = {
  agentsResp: AgentsResponse;
  groupedAgents: GroupedAgents;
  agentSearch: string;
  setAgentSearch: StringSetter;
  selectedAgentID: string;
  setSelectedAgentID: StringSetter;
  renderAgentDropdown: (agent: AgentRecord) => ReactNode;
  navigateToTask: (taskID: string) => void;
  digestResp: DigestResponse;
  loadDigest: AsyncAction;
  addToast: (message: string, type?: string) => void;
  filteredEvents: EventRecord[];
  filteredRuntimeLogs: RuntimeLogRecord[];
  eventDetail: EventDetail | null;
  eventsFilter: EventFilter;
  setEventsFilter: FilterSetter<EventFilter>;
  eventsIncludeRuntime: boolean;
  setEventsIncludeRuntime: (value: boolean) => void;
  eventsRuntimeErrorsOnly: boolean;
  setEventsRuntimeErrorsOnly: (value: boolean) => void;
  selectedEventID: string;
  setSelectedEventID: StringSetter;
  loadEvents: AsyncAction;
  loadRuntimeLogs: AsyncAction;
  filteredLogsData: RuntimeLogRecord[];
  selectedLog: RuntimeLogRecord | null;
  logsFilter: LogFilter;
  setLogsFilter: FilterSetter<LogFilter>;
  logsRuntimeErrorsOnly: boolean;
  setLogsRuntimeErrorsOnly: (value: boolean) => void;
  logsOrder: string;
  setLogsOrder: StringSetter;
  selectedLogID: string | null;
  setSelectedLogID: NullableStringSetter;
  loadLogs: AsyncAction;
  incidentsData: IncidentRecord[];
  selectedIncident: IncidentRecord | null;
  incidentArtifacts: IncidentArtifacts | null;
  incidentLogs: RuntimeLogRecord[];
  incidentsFilter: IncidentFilter;
  setIncidentsFilter: FilterSetter<IncidentFilter>;
  selectedIncidentCode: string;
  setSelectedIncidentCode: StringSetter;
  selectedIncidentAgent: string;
  setSelectedIncidentAgent: StringSetter;
  loadIncidents: AsyncAction;
  openLogsForAgent: (agentID: string) => void;
  openConvoForAgent: (agentID: string) => void;
  conversations: ConversationRecord[];
  conversationDetail: ConversationDetail;
  selectedConv: string;
  setSelectedConv: StringSetter;
  loadConversationDetail: (conversationID: string) => Promise<unknown>;
  copyConversation: (agentID: string, messages: ConversationMessage[]) => void;
  setModalContent: (value: ModalContent) => void;
};

export function useDashboardRuntimeController({
  agentsResp,
  groupedAgents,
  agentSearch,
  setAgentSearch,
  selectedAgentID,
  setSelectedAgentID,
  renderAgentDropdown,
  navigateToTask,
  digestResp,
  loadDigest,
  addToast,
  filteredEvents,
  filteredRuntimeLogs,
  eventDetail,
  eventsFilter,
  setEventsFilter,
  eventsIncludeRuntime,
  setEventsIncludeRuntime,
  eventsRuntimeErrorsOnly,
  setEventsRuntimeErrorsOnly,
  selectedEventID,
  setSelectedEventID,
  loadEvents,
  loadRuntimeLogs,
  filteredLogsData,
  selectedLog,
  logsFilter,
  setLogsFilter,
  logsRuntimeErrorsOnly,
  setLogsRuntimeErrorsOnly,
  logsOrder,
  setLogsOrder,
  selectedLogID,
  setSelectedLogID,
  loadLogs,
  incidentsData,
  selectedIncident,
  incidentArtifacts,
  incidentLogs,
  incidentsFilter,
  setIncidentsFilter,
  selectedIncidentCode,
  setSelectedIncidentCode,
  selectedIncidentAgent,
  setSelectedIncidentAgent,
  loadIncidents,
  openLogsForAgent,
  openConvoForAgent,
  conversations,
  conversationDetail,
  selectedConv,
  setSelectedConv,
  loadConversationDetail,
  copyConversation,
  setModalContent,
}: DashboardRuntimeControllerInput) {
  const agents = useAgentsController({
    agentsResp,
    groupedAgents,
    agentSearch,
    selectedAgentID,
    setAgentSearch,
    setSelectedAgentID,
    renderAgentDropdown,
    navigateToTask,
  });

  return useMemo(() => ({
    agents,
    digest: {
      state: { digestResp },
      actions: { refresh: () => loadDigest().catch((err: Error) => addToast(err.message, "error")) },
    },
    events: {
      state: { filteredEvents, filteredRuntimeLogs, eventDetail, eventsFilter, eventsIncludeRuntime, eventsRuntimeErrorsOnly, selectedEventID },
      actions: {
        setEventsFilter,
        setEventsIncludeRuntime,
        setEventsRuntimeErrorsOnly,
        setSelectedEventID,
        refresh: () => Promise.all([loadEvents(), loadRuntimeLogs()]),
        clear: () => {
          setEventsFilter({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
          setEventsIncludeRuntime(true);
          setEventsRuntimeErrorsOnly(false);
        },
      },
    },
    logs: {
      state: { filteredLogsData, selectedLog, logsFilter, logsRuntimeErrorsOnly, logsOrder, selectedLogID },
      actions: {
        setLogsFilter,
        setLogsRuntimeErrorsOnly,
        setLogsOrder,
        setSelectedLogID,
        refresh: loadLogs,
        clear: () => {
          setLogsFilter({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
          setLogsOrder("desc");
          setLogsRuntimeErrorsOnly(false);
        },
      },
    },
    incidents: {
      state: { incidentsData, selectedIncident, incidentArtifacts, incidentLogs, incidentsFilter, selectedIncidentCode, selectedIncidentAgent },
      actions: {
        setIncidentsFilter,
        setSelectedIncidentCode,
        setSelectedIncidentAgent,
        refresh: loadIncidents,
        resetFilters: () => setIncidentsFilter({ sinceHours: 24, mcpOnly: true, level: "warn", component: "" }),
        openLogs: openLogsForAgent,
        openConvo: openConvoForAgent,
      },
    },
    conversations: {
      state: { conversations, conversationDetail, selectedConv },
      actions: {
        setSelectedConv,
        openConversation: loadConversationDetail,
        copyConversation,
        openMessage: (message: ConversationMessage) => setModalContent({ title: `Message — ${message.role}`, text: message.text }),
      },
    },
  }), [agents, addToast, digestResp, filteredEvents, filteredRuntimeLogs, eventDetail, eventsFilter, eventsIncludeRuntime, eventsRuntimeErrorsOnly, selectedEventID, loadDigest, setEventsFilter, setEventsIncludeRuntime, setEventsRuntimeErrorsOnly, setSelectedEventID, loadEvents, loadRuntimeLogs, filteredLogsData, selectedLog, logsFilter, logsRuntimeErrorsOnly, logsOrder, selectedLogID, setLogsFilter, setLogsRuntimeErrorsOnly, setLogsOrder, setSelectedLogID, loadLogs, incidentsData, selectedIncident, incidentArtifacts, incidentLogs, incidentsFilter, selectedIncidentCode, selectedIncidentAgent, setIncidentsFilter, setSelectedIncidentCode, setSelectedIncidentAgent, loadIncidents, openLogsForAgent, openConvoForAgent, conversations, conversationDetail, selectedConv, setSelectedConv, loadConversationDetail, copyConversation, setModalContent]);
}
