import { useMemo } from "react";
import { useAgentsController } from "../features/agents/useAgentsController.js";

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
}) {
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
      actions: { refresh: () => loadDigest().catch((err) => addToast(err.message, "error")) },
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
        openMessage: (message) => setModalContent({ title: `Message — ${message.role}`, text: message.text }),
      },
    },
  }), [agents, addToast, digestResp, filteredEvents, filteredRuntimeLogs, eventDetail, eventsFilter, eventsIncludeRuntime, eventsRuntimeErrorsOnly, selectedEventID, loadDigest, setEventsFilter, setEventsIncludeRuntime, setEventsRuntimeErrorsOnly, setSelectedEventID, loadEvents, loadRuntimeLogs, filteredLogsData, selectedLog, logsFilter, logsRuntimeErrorsOnly, logsOrder, selectedLogID, setLogsFilter, setLogsRuntimeErrorsOnly, setLogsOrder, setSelectedLogID, loadLogs, incidentsData, selectedIncident, incidentArtifacts, incidentLogs, incidentsFilter, selectedIncidentCode, selectedIncidentAgent, setIncidentsFilter, setSelectedIncidentCode, setSelectedIncidentAgent, loadIncidents, openLogsForAgent, openConvoForAgent, conversations, conversationDetail, selectedConv, setSelectedConv, loadConversationDetail, copyConversation, setModalContent]);
}
