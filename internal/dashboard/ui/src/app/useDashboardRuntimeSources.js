import { useDashboardRuntimeData } from "../hooks/useDashboardRuntimeData.js";

export function useDashboardRuntimeSources({
  activeView,
  addToast,
  runtimeState,
}) {
  return useDashboardRuntimeData({
    activeView,
    addToast,
    eventsFilter: runtimeState.eventsFilter,
    eventsRuntimeErrorsOnly: runtimeState.eventsRuntimeErrorsOnly,
    setEvents: runtimeState.setEvents,
    setRuntimeLogs: runtimeState.setRuntimeLogs,
    logsFilter: runtimeState.logsFilter,
    logsOrder: runtimeState.logsOrder,
    logsRuntimeErrorsOnly: runtimeState.logsRuntimeErrorsOnly,
    setLogsData: runtimeState.setLogsData,
    incidentsFilter: runtimeState.incidentsFilter,
    setIncidentsData: runtimeState.setIncidentsData,
    setSelectedIncidentCode: runtimeState.setSelectedIncidentCode,
    selectedIncidentCode: runtimeState.selectedIncidentCode,
    setSelectedIncidentAgent: runtimeState.setSelectedIncidentAgent,
    selectedIncidentAgent: runtimeState.selectedIncidentAgent,
    setIncidentLogs: runtimeState.setIncidentLogs,
    setIncidentArtifacts: runtimeState.setIncidentArtifacts,
    selectedConv: runtimeState.selectedConv,
    setConversations: runtimeState.setConversations,
    setSelectedConv: runtimeState.setSelectedConv,
    setConversationDetail: runtimeState.setConversationDetail,
    selectedEventID: runtimeState.selectedEventID,
    setEventDetail: runtimeState.setEventDetail,
  });
}
