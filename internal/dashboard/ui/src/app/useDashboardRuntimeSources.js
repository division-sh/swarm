import { useDashboardRuntimeQueries } from "./useDashboardRuntimeQueries.ts";

export function useDashboardRuntimeSources({
  activeView,
  activeSubview,
  runtimeState,
}) {
  return useDashboardRuntimeQueries({
    activeView,
    activeSubview,
    eventsFilter: runtimeState.eventsFilter,
    eventsRuntimeErrorsOnly: runtimeState.eventsRuntimeErrorsOnly,
    logsFilter: runtimeState.logsFilter,
    logsOrder: runtimeState.logsOrder,
    logsRuntimeErrorsOnly: runtimeState.logsRuntimeErrorsOnly,
    incidentsFilter: runtimeState.incidentsFilter,
    selectedIncidentCode: runtimeState.selectedIncidentCode,
    selectedIncidentAgent: runtimeState.selectedIncidentAgent,
    selectedConv: runtimeState.selectedConv,
    setSelectedConv: runtimeState.setSelectedConv,
    setSelectedIncidentCode: runtimeState.setSelectedIncidentCode,
    setSelectedIncidentAgent: runtimeState.setSelectedIncidentAgent,
    selectedEventID: runtimeState.selectedEventID,
  });
}
