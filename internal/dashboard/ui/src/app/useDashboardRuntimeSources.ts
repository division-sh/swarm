import { useDashboardRuntimeQueries } from "./useDashboardRuntimeQueries.ts";

type RuntimeSourcesInput = {
  activeView: string;
  activeSubview: string;
  runtimeState: Record<string, any>;
};

export function useDashboardRuntimeSources({
  activeView,
  activeSubview,
  runtimeState,
}: RuntimeSourcesInput) {
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
