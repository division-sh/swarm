import { useDashboardRuntimeQueries } from "./useDashboardRuntimeQueries.ts";
import type { EventFilter, IncidentFilter, LogFilter } from "../types/runtime.ts";

type RuntimeSourcesInput = {
  activeView: string;
  activeSubview: string;
  runtimeState: {
    eventsFilter: EventFilter;
    eventsRuntimeErrorsOnly: boolean;
    logsFilter: LogFilter;
    logsOrder: string;
    logsRuntimeErrorsOnly: boolean;
    incidentsFilter: IncidentFilter;
    selectedIncidentCode: string;
    setSelectedIncidentCode: (value: string | ((current: string) => string)) => void;
    selectedIncidentAgent: string;
    setSelectedIncidentAgent: (value: string | ((current: string) => string)) => void;
    selectedConv: string;
    setSelectedConv: (value: string | ((current: string) => string)) => void;
    selectedEventID: string;
  };
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
