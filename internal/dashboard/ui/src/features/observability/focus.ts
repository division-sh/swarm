export const EMPTY_EVENT_FILTER = { type: "", source: "", vertical: "", component: "", level: "", subscriber: "" };
export const EMPTY_LOG_FILTER = { type: "", source: "", vertical: "", component: "", level: "", subscriber: "" };
export const DEFAULT_INCIDENTS_FILTER = { sinceHours: 24, mcpOnly: true, level: "warn", component: "" };

function trim(value: unknown) {
  return typeof value === "string" ? value.trim() : "";
}

type FocusInput = {
  events: { state?: Record<string, any> };
  logs: { state?: Record<string, any> };
  incidents: { state?: Record<string, any> };
};

export function deriveObservabilityFocus({ events, logs, incidents }: FocusInput) {
  const agent = trim(
    events?.state?.eventsFilter?.subscriber
      || logs?.state?.logsFilter?.source
      || logs?.state?.logsFilter?.subscriber
      || incidents?.state?.selectedIncidentAgent,
  );
  const vertical = trim(events?.state?.eventsFilter?.vertical || logs?.state?.logsFilter?.vertical);
  const component = trim(
    incidents?.state?.incidentsFilter?.component
      || events?.state?.eventsFilter?.component
      || logs?.state?.logsFilter?.component,
  );
  const incidentCode = trim(incidents?.state?.selectedIncidentCode);
  const eventType = trim(events?.state?.eventsFilter?.type || logs?.state?.logsFilter?.type);
  const level = trim(events?.state?.eventsFilter?.level || logs?.state?.logsFilter?.level || incidents?.state?.incidentsFilter?.level);
  const runtimeErrorsOnly = !!events?.state?.eventsRuntimeErrorsOnly || !!logs?.state?.logsRuntimeErrorsOnly;
  const includeRuntime = !!events?.state?.eventsIncludeRuntime;

  const chips = [
    agent ? `agent:${agent}` : "",
    vertical ? `vertical:${vertical}` : "",
    component ? `component:${component}` : "",
    incidentCode ? `incident:${incidentCode}` : "",
    eventType ? `type:${eventType}` : "",
    level ? `level:${level}` : "",
    runtimeErrorsOnly ? "errors-only" : "",
    includeRuntime ? "runtime-on" : "",
  ].filter(Boolean);

  return {
    agent,
    vertical,
    component,
    incidentCode,
    eventType,
    level,
    runtimeErrorsOnly,
    includeRuntime,
    chips,
  };
}
