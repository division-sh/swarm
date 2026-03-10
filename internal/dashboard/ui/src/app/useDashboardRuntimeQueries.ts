import { useEffect, useMemo } from "react";
import { useQuery, type QueryObserverResult } from "@tanstack/react-query";
import {
  fetchConversationDetail,
  fetchConversations,
  fetchEventDetail,
  fetchEvents,
  fetchIncidentArtifacts,
  fetchIncidentLogs,
  fetchIncidents,
  fetchLogs,
  fetchRuntimeLogs,
} from "../api/dashboardRuntime.ts";
import { dashboardQueryKeys } from "./dashboardQueryKeys.ts";
import type { ConversationDetail, ConversationRecord, EventDetail, EventFilter, IncidentFilter, LogFilter } from "../types/runtime.ts";

async function runRefetch<T>(refetch: () => Promise<QueryObserverResult<T, Error>>): Promise<T | undefined> {
  const result = await refetch();
  if (result.error) throw result.error;
  return result.data;
}

type StringUpdater = (value: string | ((current: string) => string)) => void;

export function useDashboardRuntimeQueries({
  activeView,
  activeSubview,
  eventsFilter,
  eventsRuntimeErrorsOnly,
  logsFilter,
  logsOrder,
  logsRuntimeErrorsOnly,
  incidentsFilter,
  selectedIncidentCode,
  setSelectedIncidentCode,
  selectedIncidentAgent,
  setSelectedIncidentAgent,
  selectedConv,
  setSelectedConv,
  selectedEventID,
}: {
  activeView: string;
  activeSubview: string;
  eventsFilter: EventFilter;
  eventsRuntimeErrorsOnly: boolean;
  logsFilter: LogFilter;
  logsOrder: string;
  logsRuntimeErrorsOnly: boolean;
  incidentsFilter: IncidentFilter;
  selectedIncidentCode: string;
  setSelectedIncidentCode: StringUpdater;
  selectedIncidentAgent: string;
  setSelectedIncidentAgent: StringUpdater;
  selectedConv: string;
  setSelectedConv: StringUpdater;
  selectedEventID: string;
}) {
  const observabilitySubview = activeView === "observability" ? (activeSubview || "events") : activeView;
  const eventsActive = observabilitySubview === "events";
  const logsActive = observabilitySubview === "logs";
  const incidentsActive = observabilitySubview === "incidents";
  const agentsActive = activeView === "agents";

  const eventsQuery = useQuery({
    queryKey: dashboardQueryKeys.events(eventsFilter),
    queryFn: () => fetchEvents(eventsFilter),
    enabled: eventsActive,
  });
  const runtimeLogsQuery = useQuery({
    queryKey: dashboardQueryKeys.runtimeLogs(eventsFilter, eventsRuntimeErrorsOnly),
    queryFn: () => fetchRuntimeLogs(eventsFilter, eventsRuntimeErrorsOnly),
    enabled: eventsActive,
  });
  const logsQuery = useQuery({
    queryKey: dashboardQueryKeys.logs(logsFilter, logsOrder, logsRuntimeErrorsOnly),
    queryFn: () => fetchLogs(logsFilter, logsOrder, logsRuntimeErrorsOnly),
    enabled: logsActive,
  });
  const incidentsQuery = useQuery({
    queryKey: dashboardQueryKeys.incidents(incidentsFilter),
    queryFn: () => fetchIncidents(incidentsFilter),
    enabled: incidentsActive,
  });
  const incidentLogsQuery = useQuery({
    queryKey: dashboardQueryKeys.incidentLogs(selectedIncidentCode),
    queryFn: () => fetchIncidentLogs(selectedIncidentCode),
    enabled: incidentsActive && !!selectedIncidentCode,
  });
  const incidentArtifactsQuery = useQuery({
    queryKey: dashboardQueryKeys.incidentArtifacts(selectedIncidentAgent),
    queryFn: () => fetchIncidentArtifacts(selectedIncidentAgent),
    enabled: incidentsActive && !!selectedIncidentAgent,
  });
  const eventDetailQuery = useQuery<EventDetail | null>({
    queryKey: dashboardQueryKeys.eventDetail(selectedEventID),
    queryFn: () => fetchEventDetail(selectedEventID),
    enabled: !!selectedEventID,
  });
  const conversationsQuery = useQuery<ConversationRecord[]>({
    queryKey: dashboardQueryKeys.conversations(),
    queryFn: fetchConversations,
    enabled: agentsActive,
  });
  const conversationDetailQuery = useQuery<ConversationDetail>({
    queryKey: dashboardQueryKeys.conversationDetail(selectedConv),
    queryFn: () => fetchConversationDetail(selectedConv),
    enabled: !!selectedConv,
  });

  useEffect(() => {
    const items = incidentsQuery.data || [];
    setSelectedIncidentCode((cur: string) => {
      if (!cur) return items.length > 0 ? items[0].code : "";
      return items.some((item) => item.code === cur) ? cur : (items[0]?.code || "");
    });
  }, [incidentsQuery.data, setSelectedIncidentCode]);

  useEffect(() => {
    const rows = incidentLogsQuery.data || [];
    setSelectedIncidentAgent((cur: string) => {
      if (cur && rows.some((row) => row.agent_id === cur)) return cur;
      const first = rows.find((row) => (row.agent_id || "").trim() !== "");
      return first ? first.agent_id : "";
    });
  }, [incidentLogsQuery.data, setSelectedIncidentAgent]);

  useEffect(() => {
    const items = conversationsQuery.data || [];
    if (items.length === 0) return;
    setSelectedConv((cur: string) => cur || items[0].agent_id);
  }, [conversationsQuery.data, setSelectedConv]);

  return useMemo(() => ({
    data: {
      events: eventsQuery.data || [],
      runtimeLogs: runtimeLogsQuery.data || [],
      logsData: logsQuery.data || [],
      incidentsData: incidentsQuery.data || [],
      incidentLogs: incidentLogsQuery.data || [],
      incidentArtifacts: {
        loading: incidentArtifactsQuery.isFetching,
        error: incidentArtifactsQuery.error?.message || "",
        data: incidentArtifactsQuery.data || null,
      },
      eventDetail: eventDetailQuery.data || null,
      conversations: conversationsQuery.data || [],
      conversationDetail: conversationDetailQuery.data || { messages: [], turns: [] },
    },
    queries: {
      events: eventsQuery,
      runtimeLogs: runtimeLogsQuery,
      logs: logsQuery,
      incidents: incidentsQuery,
      incidentLogs: incidentLogsQuery,
      incidentArtifacts: incidentArtifactsQuery,
      eventDetail: eventDetailQuery,
      conversations: conversationsQuery,
      conversationDetail: conversationDetailQuery,
    },
    loaders: {
      loadEvents: () => runRefetch(eventsQuery.refetch),
      loadRuntimeLogs: () => runRefetch(runtimeLogsQuery.refetch),
      loadLogs: () => runRefetch(logsQuery.refetch),
      loadIncidents: () => runRefetch(incidentsQuery.refetch),
      loadIncidentLogs: () => runRefetch(incidentLogsQuery.refetch),
      loadIncidentArtifacts: () => runRefetch(incidentArtifactsQuery.refetch),
      loadEventDetail: () => runRefetch(eventDetailQuery.refetch),
      loadConversations: () => runRefetch(conversationsQuery.refetch),
      loadConversationDetail: () => runRefetch(conversationDetailQuery.refetch),
    },
  }), [
    conversationDetailQuery,
    conversationsQuery,
    eventDetailQuery,
    eventsQuery,
    incidentArtifactsQuery,
    incidentLogsQuery,
    incidentsQuery,
    logsQuery,
    runtimeLogsQuery,
  ]);
}
