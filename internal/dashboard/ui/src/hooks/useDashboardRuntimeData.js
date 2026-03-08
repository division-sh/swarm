import { useCallback, useEffect } from "react";
import { fetchJSON } from "../api/client.js";

export function useDashboardRuntimeData({
  activeView,
  addToast,
  eventsFilter,
  eventsRuntimeErrorsOnly,
  setEvents,
  setRuntimeLogs,
  logsFilter,
  logsOrder,
  logsRuntimeErrorsOnly,
  setLogsData,
  incidentsFilter,
  setIncidentsData,
  setSelectedIncidentCode,
  selectedIncidentCode,
  setSelectedIncidentAgent,
  selectedIncidentAgent,
  setIncidentLogs,
  setIncidentArtifacts,
  selectedConv,
  setConversations,
  setSelectedConv,
  setConversationDetail,
  selectedEventID,
  setEventDetail,
}) {
  const loadEvents = useCallback(async () => {
    const p = new URLSearchParams();
    if (eventsFilter.type) p.set("type", eventsFilter.type);
    if (eventsFilter.source) p.set("source", eventsFilter.source);
    if (eventsFilter.vertical) p.set("vertical", eventsFilter.vertical);
    if (eventsFilter.subscriber) p.set("subscriber", eventsFilter.subscriber);
    p.set("limit", "200");
    const d = await fetchJSON(`/api/events?${p.toString()}`);
    setEvents(d.events || []);
  }, [eventsFilter, setEvents]);

  const loadRuntimeLogs = useCallback(async () => {
    const p = new URLSearchParams();
    if (eventsFilter.type) p.set("type", eventsFilter.type);
    if (eventsFilter.subscriber) p.set("source", eventsFilter.subscriber);
    else if (eventsFilter.source) p.set("source", eventsFilter.source);
    if (eventsFilter.vertical) p.set("vertical", eventsFilter.vertical);
    if (eventsFilter.component) p.set("component", eventsFilter.component);
    if (eventsFilter.level) p.set("level", eventsFilter.level);
    else if (eventsRuntimeErrorsOnly) p.set("level", "error");
    p.set("limit", "200");
    const d = await fetchJSON(`/api/runtime/logs?${p.toString()}`);
    setRuntimeLogs(d.runtime_logs || []);
  }, [eventsFilter, eventsRuntimeErrorsOnly, setRuntimeLogs]);

  const loadLogs = useCallback(async () => {
    const p = new URLSearchParams();
    if (logsFilter.type) p.set("type", logsFilter.type);
    if (logsFilter.subscriber) p.set("source", logsFilter.subscriber);
    else if (logsFilter.source) p.set("source", logsFilter.source);
    if (logsFilter.vertical) p.set("vertical", logsFilter.vertical);
    if (logsFilter.component) p.set("component", logsFilter.component);
    if (logsFilter.level) p.set("level", logsFilter.level);
    else if (logsRuntimeErrorsOnly) p.set("level", "error");
    p.set("order", logsOrder);
    p.set("limit", "200");
    const d = await fetchJSON(`/api/runtime/logs?${p.toString()}`);
    setLogsData(d.runtime_logs || []);
  }, [logsFilter, logsOrder, logsRuntimeErrorsOnly, setLogsData]);

  const loadIncidents = useCallback(async () => {
    const p = new URLSearchParams();
    p.set("since_hours", String(Math.max(1, Number(incidentsFilter.sinceHours || 24))));
    p.set("mcp_only", incidentsFilter.mcpOnly ? "true" : "false");
    if (incidentsFilter.level) p.set("level", incidentsFilter.level);
    if (incidentsFilter.component) p.set("component", incidentsFilter.component);
    p.set("limit", "2000");
    const d = await fetchJSON(`/api/runtime/incidents?${p.toString()}`);
    const items = d.incidents || [];
    setIncidentsData(items);
    setSelectedIncidentCode((cur) => {
      if (!cur) return items.length > 0 ? items[0].code : "";
      const exists = items.some((it) => it.code === cur);
      return exists ? cur : (items.length > 0 ? items[0].code : "");
    });
  }, [incidentsFilter, setIncidentsData, setSelectedIncidentCode]);

  const loadIncidentLogs = useCallback(async (code) => {
    const c = (code || "").trim();
    if (!c) {
      setIncidentLogs([]);
      return;
    }
    const p = new URLSearchParams();
    p.set("error_code", c);
    p.set("order", "desc");
    p.set("limit", "250");
    const d = await fetchJSON(`/api/runtime/logs?${p.toString()}`);
    const rows = d.runtime_logs || [];
    setIncidentLogs(rows);
    setSelectedIncidentAgent((cur) => {
      if (cur && rows.some((r) => r.agent_id === cur)) return cur;
      const first = rows.find((r) => (r.agent_id || "").trim() !== "");
      return first ? first.agent_id : "";
    });
  }, [setIncidentLogs, setSelectedIncidentAgent]);

  const loadIncidentArtifacts = useCallback(async (agentID) => {
    const id = (agentID || "").trim();
    if (!id) {
      setIncidentArtifacts({ loading: false, error: "", data: null });
      return;
    }
    setIncidentArtifacts({ loading: true, error: "", data: null });
    try {
      const d = await fetchJSON(`/dashboard/api/conversations/${encodeURIComponent(id)}/artifacts?lines=120`);
      setIncidentArtifacts({ loading: false, error: "", data: d || null });
    } catch (err) {
      setIncidentArtifacts({
        loading: false,
        error: (err && err.message) ? err.message : "failed to load artifacts",
        data: null,
      });
    }
  }, [setIncidentArtifacts]);

  const loadEventDetail = useCallback(async (id) => {
    if (!id) {
      setEventDetail(null);
      return;
    }
    const d = await fetchJSON(`/api/events/${encodeURIComponent(id)}`);
    setEventDetail(d);
  }, [setEventDetail]);

  const loadConversations = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/conversations?limit=100");
    const items = d.conversations || [];
    setConversations(items);
    if (items.length > 0) {
      setSelectedConv((cur) => cur || items[0].agent_id);
    }
  }, [setConversations, setSelectedConv]);

  const loadConversationDetail = useCallback(async (agentID) => {
    if (!agentID) {
      setConversationDetail({ messages: [], turns: [] });
      return;
    }
    const d = await fetchJSON(`/dashboard/api/conversations/${encodeURIComponent(agentID)}`);
    setConversationDetail({ messages: d.messages || [], turns: d.turns || [] });
  }, [setConversationDetail]);

  useEffect(() => {
    if (activeView !== "logs") return;
    loadLogs().catch(() => {});
  }, [activeView, loadLogs]);

  useEffect(() => {
    if (activeView !== "incidents") return;
    loadIncidents().catch(() => {});
  }, [activeView, loadIncidents]);

  useEffect(() => {
    if (activeView !== "incidents") return;
    loadIncidentLogs(selectedIncidentCode).catch(() => {});
  }, [activeView, selectedIncidentCode, loadIncidentLogs]);

  useEffect(() => {
    if (activeView !== "incidents") return;
    loadIncidentArtifacts(selectedIncidentAgent).catch(() => {});
  }, [activeView, selectedIncidentAgent, loadIncidentArtifacts]);

  useEffect(() => {
    if (selectedConv) {
      loadConversationDetail(selectedConv).catch((err) => addToast(err.message, "error"));
    }
  }, [addToast, loadConversationDetail, selectedConv]);

  useEffect(() => {
    if (selectedEventID) {
      loadEventDetail(selectedEventID).catch((err) => addToast(err.message, "error"));
    }
  }, [addToast, loadEventDetail, selectedEventID]);

  return {
    loadEvents,
    loadRuntimeLogs,
    loadLogs,
    loadIncidents,
    loadIncidentLogs,
    loadIncidentArtifacts,
    loadEventDetail,
    loadConversations,
    loadConversationDetail,
  };
}
