import React, { useCallback } from "react";
import AgentDropdown from "../features/agents/AgentDropdown.jsx";

export function useNavigationActions({
  addToast,
  loadAgents,
  loadTargets,
  setActiveView,
  setViewRoute,
  setModalContent,
  setControlTarget,
  setSelectedAgentID,
  setSelectedTaskID,
  setSelectedEventID,
  setSelectedConv,
  setEventsFilter,
  setEventsRuntimeErrorsOnly,
  setLogsFilter,
  setLogsRuntimeErrorsOnly,
}) {
  const routeForView = useCallback((view) => {
    switch (view) {
    case "events":
    case "logs":
    case "incidents":
      return { tab: "observability", subview: view };
    case "flow":
    case "graph":
      return { tab: "workflow", subview: view };
    case "pipeline":
    case "holding":
      return { tab: "portfolio", subview: view };
    case "control":
    case "tasks":
      return { tab: "operations", subview: view };
    default:
      return { tab: view, subview: "" };
    }
  }, []);

  const handleAgentNavigate = useCallback((view, opts) => {
    if (opts && opts.eventID) setSelectedEventID(opts.eventID);
    if (opts && opts.convID) {
      setSelectedConv(opts.convID);
      setSelectedAgentID(opts.agentID || opts.convID);
    }
    if (opts && opts.agentID) setSelectedAgentID(opts.agentID);
    if (opts && opts.eventsSubscriber) {
      setEventsFilter({ type: "", source: "", vertical: "", component: "", level: "", subscriber: opts.eventsSubscriber });
      setEventsRuntimeErrorsOnly(false);
    }
    if (opts && opts.logsAgent) {
      setLogsFilter({ type: "", source: opts.logsAgent, vertical: "", component: "", level: "", subscriber: "" });
      setLogsRuntimeErrorsOnly(false);
    }
    const route = routeForView(view);
    if (route.subview) setViewRoute(route.tab, route.subview);
    else setActiveView(route.tab);
  }, [
    setActiveView,
    setEventsFilter,
    setEventsRuntimeErrorsOnly,
    setLogsFilter,
    setLogsRuntimeErrorsOnly,
    setSelectedConv,
    setSelectedEventID,
    setSelectedAgentID,
    setViewRoute,
    routeForView,
  ]);

  const copyConversation = useCallback((agentID, messages) => {
    const msgs = messages || [];
    if (msgs.length === 0) {
      addToast("No messages to copy", "error");
      return;
    }
    const text = msgs.map((m) => {
      const role = m.role || "unknown";
      const label = agentID ? (role === "assistant" ? agentID : role === "user" ? "orchestrator" : role) : role;
      const content = typeof m.content === "string"
        ? m.content
        : Array.isArray(m.content)
          ? m.content.map((c) => c.text || c.type || "").join("\n")
          : JSON.stringify(m.content, null, 2);
      return `[${label}]\n${content}`;
    }).join("\n\n---\n\n");
    navigator.clipboard.writeText(text).then(() => addToast("Conversation copied", "success")).catch(() => addToast("Copy failed", "error"));
  }, [addToast]);

  const openControl = useCallback((agentID) => {
    setControlTarget(agentID);
    setViewRoute("operations", "control");
  }, [setControlTarget, setViewRoute]);

  const inspectAgent = useCallback((agentID) => {
    setSelectedAgentID(agentID);
    setActiveView("agents");
  }, [setActiveView, setSelectedAgentID]);

  const navigateToTask = useCallback((taskID) => {
    setSelectedTaskID(taskID);
    setViewRoute("operations", "tasks");
  }, [setSelectedTaskID, setViewRoute]);

  const renderAgentDropdown = useCallback((agent) => React.createElement(AgentDropdown, {
    agent,
    addToast,
    onNavigate: handleAgentNavigate,
    onOpenMessage: (message) => setModalContent({ title: `Message — ${message.role}`, text: message.text }),
    onCopyConversation: copyConversation,
    onAction: () => {
      loadAgents().catch(() => {});
      loadTargets().catch(() => {});
    },
  }), [addToast, copyConversation, handleAgentNavigate, loadAgents, loadTargets, setModalContent]);

  const openLogsForAgent = useCallback((agentID) => {
    setLogsFilter({ type: "", source: agentID, vertical: "", component: "", level: "", subscriber: "" });
    setLogsRuntimeErrorsOnly(false);
    setViewRoute("observability", "logs");
  }, [setLogsFilter, setLogsRuntimeErrorsOnly, setViewRoute]);

  const openConvoForAgent = useCallback((agentID) => {
    setSelectedConv(agentID);
    setSelectedAgentID(agentID);
    setViewRoute("agents");
  }, [setSelectedAgentID, setSelectedConv, setViewRoute]);

  return {
    handleAgentNavigate,
    openControl,
    inspectAgent,
    navigateToTask,
    renderAgentDropdown,
    copyConversation,
    openLogsForAgent,
    openConvoForAgent,
  };
}
