import { useMemo, useState } from "react";
import type { EventFilter, IncidentFilter, LogFilter } from "../types/runtime.ts";
import type { GraphResponse } from "../types/workflow.ts";

export function useDashboardTaskState() {
  const [taskStatus, setTaskStatus] = useState("open");
  const [selectedTaskID, setSelectedTaskID] = useState("");
  const [taskResultText, setTaskResultText] = useState("");
  const [taskOutcome, setTaskOutcome] = useState("success");
  const [taskFollowUpNeeded, setTaskFollowUpNeeded] = useState(false);
  const [taskRejectReason, setTaskRejectReason] = useState("");

  return useMemo(() => ({
    taskStatus,
    setTaskStatus,
    selectedTaskID,
    setSelectedTaskID,
    taskResultText,
    setTaskResultText,
    taskOutcome,
    setTaskOutcome,
    taskFollowUpNeeded,
    setTaskFollowUpNeeded,
    taskRejectReason,
    setTaskRejectReason,
  }), [
    taskStatus,
    selectedTaskID,
    taskResultText,
    taskOutcome,
    taskFollowUpNeeded,
    taskRejectReason,
  ]);
}

export function useDashboardRuntimeState() {
  const [eventsFilter, setEventsFilter] = useState<EventFilter>({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
  const [eventsIncludeRuntime, setEventsIncludeRuntime] = useState(true);
  const [eventsRuntimeErrorsOnly, setEventsRuntimeErrorsOnly] = useState(false);
  const [selectedEventID, setSelectedEventID] = useState("");
  const [selectedConv, setSelectedConv] = useState("");
  const [logsFilter, setLogsFilter] = useState<LogFilter>({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
  const [logsRuntimeErrorsOnly, setLogsRuntimeErrorsOnly] = useState(false);
  const [selectedLogID, setSelectedLogID] = useState<string | null>(null);
  const [logsOrder, setLogsOrder] = useState("desc");
  const [incidentsFilter, setIncidentsFilter] = useState<IncidentFilter>({ sinceHours: 24, mcpOnly: true, level: "warn", component: "" });
  const [selectedIncidentCode, setSelectedIncidentCode] = useState("");
  const [selectedIncidentAgent, setSelectedIncidentAgent] = useState("");

  return useMemo(() => ({
    eventsFilter,
    setEventsFilter,
    eventsIncludeRuntime,
    setEventsIncludeRuntime,
    eventsRuntimeErrorsOnly,
    setEventsRuntimeErrorsOnly,
    selectedEventID,
    setSelectedEventID,
    selectedConv,
    setSelectedConv,
    logsFilter,
    setLogsFilter,
    logsRuntimeErrorsOnly,
    setLogsRuntimeErrorsOnly,
    selectedLogID,
    setSelectedLogID,
    logsOrder,
    setLogsOrder,
    incidentsFilter,
    setIncidentsFilter,
    selectedIncidentCode,
    setSelectedIncidentCode,
    selectedIncidentAgent,
    setSelectedIncidentAgent,
  }), [
    eventsFilter,
    eventsIncludeRuntime,
    eventsRuntimeErrorsOnly,
    selectedEventID,
    selectedConv,
    logsFilter,
    logsRuntimeErrorsOnly,
    selectedLogID,
    logsOrder,
    incidentsFilter,
    selectedIncidentCode,
    selectedIncidentAgent,
  ]);
}

export function useDashboardOpsState() {
  const [mailStatus, setMailStatus] = useState("all");
  const [selectedAgentID, setSelectedAgentID] = useState("");
  const [controlTarget, setControlTarget] = useState("");
  const [directiveMessage, setDirectiveMessage] = useState("");
  const [chatMode, setChatMode] = useState("live");
  const [chatMessage, setChatMessage] = useState("");
  const [verticalName, setVerticalName] = useState("");
  const [verticalGeo, setVerticalGeo] = useState("");
  const [verticalSlug, setVerticalSlug] = useState("");
  const [requeueEventID, setRequeueEventID] = useState("");
  const [requeueAgentID, setRequeueAgentID] = useState("");
  const [mailboxID, setMailboxID] = useState("");
  const [mailboxAction, setMailboxAction] = useState("approve");
  const [mailboxNotes, setMailboxNotes] = useState("");
  const [resetConfirm, setResetConfirm] = useState("");
  const [controlOutput, setControlOutput] = useState<Record<string, any>>({ ok: true, message: "Ready." });

  return useMemo(() => ({
    mailStatus,
    setMailStatus,
    selectedAgentID,
    setSelectedAgentID,
    controlTarget,
    setControlTarget,
    directiveMessage,
    setDirectiveMessage,
    chatMode,
    setChatMode,
    chatMessage,
    setChatMessage,
    verticalName,
    setVerticalName,
    verticalGeo,
    setVerticalGeo,
    verticalSlug,
    setVerticalSlug,
    requeueEventID,
    setRequeueEventID,
    requeueAgentID,
    setRequeueAgentID,
    mailboxID,
    setMailboxID,
    mailboxAction,
    setMailboxAction,
    mailboxNotes,
    setMailboxNotes,
    resetConfirm,
    setResetConfirm,
    controlOutput,
    setControlOutput,
  }), [
    mailStatus,
    selectedAgentID,
    controlTarget,
    directiveMessage,
    chatMode,
    chatMessage,
    verticalName,
    verticalGeo,
    verticalSlug,
    requeueEventID,
    requeueAgentID,
    mailboxID,
    mailboxAction,
    mailboxNotes,
    resetConfirm,
    controlOutput,
  ]);
}

export function useDashboardPipelineState() {
  const [selectedShardScanID, setSelectedShardScanID] = useState("");
  const [traceVertical, setTraceVertical] = useState("");
  const [graphMode, setGraphMode] = useState("holding");
  const [graphVertical, setGraphVertical] = useState("");
  const [graphViewGraph, setGraphViewGraph] = useState<GraphResponse | null>(null);
  const [selectedGraphNodeID, setSelectedGraphNodeID] = useState("");
  const [selectedGraphEdgeID, setSelectedGraphEdgeID] = useState("");
  const [graphFullscreen, setGraphFullscreen] = useState(false);
  const [flowView, setFlowView] = useState("design");
  const [flowVertical, setFlowVertical] = useState("");
  const [selectedFlowNodeID, setSelectedFlowNodeID] = useState("");
  const [selectedFlowEdgeID, setSelectedFlowEdgeID] = useState("");
  const [flowViewGraph, setFlowViewGraph] = useState<GraphResponse | null>(null);
  const [flowStart, setFlowStart] = useState("");
  const [flowEnd, setFlowEnd] = useState("");
  const [flowStage, setFlowStage] = useState("all");
  const [flowRubric, setFlowRubric] = useState("all");
  const [flowReplaySpeed, setFlowReplaySpeed] = useState(10);
  const [flowReplayOn, setFlowReplayOn] = useState(false);
  const [flowReplayIndex, setFlowReplayIndex] = useState(0);
  const [holdingDetailModal, setHoldingDetailModal] = useState({
    open: false,
    loading: false,
    id: "",
    error: "",
    data: null as Record<string, any> | null,
  });

  return useMemo(() => ({
    selectedShardScanID,
    setSelectedShardScanID,
    traceVertical,
    setTraceVertical,
    graphMode,
    setGraphMode,
    graphVertical,
    setGraphVertical,
    graphViewGraph,
    setGraphViewGraph,
    selectedGraphNodeID,
    setSelectedGraphNodeID,
    selectedGraphEdgeID,
    setSelectedGraphEdgeID,
    graphFullscreen,
    setGraphFullscreen,
    flowView,
    setFlowView,
    flowVertical,
    setFlowVertical,
    selectedFlowNodeID,
    setSelectedFlowNodeID,
    selectedFlowEdgeID,
    setSelectedFlowEdgeID,
    flowViewGraph,
    setFlowViewGraph,
    flowStart,
    setFlowStart,
    flowEnd,
    setFlowEnd,
    flowStage,
    setFlowStage,
    flowRubric,
    setFlowRubric,
    flowReplaySpeed,
    setFlowReplaySpeed,
    flowReplayOn,
    setFlowReplayOn,
    flowReplayIndex,
    setFlowReplayIndex,
    holdingDetailModal,
    setHoldingDetailModal,
  }), [
    selectedShardScanID,
    traceVertical,
    graphMode,
    graphVertical,
    graphViewGraph,
    selectedGraphNodeID,
    selectedGraphEdgeID,
    graphFullscreen,
    flowView,
    flowVertical,
    selectedFlowNodeID,
    selectedFlowEdgeID,
    flowViewGraph,
    flowStart,
    flowEnd,
    flowStage,
    flowRubric,
    flowReplaySpeed,
    flowReplayOn,
    flowReplayIndex,
    holdingDetailModal,
  ]);
}
