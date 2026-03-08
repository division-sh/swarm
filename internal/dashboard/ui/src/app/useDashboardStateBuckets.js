import { useState } from "react";

export function useDashboardTaskState() {
  const [taskStatus, setTaskStatus] = useState("open");
  const [tasksResp, setTasksResp] = useState({ tasks: [] });
  const [selectedTaskID, setSelectedTaskID] = useState("");
  const [taskResultText, setTaskResultText] = useState("");
  const [taskOutcome, setTaskOutcome] = useState("success");
  const [taskFollowUpNeeded, setTaskFollowUpNeeded] = useState(false);
  const [taskRejectReason, setTaskRejectReason] = useState("");
  const [tasksStats, setTasksStats] = useState(null);

  return {
    taskStatus,
    setTaskStatus,
    tasksResp,
    setTasksResp,
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
    tasksStats,
    setTasksStats,
  };
}

export function useDashboardRuntimeState() {
  const [eventsFilter, setEventsFilter] = useState({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
  const [eventsIncludeRuntime, setEventsIncludeRuntime] = useState(true);
  const [eventsRuntimeErrorsOnly, setEventsRuntimeErrorsOnly] = useState(false);
  const [events, setEvents] = useState([]);
  const [runtimeLogs, setRuntimeLogs] = useState([]);
  const [selectedEventID, setSelectedEventID] = useState("");
  const [eventDetail, setEventDetail] = useState(null);
  const [conversations, setConversations] = useState([]);
  const [selectedConv, setSelectedConv] = useState("");
  const [conversationDetail, setConversationDetail] = useState({ messages: [], turns: [] });
  const [logsFilter, setLogsFilter] = useState({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
  const [logsRuntimeErrorsOnly, setLogsRuntimeErrorsOnly] = useState(false);
  const [logsData, setLogsData] = useState([]);
  const [selectedLogID, setSelectedLogID] = useState(null);
  const [logsOrder, setLogsOrder] = useState("desc");
  const [incidentsFilter, setIncidentsFilter] = useState({ sinceHours: 24, mcpOnly: true, level: "warn", component: "" });
  const [incidentsData, setIncidentsData] = useState([]);
  const [selectedIncidentCode, setSelectedIncidentCode] = useState("");
  const [selectedIncidentAgent, setSelectedIncidentAgent] = useState("");
  const [incidentLogs, setIncidentLogs] = useState([]);
  const [incidentArtifacts, setIncidentArtifacts] = useState({ loading: false, error: "", data: null });

  return {
    eventsFilter,
    setEventsFilter,
    eventsIncludeRuntime,
    setEventsIncludeRuntime,
    eventsRuntimeErrorsOnly,
    setEventsRuntimeErrorsOnly,
    events,
    setEvents,
    runtimeLogs,
    setRuntimeLogs,
    selectedEventID,
    setSelectedEventID,
    eventDetail,
    setEventDetail,
    conversations,
    setConversations,
    selectedConv,
    setSelectedConv,
    conversationDetail,
    setConversationDetail,
    logsFilter,
    setLogsFilter,
    logsRuntimeErrorsOnly,
    setLogsRuntimeErrorsOnly,
    logsData,
    setLogsData,
    selectedLogID,
    setSelectedLogID,
    logsOrder,
    setLogsOrder,
    incidentsFilter,
    setIncidentsFilter,
    incidentsData,
    setIncidentsData,
    selectedIncidentCode,
    setSelectedIncidentCode,
    selectedIncidentAgent,
    setSelectedIncidentAgent,
    incidentLogs,
    setIncidentLogs,
    incidentArtifacts,
    setIncidentArtifacts,
  };
}

export function useDashboardOpsState() {
  const [mailStatus, setMailStatus] = useState("all");
  const [mailbox, setMailbox] = useState({ summary: {}, items: [] });
  const [health, setHealth] = useState({});
  const [targets, setTargets] = useState([]);
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
  const [controlOutput, setControlOutput] = useState({ ok: true, message: "Ready." });

  return {
    mailStatus,
    setMailStatus,
    mailbox,
    setMailbox,
    health,
    setHealth,
    targets,
    setTargets,
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
  };
}

export function useDashboardPipelineState() {
  const [funnel, setFunnel] = useState({ throughput: {}, stuck: [] });
  const [shardScans, setShardScans] = useState([]);
  const [selectedShardScanID, setSelectedShardScanID] = useState("");
  const [shardScanDetails, setShardScanDetails] = useState({});
  const [traceVertical, setTraceVertical] = useState("");
  const [traceRows, setTraceRows] = useState([]);
  const [verticals, setVerticals] = useState([]);
  const [graphMode, setGraphMode] = useState("holding");
  const [graphVertical, setGraphVertical] = useState("");
  const [graph, setGraph] = useState({ nodes: [], edges: [] });
  const [graphViewGraph, setGraphViewGraph] = useState(null);
  const [selectedGraphNodeID, setSelectedGraphNodeID] = useState("");
  const [selectedGraphEdgeID, setSelectedGraphEdgeID] = useState("");
  const [graphFullscreen, setGraphFullscreen] = useState(false);
  const [flowView, setFlowView] = useState("design");
  const [flowVertical, setFlowVertical] = useState("");
  const [flowGraph, setFlowGraph] = useState({ nodes: [], edges: [] });
  const [flowGraphMeta, setFlowGraphMeta] = useState({});
  const [flowEvents, setFlowEvents] = useState([]);
  const [selectedFlowNodeID, setSelectedFlowNodeID] = useState("");
  const [selectedFlowEdgeID, setSelectedFlowEdgeID] = useState("");
  const [flowViewGraph, setFlowViewGraph] = useState(null);
  const [flowStart, setFlowStart] = useState("");
  const [flowEnd, setFlowEnd] = useState("");
  const [flowStage, setFlowStage] = useState("all");
  const [flowRubric, setFlowRubric] = useState("all");
  const [flowReplaySpeed, setFlowReplaySpeed] = useState(10);
  const [flowReplayOn, setFlowReplayOn] = useState(false);
  const [flowReplayIndex, setFlowReplayIndex] = useState(0);
  const [holdingData, setHoldingData] = useState({ campaigns: [], verticals: [], agent_counts: {}, summary: {}, workflow_summary: {} });
  const [holdingDetailModal, setHoldingDetailModal] = useState({
    open: false,
    loading: false,
    id: "",
    error: "",
    data: null,
  });

  return {
    funnel,
    setFunnel,
    shardScans,
    setShardScans,
    selectedShardScanID,
    setSelectedShardScanID,
    shardScanDetails,
    setShardScanDetails,
    traceVertical,
    setTraceVertical,
    traceRows,
    setTraceRows,
    verticals,
    setVerticals,
    graphMode,
    setGraphMode,
    graphVertical,
    setGraphVertical,
    graph,
    setGraph,
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
    flowGraph,
    setFlowGraph,
    flowGraphMeta,
    setFlowGraphMeta,
    flowEvents,
    setFlowEvents,
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
    holdingData,
    setHoldingData,
    holdingDetailModal,
    setHoldingDetailModal,
  };
}
