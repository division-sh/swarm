import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { fetchAgents } from "../api/agents.js";
import { getEmpireKey } from "../api/client.js";
import { useActionRunner } from "../hooks/useActionRunner.js";
import { useControlActions } from "../hooks/useControlActions.js";
import { useDashboardCoreData } from "../hooks/useDashboardCoreData.js";
import { useDashboardPipelineData } from "../hooks/useDashboardPipelineData.js";
import { useDashboardPolling } from "../hooks/useDashboardPolling.js";
import { useDashboardRuntimeData } from "../hooks/useDashboardRuntimeData.js";
import { useEventStream } from "../hooks/useEventStream.js";
import { useFlowRuntimeStream } from "../hooks/useFlowRuntimeStream.js";
import { useHashTab } from "../hooks/useHashTab.js";
import { useNavigationActions } from "../hooks/useNavigationActions.js";
import { usePersistentState } from "../hooks/usePersistentState.js";
import { useReplayTicker } from "../hooks/useReplayTicker.js";
import { useGroupedAgents } from "../features/agents/useGroupedAgents.js";
import { useEventsState } from "../features/events/useEventsState.js";
import { useFlowDerivedState } from "../features/flow/useFlowDerivedState.js";
import { useGraphSelection } from "../features/graph/useGraphSelection.js";
import { useHealthContracts } from "../features/health/useHealthContracts.js";
import { useHoldingViewState } from "../features/holding/useHoldingViewState.js";
import { useIncidentState } from "../features/incidents/useIncidentState.js";
import { useLogsState } from "../features/logs/useLogsState.js";
import { useSelectedTask } from "../features/tasks/useSelectedTask.js";
import { useTaskActions } from "../hooks/useTaskActions.js";
import { VALID_TABS } from "./dashboardTabs.js";
import { useDashboardDerivedState } from "./useDashboardDerivedState.js";
import { useDashboardOpsController } from "./useDashboardOpsController.js";
import { useDashboardPipelineController } from "./useDashboardPipelineController.js";
import { useDashboardRuntimeController } from "./useDashboardRuntimeController.js";
import {
  useDashboardOpsState,
  useDashboardPipelineState,
  useDashboardRuntimeState,
  useDashboardTaskState,
} from "./useDashboardStateBuckets.js";
import { relTime } from "../lib/format.js";

export function useDashboardCoordinator() {
  const [activeView, setActiveView] = useHashTab(VALID_TABS, "agents");
  const [statusText, setStatusText] = useState("Loading...");
  const [apiKey, setApiKey] = usePersistentState("empire_api_key", getEmpireKey());
  const [initialLoading, setInitialLoading] = useState(true);
  const [agentSearch, setAgentSearch] = useState("");
  const [selectedMailboxItem, setSelectedMailboxItem] = useState("");
  const [modalContent, setModalContent] = useState(null);

  const [overview, setOverview] = useState({});
  const [agentsResp, setAgentsResp] = useState({ agents: [], states: {} });
  const [digestResp, setDigestResp] = useState(null);
  const taskState = useDashboardTaskState();
  const runtimeState = useDashboardRuntimeState();
  const opsState = useDashboardOpsState();
  const pipelineState = useDashboardPipelineState();

  const {
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
  } = taskState;
  const {
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
  } = runtimeState;
  const {
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
  } = opsState;
  const {
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
  } = pipelineState;

  const [toasts, setToasts] = useState([]);
  const toastSeq = useRef(0);
  const addToast = useCallback((msg, type) => {
    const id = ++toastSeq.current;
    setToasts((prev) => [...prev, { id, msg, type: type || "info" }]);
    setTimeout(() => setToasts((prev) => prev.filter((toast) => toast.id !== id)), 4000);
  }, []);

  const loadAgents = useCallback(async () => {
    setAgentsResp(await fetchAgents());
  }, []);

  const {
    loadOverview,
    loadTasks,
    loadMailbox,
    loadDigest,
    loadHealth,
    loadTargets,
  } = useDashboardCoreData({
    setOverview,
    setStatusText,
    relTime,
    taskStatus,
    setTasksResp,
    mailStatus,
    setMailbox,
    setDigestResp,
    setHealth,
    setTargets,
    setControlTarget,
  });

  const {
    loadEvents,
    loadRuntimeLogs,
    loadLogs,
    loadIncidents,
    loadIncidentLogs,
    loadIncidentArtifacts,
    loadEventDetail,
    loadConversations,
    loadConversationDetail,
  } = useDashboardRuntimeData({
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
  });

  const {
    loadFunnel,
    loadShardScans,
    loadShardScanDetail,
    shardAction,
    loadTrace,
    loadHolding,
    openHoldingVerticalDetail,
    loadVerticals,
    loadGraph,
    loadPipelineFlow,
  } = useDashboardPipelineData({
    activeView,
    addToast,
    setFunnel,
    setShardScans,
    setShardScanDetails,
    selectedShardScanID,
    setSelectedShardScanID,
    shardScans,
    setTraceRows,
    setHoldingData,
    setHoldingDetailModal,
    graphMode,
    graphVertical,
    setGraphVertical,
    setVerticals,
    flowVertical,
    setFlowVertical,
    selectedGraphNodeID,
    setSelectedGraphNodeID,
    selectedGraphEdgeID,
    setSelectedGraphEdgeID,
    setGraph,
    flowView,
    flowStart,
    flowEnd,
    selectedFlowNodeID,
    setSelectedFlowNodeID,
    selectedFlowEdgeID,
    setSelectedFlowEdgeID,
    setFlowGraph,
    setFlowGraphMeta,
    setFlowEvents,
    setFlowReplayIndex,
    setFlowReplayOn,
  });

  const refreshAll = useCallback(async () => {
    await Promise.all([
      loadOverview(),
      loadAgents(),
      loadDigest(),
      loadTasks(),
      loadEvents(),
      loadRuntimeLogs(),
      loadConversations(),
      loadTargets(),
      loadFunnel(),
      loadShardScans(),
      loadMailbox(),
      loadHealth(),
      loadVerticals(),
      loadHolding(),
      loadIncidents(),
    ]);
  }, [loadOverview, loadAgents, loadDigest, loadTasks, loadEvents, loadRuntimeLogs, loadConversations, loadTargets, loadFunnel, loadShardScans, loadMailbox, loadHealth, loadVerticals, loadHolding, loadIncidents]);

  useEffect(() => {
    if (!graphFullscreen) return;
    function onKey(event) {
      if (event.key === "Escape") setGraphFullscreen(false);
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [graphFullscreen]);

  useEffect(() => {
    refreshAll()
      .catch((err) => { setStatusText(`Dashboard error: ${err.message}`); })
      .finally(() => setInitialLoading(false));
  }, [refreshAll]);

  useEventStream({
    eventsFilter,
    eventsIncludeRuntime,
    eventsRuntimeErrorsOnly,
    getKey: getEmpireKey,
    loadEvents,
    loadRuntimeLogs,
    addToast,
  });

  useFlowRuntimeStream({
    activeView,
    flowView,
    flowVertical,
    getKey: getEmpireKey,
    setFlowEvents,
  });

  useReplayTicker({
    flowView,
    flowReplayOn,
    flowReplaySpeed,
    flowEvents,
    setFlowReplayIndex,
    setFlowReplayOn,
  });

  useDashboardPolling({
    loadOverview,
    loadAgents,
    loadDigest,
    loadTasks,
    loadMailbox,
    loadHealth,
    loadRuntimeLogs,
    loadIncidents,
    loadTargets,
    loadFunnel,
    loadVerticals,
    loadHolding,
    activeView,
    flowView,
    loadPipelineFlow,
  });

  const groupedAgents = useGroupedAgents({
    agents: agentsResp.agents,
    agentSearch,
    selectedAgentID,
    setSelectedAgentID,
  });

  const { filteredEvents, filteredRuntimeLogs } = useEventsState({
    events,
    runtimeLogs,
    eventsRuntimeErrorsOnly,
  });

  const { filteredLogsData, selectedLog } = useLogsState({
    logsData,
    logsRuntimeErrorsOnly,
    selectedLogID,
    setSelectedLogID,
  });

  const selectedIncident = useIncidentState({
    incidentsData,
    selectedIncidentCode,
  });

  useGraphSelection({
    graph,
    graphViewGraph,
    selectedGraphNodeID,
    setSelectedGraphNodeID,
    selectedGraphEdgeID,
    setSelectedGraphEdgeID,
  });

  const flowDerived = useFlowDerivedState({
    flowGraphMeta,
    flowEvents,
    flowView,
    flowReplayIndex,
    flowStage,
    flowRubric,
    flowGraph,
    flowViewGraph,
    selectedFlowNodeID,
    setSelectedFlowNodeID,
    selectedFlowEdgeID,
    setSelectedFlowEdgeID,
  });

  const selectedTask = useSelectedTask({
    tasks: tasksResp.tasks,
    selectedTaskID,
  });

  const healthContracts = useHealthContracts({ health });
  const holdingViewState = useHoldingViewState({
    holdingData,
    validationGateData: healthContracts.validationGateData,
    contractWorkflow: healthContracts.contractWorkflow,
  });

  const derived = useDashboardDerivedState({
    agentsResp,
    incidentsData,
    flowEvents,
    mailbox,
    funnel,
    holdingData,
  });

  const refreshAfterControl = useMemo(
    () => async () => Promise.all([loadAgents(), loadTasks(), loadEvents(), loadMailbox(), loadTargets(), loadOverview(), loadFunnel()]),
    [loadAgents, loadEvents, loadFunnel, loadMailbox, loadOverview, loadTargets, loadTasks],
  );

  const { runControl } = useActionRunner({
    addToast,
    setControlOutput,
    refreshAfterControl,
  });

  const navigationActions = useNavigationActions({
    addToast,
    loadAgents,
    loadTargets,
    setActiveView,
    setControlTarget,
    setSelectedAgentID,
    setSelectedTaskID,
    setSelectedEventID,
    setSelectedConv,
    setEventsFilter,
    setEventsRuntimeErrorsOnly,
    setLogsFilter,
    setLogsRuntimeErrorsOnly,
  });

  const controlActions = useControlActions({
    addToast,
    runControl,
    loadAgents,
    loadMailbox,
  });

  const taskActions = useTaskActions({
    runControl,
    loadTasks,
    selectedTaskID,
    taskResultText,
    setTaskResultText,
    taskOutcome,
    setTaskOutcome,
    taskFollowUpNeeded,
    setTaskFollowUpNeeded,
    taskRejectReason,
    setTaskRejectReason,
    setTasksStats,
  });

  const runtime = useDashboardRuntimeController({
    agentsResp,
    groupedAgents,
    agentSearch,
    setAgentSearch,
    selectedAgentID,
    setSelectedAgentID,
    renderAgentDropdown: navigationActions.renderAgentDropdown,
    navigateToTask: navigationActions.navigateToTask,
    digestResp,
    loadDigest,
    addToast,
    filteredEvents,
    filteredRuntimeLogs,
    eventDetail,
    eventsFilter,
    setEventsFilter,
    eventsIncludeRuntime,
    setEventsIncludeRuntime,
    eventsRuntimeErrorsOnly,
    setEventsRuntimeErrorsOnly,
    selectedEventID,
    setSelectedEventID,
    loadEvents,
    loadRuntimeLogs,
    filteredLogsData,
    selectedLog,
    logsFilter,
    setLogsFilter,
    logsRuntimeErrorsOnly,
    setLogsRuntimeErrorsOnly,
    logsOrder,
    setLogsOrder,
    selectedLogID,
    setSelectedLogID,
    loadLogs,
    incidentsData,
    selectedIncident,
    incidentArtifacts,
    incidentLogs,
    incidentsFilter,
    setIncidentsFilter,
    selectedIncidentCode,
    setSelectedIncidentCode,
    selectedIncidentAgent,
    setSelectedIncidentAgent,
    loadIncidents,
    openLogsForAgent: navigationActions.openLogsForAgent,
    openConvoForAgent: navigationActions.openConvoForAgent,
    conversations,
    conversationDetail,
    selectedConv,
    setSelectedConv,
    loadConversationDetail,
    copyConversation: navigationActions.copyConversation,
    setModalContent,
  });

  const pipeline = useDashboardPipelineController({
    verticals,
    visibleFlowEvents: flowDerived.visibleFlowEvents,
    flowEvents,
    flowGraph,
    flowGraphMeta,
    flowActiveEdgeKeys: flowDerived.flowActiveEdgeKeys,
    selectedFlowSummary: flowDerived.selectedFlowSummary,
    agentsResp,
    flowView,
    setFlowView,
    flowStage,
    setFlowStage,
    flowStageOptions: flowDerived.flowStageOptions,
    flowRubric,
    setFlowRubric,
    flowRubricOptions: flowDerived.flowRubricOptions,
    flowVertical,
    setFlowVertical,
    flowStart,
    setFlowStart,
    flowEnd,
    setFlowEnd,
    flowReplaySpeed,
    setFlowReplaySpeed,
    flowReplayOn,
    setFlowReplayOn,
    flowReplayIndex,
    setFlowReplayIndex,
    loadPipelineFlow,
    addToast,
    selectedFlowNodeID,
    setSelectedFlowNodeID,
    selectedFlowEdgeID,
    setSelectedFlowEdgeID,
    flowViewGraph,
    setFlowViewGraph,
    graphFullscreen,
    setGraphFullscreen,
    graph,
    graphViewGraph,
    graphMode,
    setGraphMode,
    graphVertical,
    setGraphVertical,
    selectedGraphNodeID,
    setSelectedGraphNodeID,
    selectedGraphEdgeID,
    setSelectedGraphEdgeID,
    loadVerticals,
    loadGraph,
    restartAgent: controlActions.restartAgent,
    openControl: navigationActions.openControl,
    inspectAgent: navigationActions.inspectAgent,
    navigateToTask: navigationActions.navigateToTask,
    funnel,
    shardScans,
    shardScanDetails,
    traceRows,
    traceVertical,
    setTraceVertical,
    selectedShardScanID,
    setSelectedShardScanID,
    loadTrace,
    loadShardScanDetail,
    shardAction,
    holdingViewState,
    openHoldingVerticalDetail,
  });

  const ops = useDashboardOpsController({
    targets,
    mailbox,
    controlOutput,
    controlTarget,
    setControlTarget,
    directiveMessage,
    setDirectiveMessage,
    chatMessage,
    setChatMessage,
    chatMode,
    setChatMode,
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
    resetConfirm,
    setResetConfirm,
    mailStatus,
    setMailStatus,
    mailboxID,
    setMailboxID,
    mailboxAction,
    setMailboxAction,
    mailboxNotes,
    setMailboxNotes,
    selectedMailboxItem,
    setSelectedMailboxItem,
    sendDirective: controlActions.sendDirective,
    sendChat: controlActions.sendChat,
    restartControlTarget: controlActions.restartControlTarget,
    replayControlTarget: controlActions.replayControlTarget,
    createVertical: controlActions.createVertical,
    requeueEvent: controlActions.requeueEvent,
    seedOrg: controlActions.seedOrg,
    pauseRuntime: controlActions.pauseRuntime,
    resumeRuntime: controlActions.resumeRuntime,
    resetDBAndSeed: controlActions.resetDBAndSeed,
    wipeDB: controlActions.wipeDB,
    decideMailbox: controlActions.decideMailbox,
    quickMailboxDecide: controlActions.quickMailboxDecide,
    tasksResp,
    tasksStats,
    selectedTask,
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
    loadTasks,
    loadTaskStats: taskActions.loadTaskStats,
    claimSelectedTask: taskActions.claimSelectedTask,
    completeSelectedTask: taskActions.completeSelectedTask,
    rejectSelectedTask: taskActions.rejectSelectedTask,
    health,
    contractsData: healthContracts.contractsData,
    contractWorkflow: healthContracts.contractWorkflow,
    contractPlatform: healthContracts.contractPlatform,
    contractVerification: healthContracts.contractVerification,
  });

  return {
    header: {
      initialLoading,
      statusText,
      apiKey,
      setApiKey,
      overview,
      stuckAgents: agentsResp.states.stuck || 0,
      tabs: derived.tabs,
      tabBadges: derived.tabBadges,
      activeView,
      setActiveView,
    },
    views: {
      runtime,
      pipeline,
      ops,
    },
    modals: {
      modalContent,
      setModalContent,
      holdingDetailModal,
      setHoldingDetailModal,
    },
    toasts,
  };
}
