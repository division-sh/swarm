import { useMemo } from "react";
import { useDashboardActionComposition } from "./useDashboardActionComposition.js";
import { useDashboardContractsState } from "./useDashboardContractsState.js";
import { useDashboardCoreQueries } from "./useDashboardCoreQueries.ts";
import { useDashboardDomainState } from "./useDashboardDomainState.js";
import { useDashboardLifecycle } from "./useDashboardLifecycle.js";
import { useDashboardOpsAssembly } from "./useDashboardOpsAssembly.js";
import { useDashboardOverviewAssembly } from "./useDashboardOverviewAssembly.ts";
import { useDashboardPipelineAssembly } from "./useDashboardPipelineAssembly.js";
import { useDashboardPipelineSources } from "./useDashboardPipelineSources.js";
import { useDashboardRuntimeAssembly } from "./useDashboardRuntimeAssembly.js";
import { useDashboardRuntimeSources } from "./useDashboardRuntimeSources.js";
import { useDashboardTabsState } from "./useDashboardTabsState.js";
import { useDashboardUIState } from "./useDashboardUIState.js";

export function useDashboardCoordinator() {
  const ui = useDashboardUIState();
  const domain = useDashboardDomainState();

  const core = useDashboardCoreQueries({
    taskStatus: domain.taskState.taskStatus,
    mailStatus: domain.opsState.mailStatus,
    controlTarget: domain.opsState.controlTarget,
    setControlTarget: domain.opsState.setControlTarget,
    setStatusText: ui.setStatusText,
  });
  const runtimeSources = useDashboardRuntimeSources({
    activeView: ui.activeView,
    activeSubview: ui.activeSubview,
    runtimeState: domain.runtimeState,
  });
  const pipelineSources = useDashboardPipelineSources({
    activeView: ui.activeView,
    activeSubview: ui.activeSubview,
    addToast: ui.addToast,
    pipelineState: domain.pipelineState,
  });
  const loaders = {
    ...core.loaders,
    ...runtimeSources,
    ...pipelineSources.loaders,
  };
  const refreshers = useMemo(() => ({
    loadEvents: loaders.loadEvents,
    loadRuntimeLogs: loaders.loadRuntimeLogs,
    loadConversations: loaders.loadConversations,
    loadIncidents: loaders.loadIncidents,
  }), [
    loaders.loadConversations,
    loaders.loadEvents,
    loaders.loadIncidents,
    loaders.loadRuntimeLogs,
  ]);

  useDashboardLifecycle({
    ui,
    runtimeState: domain.runtimeState,
    pipelineState: domain.pipelineState,
    workflowStream: pipelineSources.workflowStream,
    flowEvents: pipelineSources.data.flowEvents,
    refreshers,
    addToast: ui.addToast,
    loadLogs: loaders.loadLogs,
    loadRuntimeLogs: loaders.loadRuntimeLogs,
    loadIncidents: loaders.loadIncidents,
    loadConversations: loaders.loadConversations,
    loadGraph: loaders.loadGraph,
    loadPipelineFlow: loaders.loadPipelineFlow,
  });

  const { navigationActions, controlActions, taskActions } = useDashboardActionComposition({
    ui,
    taskState: domain.taskState,
    runtimeState: domain.runtimeState,
    opsState: domain.opsState,
    addToast: ui.addToast,
    loadAgents: loaders.loadAgents,
    loadTasks: loaders.loadTasks,
    loadTaskStats: loaders.loadTaskStats,
    loadEvents: loaders.loadEvents,
    loadMailbox: loaders.loadMailbox,
    loadTargets: loaders.loadTargets,
    loadOverview: loaders.loadOverview,
    loadFunnel: loaders.loadFunnel,
  });

  const { healthContracts, holdingViewState } = useDashboardContractsState({
    health: core.data.health,
    holdingData: pipelineSources.data.holdingData,
  });

  const runtime = useDashboardRuntimeAssembly({
    agentsResp: core.data.agentsResp,
    digestResp: core.data.digestResp,
    ui: {
      agentSearch: ui.agentSearch,
      setAgentSearch: ui.setAgentSearch,
      setModalContent: ui.setModalContent,
    },
    runtimeState: domain.runtimeState,
    runtimeData: runtimeSources.data,
    opsState: domain.opsState,
    loaders: {
      loadDigest: loaders.loadDigest,
      loadEvents: loaders.loadEvents,
      loadRuntimeLogs: loaders.loadRuntimeLogs,
      loadLogs: loaders.loadLogs,
      loadIncidents: loaders.loadIncidents,
      loadConversationDetail: loaders.loadConversationDetail,
    },
    navigationActions,
    addToast: ui.addToast,
  });

  const pipeline = useDashboardPipelineAssembly({
    agentsResp: core.data.agentsResp,
    pipelineState: domain.pipelineState,
    portfolioData: pipelineSources.data,
    workflowData: pipelineSources.data,
    loaders: {
      loadVerticals: loaders.loadVerticals,
      loadGraph: loaders.loadGraph,
      loadPipelineFlow: loaders.loadPipelineFlow,
      loadTrace: loaders.loadTrace,
      loadShardScanDetail: loaders.loadShardScanDetail,
      shardAction: loaders.shardAction,
      openHoldingVerticalDetail: loaders.openHoldingVerticalDetail,
    },
    addToast: ui.addToast,
    navigationActions,
    controlActions,
    holdingViewState,
  });

  const ops = useDashboardOpsAssembly({
    taskState: domain.taskState,
    opsState: domain.opsState,
    loaders: {
      loadTasks: loaders.loadTasks,
      loadTaskStats: loaders.loadTaskStats,
    },
    queryData: {
      targets: core.data.targets,
      mailbox: core.data.mailbox,
      health: core.data.health,
      tasksResp: core.data.tasksResp,
      tasksStats: core.data.tasksStats,
    },
    ui: {
      selectedMailboxItem: ui.selectedMailboxItem,
      setSelectedMailboxItem: ui.setSelectedMailboxItem,
    },
    taskActions,
    controlActions,
    healthContracts,
    openView: ui.setViewRoute,
  });

  const overview = useDashboardOverviewAssembly({
    overview: core.data.overview,
    digestResp: core.data.digestResp,
    agentsResp: core.data.agentsResp,
    incidentsData: runtimeSources.data.incidentsData,
    mailbox: core.data.mailbox,
    tasksResp: core.data.tasksResp,
    health: core.data.health,
    funnel: pipelineSources.data.funnel,
    holdingData: pipelineSources.data.holdingData,
    openView: ui.setViewRoute,
  });

  const { tabs, tabBadges } = useDashboardTabsState({
    agentsResp: core.data.agentsResp,
    incidentsData: runtimeSources.data.incidentsData,
    flowEvents: pipelineSources.data.flowEvents,
    mailbox: core.data.mailbox,
    funnel: pipelineSources.data.funnel,
    holdingData: pipelineSources.data.holdingData,
  });

  return {
    header: {
      initialLoading: ui.initialLoading || core.isInitialLoading,
      statusText: ui.statusText,
      apiKey: ui.apiKey,
      setApiKey: ui.setApiKey,
      tabs,
      tabBadges,
      activeView: ui.activeView,
      activeSubview: ui.activeSubview,
      setActiveView: ui.setActiveView,
      setViewRoute: ui.setViewRoute,
    },
    views: {
      overview,
      runtime,
      pipeline,
      ops,
    },
    modals: {
      modalContent: ui.modalContent,
      setModalContent: ui.setModalContent,
      holdingDetailModal: domain.pipelineState.holdingDetailModal,
      setHoldingDetailModal: domain.pipelineState.setHoldingDetailModal,
    },
    toasts: ui.toasts,
  };
}
