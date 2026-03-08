import { useDashboardActionComposition } from "./useDashboardActionComposition.js";
import { useDashboardContractsState } from "./useDashboardContractsState.js";
import { useDashboardCoreSources } from "./useDashboardCoreSources.js";
import { useDashboardDomainState } from "./useDashboardDomainState.js";
import { useDashboardLifecycle } from "./useDashboardLifecycle.js";
import { useDashboardOpsAssembly } from "./useDashboardOpsAssembly.js";
import { useDashboardPipelineAssembly } from "./useDashboardPipelineAssembly.js";
import { useDashboardPipelineSources } from "./useDashboardPipelineSources.js";
import { useDashboardRuntimeAssembly } from "./useDashboardRuntimeAssembly.js";
import { useDashboardRuntimeSources } from "./useDashboardRuntimeSources.js";
import { useDashboardTabsState } from "./useDashboardTabsState.js";
import { useDashboardUIState } from "./useDashboardUIState.js";

export function useDashboardCoordinator() {
  const ui = useDashboardUIState();
  const domain = useDashboardDomainState();

  const core = useDashboardCoreSources({
    ui,
    domain,
  });
  const runtimeSources = useDashboardRuntimeSources({
    activeView: ui.activeView,
    addToast: ui.addToast,
    runtimeState: domain.runtimeState,
  });
  const pipelineSources = useDashboardPipelineSources({
    activeView: ui.activeView,
    addToast: ui.addToast,
    pipelineState: domain.pipelineState,
  });
  const loaders = {
    ...core,
    ...runtimeSources,
    ...pipelineSources,
  };

  useDashboardLifecycle({
    ui,
    runtimeState: domain.runtimeState,
    pipelineState: domain.pipelineState,
    refreshers: {
      loadOverview: loaders.loadOverview,
      loadAgents: loaders.loadAgents,
      loadDigest: loaders.loadDigest,
      loadTasks: loaders.loadTasks,
      loadEvents: loaders.loadEvents,
      loadRuntimeLogs: loaders.loadRuntimeLogs,
      loadConversations: loaders.loadConversations,
      loadTargets: loaders.loadTargets,
      loadFunnel: loaders.loadFunnel,
      loadShardScans: loaders.loadShardScans,
      loadMailbox: loaders.loadMailbox,
      loadHealth: loaders.loadHealth,
      loadVerticals: loaders.loadVerticals,
      loadHolding: loaders.loadHolding,
      loadIncidents: loaders.loadIncidents,
    },
    addToast: ui.addToast,
    loadOverview: loaders.loadOverview,
    loadAgents: loaders.loadAgents,
    loadDigest: loaders.loadDigest,
    loadTasks: loaders.loadTasks,
    loadMailbox: loaders.loadMailbox,
    loadHealth: loaders.loadHealth,
    loadIncidents: loaders.loadIncidents,
    loadTargets: loaders.loadTargets,
    loadFunnel: loaders.loadFunnel,
    loadVerticals: loaders.loadVerticals,
    loadHolding: loaders.loadHolding,
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
    loadEvents: loaders.loadEvents,
    loadMailbox: loaders.loadMailbox,
    loadTargets: loaders.loadTargets,
    loadOverview: loaders.loadOverview,
    loadFunnel: loaders.loadFunnel,
  });

  const { healthContracts, holdingViewState } = useDashboardContractsState({
    health: domain.opsState.health,
    holdingData: domain.pipelineState.holdingData,
  });

  const runtime = useDashboardRuntimeAssembly({
    agentsResp: domain.agentsResp,
    digestResp: domain.digestResp,
    ui: {
      agentSearch: ui.agentSearch,
      setAgentSearch: ui.setAgentSearch,
      setModalContent: ui.setModalContent,
    },
    runtimeState: domain.runtimeState,
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
    agentsResp: domain.agentsResp,
    pipelineState: domain.pipelineState,
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
    },
    ui: {
      selectedMailboxItem: ui.selectedMailboxItem,
      setSelectedMailboxItem: ui.setSelectedMailboxItem,
    },
    taskActions,
    controlActions,
    healthContracts,
  });

  const { tabs, tabBadges } = useDashboardTabsState({
    agentsResp: domain.agentsResp,
    incidentsData: domain.runtimeState.incidentsData,
    flowEvents: domain.pipelineState.flowEvents,
    mailbox: domain.opsState.mailbox,
    funnel: domain.pipelineState.funnel,
    holdingData: domain.pipelineState.holdingData,
  });

  return {
    header: {
      initialLoading: ui.initialLoading,
      statusText: ui.statusText,
      apiKey: ui.apiKey,
      setApiKey: ui.setApiKey,
      overview: domain.overview,
      stuckAgents: domain.agentsResp.states.stuck || 0,
      tabs,
      tabBadges,
      activeView: ui.activeView,
      setActiveView: ui.setActiveView,
    },
    views: {
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
