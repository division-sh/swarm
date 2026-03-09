import type { AgentsResponse } from "../types/core.ts";
import { useFlowDerivedState } from "../features/flow/useFlowDerivedState.ts";
import { useGraphSelection } from "../features/graph/useGraphSelection.ts";
import { useDashboardPipelineController } from "./useDashboardPipelineController.ts";

type PipelineAssemblyInput = {
  agentsResp: AgentsResponse;
  pipelineState: Record<string, any>;
  portfolioData: Record<string, any>;
  workflowData: Record<string, any>;
  loaders: {
    loadVerticals: () => Promise<unknown>;
    loadGraph: () => Promise<unknown>;
    loadPipelineFlow: () => Promise<unknown>;
    loadTrace: (vertical?: string) => Promise<unknown>;
    loadShardScanDetail: (scanID?: string) => Promise<unknown>;
    shardAction: (scanID: string, shardID: string, action: string) => Promise<void>;
    openHoldingVerticalDetail: (verticalID: string) => Promise<void> | void;
  };
  addToast: (message: string, type?: string) => void;
  navigationActions: Record<string, any>;
  controlActions: Record<string, any>;
  holdingViewState: { domain: any; controls: any };
};

export function useDashboardPipelineAssembly({
  agentsResp,
  pipelineState,
  portfolioData,
  workflowData,
  loaders,
  addToast,
  navigationActions,
  controlActions,
  holdingViewState,
}: PipelineAssemblyInput) {
  useGraphSelection({
    graph: workflowData.graph,
    graphViewGraph: pipelineState.graphViewGraph,
    selectedGraphNodeID: pipelineState.selectedGraphNodeID,
    setSelectedGraphNodeID: pipelineState.setSelectedGraphNodeID,
    selectedGraphEdgeID: pipelineState.selectedGraphEdgeID,
    setSelectedGraphEdgeID: pipelineState.setSelectedGraphEdgeID,
  });

  const flowDerived = useFlowDerivedState({
    flowGraphMeta: workflowData.flowGraphMeta,
    flowEvents: workflowData.flowEvents,
    flowView: pipelineState.flowView,
    flowReplayIndex: pipelineState.flowReplayIndex,
    flowStage: pipelineState.flowStage,
    flowRubric: pipelineState.flowRubric,
    flowGraph: workflowData.flowGraph,
    flowViewGraph: pipelineState.flowViewGraph,
    selectedFlowNodeID: pipelineState.selectedFlowNodeID,
    setSelectedFlowNodeID: pipelineState.setSelectedFlowNodeID,
    selectedFlowEdgeID: pipelineState.selectedFlowEdgeID,
    setSelectedFlowEdgeID: pipelineState.setSelectedFlowEdgeID,
  });

  return useDashboardPipelineController({
    verticals: portfolioData.verticals,
    visibleFlowEvents: flowDerived.visibleFlowEvents,
    flowEvents: workflowData.flowEvents,
    flowGraph: workflowData.flowGraph,
    flowGraphMeta: workflowData.flowGraphMeta,
    flowActiveEdgeKeys: flowDerived.flowActiveEdgeKeys as Set<string>,
    selectedFlowSummary: flowDerived.selectedFlowSummary,
    agentsResp,
    flowView: pipelineState.flowView,
    setFlowView: pipelineState.setFlowView,
    flowStage: pipelineState.flowStage,
    setFlowStage: pipelineState.setFlowStage,
    flowStageOptions: flowDerived.flowStageOptions,
    flowRubric: pipelineState.flowRubric,
    setFlowRubric: pipelineState.setFlowRubric,
    flowRubricOptions: flowDerived.flowRubricOptions,
    flowVertical: pipelineState.flowVertical,
    setFlowVertical: pipelineState.setFlowVertical,
    flowStart: pipelineState.flowStart,
    setFlowStart: pipelineState.setFlowStart,
    flowEnd: pipelineState.flowEnd,
    setFlowEnd: pipelineState.setFlowEnd,
    flowReplaySpeed: pipelineState.flowReplaySpeed,
    setFlowReplaySpeed: pipelineState.setFlowReplaySpeed,
    flowReplayOn: pipelineState.flowReplayOn,
    setFlowReplayOn: pipelineState.setFlowReplayOn,
    flowReplayIndex: pipelineState.flowReplayIndex,
    setFlowReplayIndex: pipelineState.setFlowReplayIndex,
    loadPipelineFlow: loaders.loadPipelineFlow,
    addToast,
    selectedFlowNodeID: pipelineState.selectedFlowNodeID,
    setSelectedFlowNodeID: pipelineState.setSelectedFlowNodeID,
    selectedFlowEdgeID: pipelineState.selectedFlowEdgeID,
    setSelectedFlowEdgeID: pipelineState.setSelectedFlowEdgeID,
    flowViewGraph: pipelineState.flowViewGraph,
    setFlowViewGraph: pipelineState.setFlowViewGraph,
    graphFullscreen: pipelineState.graphFullscreen,
    setGraphFullscreen: pipelineState.setGraphFullscreen,
    graph: workflowData.graph,
    graphViewGraph: pipelineState.graphViewGraph,
    setGraphViewGraph: pipelineState.setGraphViewGraph,
    graphMode: pipelineState.graphMode,
    setGraphMode: pipelineState.setGraphMode,
    graphVertical: pipelineState.graphVertical,
    setGraphVertical: pipelineState.setGraphVertical,
    selectedGraphNodeID: pipelineState.selectedGraphNodeID,
    setSelectedGraphNodeID: pipelineState.setSelectedGraphNodeID,
    selectedGraphEdgeID: pipelineState.selectedGraphEdgeID,
    setSelectedGraphEdgeID: pipelineState.setSelectedGraphEdgeID,
    loadVerticals: loaders.loadVerticals,
    loadGraph: loaders.loadGraph,
    restartAgent: controlActions.restartAgent,
    openControl: navigationActions.openControl,
    inspectAgent: navigationActions.inspectAgent,
    navigateToTask: navigationActions.navigateToTask,
    funnel: portfolioData.funnel,
    shardScans: portfolioData.shardScans,
    shardScanDetails: portfolioData.shardScanDetails,
    traceRows: portfolioData.traceRows,
    traceVertical: pipelineState.traceVertical,
    setTraceVertical: pipelineState.setTraceVertical,
    selectedShardScanID: pipelineState.selectedShardScanID,
    setSelectedShardScanID: pipelineState.setSelectedShardScanID,
    loadTrace: loaders.loadTrace,
    loadShardScanDetail: loaders.loadShardScanDetail,
    shardAction: loaders.shardAction,
    holdingViewState,
    openHoldingVerticalDetail: loaders.openHoldingVerticalDetail,
  });
}
