import type { AgentsResponse } from "../types/core.ts";
import type { FunnelResponse, HoldingResponse, ShardDetailRecord, ShardScanRecord, TraceRecord, VerticalRecord } from "../types/portfolio.ts";
import type { GraphResponse, WorkflowFlowMeta, WorkflowFlowResponse } from "../types/workflow.ts";
import { useFlowDerivedState } from "../features/flow/useFlowDerivedState.ts";
import { useGraphSelection } from "../features/graph/useGraphSelection.ts";
import { useDashboardPipelineController } from "./useDashboardPipelineController.ts";

type StringSetter = (value: string) => void;
type BoolSetter = (value: boolean) => void;
type GraphSetter = (value: GraphResponse) => void;

type PipelineState = {
  flowView: string;
  setFlowView: StringSetter;
  flowStage: string;
  setFlowStage: StringSetter;
  flowRubric: string;
  setFlowRubric: StringSetter;
  flowVertical: string;
  setFlowVertical: StringSetter;
  flowStart: string;
  setFlowStart: StringSetter;
  flowEnd: string;
  setFlowEnd: StringSetter;
  flowReplaySpeed: number;
  setFlowReplaySpeed: (value: number) => void;
  flowReplayOn: boolean;
  setFlowReplayOn: BoolSetter;
  flowReplayIndex: number;
  setFlowReplayIndex: (value: number | ((prev: number) => number)) => void;
  selectedFlowNodeID: string;
  setSelectedFlowNodeID: StringSetter;
  selectedFlowEdgeID: string;
  setSelectedFlowEdgeID: StringSetter;
  flowViewGraph: GraphResponse;
  setFlowViewGraph: GraphSetter;
  graphFullscreen: boolean;
  setGraphFullscreen: BoolSetter;
  graphViewGraph: GraphResponse;
  setGraphViewGraph: GraphSetter;
  graphMode: string;
  setGraphMode: StringSetter;
  graphVertical: string;
  setGraphVertical: StringSetter;
  selectedGraphNodeID: string;
  setSelectedGraphNodeID: StringSetter;
  selectedGraphEdgeID: string;
  setSelectedGraphEdgeID: StringSetter;
  traceVertical: string;
  setTraceVertical: StringSetter;
  selectedShardScanID: string;
  setSelectedShardScanID: StringSetter;
};

type PortfolioData = {
  verticals: VerticalRecord[];
  funnel: FunnelResponse;
  shardScans: ShardScanRecord[];
  shardScanDetails: Record<string, ShardDetailRecord[]>;
  traceRows: TraceRecord[];
};

type WorkflowData = {
  graph: GraphResponse;
  flowGraph: GraphResponse;
  flowGraphMeta: WorkflowFlowMeta;
  flowEvents: WorkflowFlowResponse["flow_events"];
};

type PipelineNavigationActions = {
  openControl: (agentID: string) => void;
  inspectAgent: (agentID: string) => void;
  navigateToTask: (taskID: string) => void;
};

type PipelineControlActions = {
  restartAgent: (agentID: string) => void;
};

type HoldingViewState = {
  domain: {
    holdingData: HoldingResponse;
    holdingVisibleVerticals: VerticalRecord[];
    holdingWorkflowSummary: {
      drift: number;
      timers: number;
      revisions: number;
    };
    holdingColumns: Array<{ key: string; label: string; stages: string[]; items: VerticalRecord[] }>;
    validationGateData: { stages: string[] };
  };
  controls: {
    holdingSearch: string;
    setHoldingSearch: StringSetter;
    holdingWorkflowFilter: string;
    setHoldingWorkflowFilter: StringSetter;
    holdingSort: string;
    setHoldingSort: StringSetter;
  };
};

type PipelineAssemblyInput = {
  agentsResp: AgentsResponse;
  pipelineState: PipelineState;
  portfolioData: PortfolioData;
  workflowData: WorkflowData;
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
  navigationActions: PipelineNavigationActions;
  controlActions: PipelineControlActions;
  holdingViewState: HoldingViewState;
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
    flowActiveEdgeKeys: flowDerived.flowActiveEdgeKeys,
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
