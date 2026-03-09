import { useMemo } from "react";
import type { AgentsResponse } from "../types/core.ts";
import type { FunnelResponse } from "../types/portfolio.ts";
import type { GraphResponse, WorkflowFlowResponse } from "../types/workflow.ts";
import { useFlowController } from "../features/flow/useFlowController.ts";
import { useGraphController } from "../features/graph/useGraphController.ts";
import { useHoldingController } from "../features/holding/useHoldingController.ts";
import { usePipelineController } from "../features/pipeline/usePipelineController.ts";

type AsyncAction = () => Promise<unknown>;
type StringSetter = (value: string) => void;
type BoolSetter = (value: boolean) => void;

type DashboardPipelineControllerInput = {
  verticals: Record<string, any>[];
  visibleFlowEvents: Record<string, any>[];
  flowEvents: WorkflowFlowResponse["flow_events"];
  flowGraph: GraphResponse;
  flowGraphMeta: Record<string, any>;
  flowActiveEdgeKeys: Set<string>;
  selectedFlowSummary: Record<string, any> | null;
  agentsResp: AgentsResponse;
  flowView: string;
  setFlowView: StringSetter;
  flowStage: string;
  setFlowStage: StringSetter;
  flowStageOptions: string[];
  flowRubric: string;
  setFlowRubric: StringSetter;
  flowRubricOptions: string[];
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
  loadPipelineFlow: AsyncAction;
  addToast: (message: string, type?: string) => void;
  selectedFlowNodeID: string;
  setSelectedFlowNodeID: StringSetter;
  selectedFlowEdgeID: string;
  setSelectedFlowEdgeID: StringSetter;
  flowViewGraph: GraphResponse;
  setFlowViewGraph: (value: GraphResponse) => void;
  graphFullscreen: boolean;
  setGraphFullscreen: BoolSetter;
  graph: GraphResponse;
  graphViewGraph: GraphResponse;
  setGraphViewGraph: (value: GraphResponse) => void;
  graphMode: string;
  setGraphMode: StringSetter;
  graphVertical: string;
  setGraphVertical: StringSetter;
  selectedGraphNodeID: string;
  setSelectedGraphNodeID: StringSetter;
  selectedGraphEdgeID: string;
  setSelectedGraphEdgeID: StringSetter;
  loadVerticals: AsyncAction;
  loadGraph: AsyncAction;
  restartAgent: (agentID: string) => void;
  openControl: (agentID: string) => void;
  inspectAgent: (agentID: string) => void;
  navigateToTask: (taskID: string) => void;
  funnel: FunnelResponse;
  shardScans: Record<string, any>[];
  shardScanDetails: Record<string, any>;
  traceRows: Record<string, any>[];
  traceVertical: string;
  setTraceVertical: StringSetter;
  selectedShardScanID: string;
  setSelectedShardScanID: StringSetter;
  loadTrace: (vertical?: string) => Promise<unknown>;
  loadShardScanDetail: (scanID?: string) => Promise<unknown>;
  shardAction: (scanID: string, shardID: string, action: string) => Promise<void>;
  holdingViewState: { domain: any; controls: any };
  openHoldingVerticalDetail: (verticalID: string) => Promise<void> | void;
};

export function useDashboardPipelineController({
  verticals,
  visibleFlowEvents,
  flowEvents,
  flowGraph,
  flowGraphMeta,
  flowActiveEdgeKeys,
  selectedFlowSummary,
  agentsResp,
  flowView,
  setFlowView,
  flowStage,
  setFlowStage,
  flowStageOptions,
  flowRubric,
  setFlowRubric,
  flowRubricOptions,
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
  setGraphViewGraph,
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
  restartAgent,
  openControl,
  inspectAgent,
  navigateToTask,
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
}: DashboardPipelineControllerInput) {
  const flow = useFlowController({
    verticals,
    visibleFlowEvents,
    flowEvents,
    flowGraph,
    flowGraphMeta,
    flowActiveEdgeKeys,
    selectedFlowSummary,
    agents: agentsResp.agents,
    flowView,
    flowStage,
    flowStageOptions,
    flowRubric,
    flowRubricOptions,
    flowVertical,
    flowStart,
    flowEnd,
    flowReplaySpeed,
    flowReplayOn,
    flowReplayIndex,
    selectedFlowNodeID,
    selectedFlowEdgeID,
    flowViewGraph,
    graphFullscreen,
    setFlowView,
    setFlowStage,
    setFlowRubric,
    setFlowVertical,
    setFlowStart,
    setFlowEnd,
    setFlowReplaySpeed,
    setFlowReplayOn,
    setFlowReplayIndex,
    refresh: () => loadPipelineFlow().catch((err: Error) => addToast(err.message, "error")),
    addToast,
    setSelectedFlowNodeID,
    setSelectedFlowEdgeID,
    setFlowViewGraph,
    setGraphFullscreen,
  });

  const graphController = useGraphController({
    verticals,
    graph,
    graphViewGraph,
    agents: agentsResp.agents,
    graphMode,
    graphVertical,
    selectedGraphNodeID,
    selectedGraphEdgeID,
    graphFullscreen,
    setGraphMode,
    setGraphVertical,
    setSelectedGraphNodeID,
    setSelectedGraphEdgeID,
    setGraphFullscreen,
    refreshGraph: () => Promise.all([loadVerticals(), loadGraph()]),
    setGraphViewGraph,
    restartAgent,
    openControl,
    inspectAgent,
    navigateToTask,
  });

  const pipeline = usePipelineController({
    funnel,
    shardScans,
    shardScanDetails,
    traceRows,
    traceVertical,
    selectedShardScanID,
    setTraceVertical,
    setSelectedShardScanID,
    traceVerticalFlow: loadTrace,
    loadShardScanDetail,
    shardAction,
  });

  const holding = useHoldingController({
    domain: holdingViewState.domain,
    controls: holdingViewState.controls,
    openHoldingVerticalDetail,
  });

  return useMemo(() => ({
    flow,
    graph: graphController,
    pipeline,
    holding,
  }), [flow, graphController, pipeline, holding]);
}
