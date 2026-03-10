import { useDashboardPortfolioQueries } from "./useDashboardPortfolioQueries.ts";
import { useDashboardWorkflowQueries } from "./useDashboardWorkflowQueries.ts";

type PipelineSourcesInput = {
  activeView: string;
  activeSubview: string;
  addToast: (message: string, type?: string) => void;
  pipelineState: {
    selectedShardScanID: string;
    setSelectedShardScanID: (value: string) => void;
    traceVertical: string;
    graphVertical: string;
    setGraphVertical: (value: string) => void;
    flowVertical: string;
    setFlowVertical: (value: string) => void;
    setHoldingDetailModal: (value: {
      open: boolean;
      loading: boolean;
      id: string;
      error: string;
      data: Record<string, unknown> | null;
    } | ((prev: {
      open: boolean;
      loading: boolean;
      id: string;
      error: string;
      data: Record<string, unknown> | null;
    }) => {
      open: boolean;
      loading: boolean;
      id: string;
      error: string;
      data: Record<string, unknown> | null;
    })) => void;
    graphMode: string;
    flowView: string;
    flowStart: string;
    flowEnd: string;
  };
};

export function useDashboardPipelineSources({
  activeView,
  activeSubview,
  addToast,
  pipelineState,
}: PipelineSourcesInput) {
  const portfolio = useDashboardPortfolioQueries({
    selectedShardScanID: pipelineState.selectedShardScanID,
    setSelectedShardScanID: pipelineState.setSelectedShardScanID,
    traceVertical: pipelineState.traceVertical,
    graphVertical: pipelineState.graphVertical,
    setGraphVertical: pipelineState.setGraphVertical,
    flowVertical: pipelineState.flowVertical,
    setFlowVertical: pipelineState.setFlowVertical,
    setHoldingDetailModal: pipelineState.setHoldingDetailModal,
    addToast,
  });

  const workflowQueries = useDashboardWorkflowQueries({
    activeView,
    activeSubview,
    graphMode: pipelineState.graphMode,
    graphVertical: pipelineState.graphVertical,
    flowView: pipelineState.flowView,
    flowVertical: pipelineState.flowVertical,
    flowStart: pipelineState.flowStart,
    flowEnd: pipelineState.flowEnd,
  });

  return {
    data: {
      ...portfolio.data,
      ...workflowQueries.data,
    },
    loaders: {
      ...portfolio.loaders,
      ...workflowQueries.loaders,
    },
    workflowStream: {
      patchRuntimeFlowEvent: workflowQueries.patchRuntimeFlowEvent,
    },
  };
}
