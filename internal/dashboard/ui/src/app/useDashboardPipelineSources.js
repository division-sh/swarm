import { useDashboardPortfolioQueries } from "./useDashboardPortfolioQueries.js";
import { useDashboardWorkflowQueries } from "./useDashboardWorkflowQueries.js";

export function useDashboardPipelineSources({
  activeView,
  activeSubview,
  addToast,
  pipelineState,
}) {
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
