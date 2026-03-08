import { useMemo } from "react";

export function usePipelineController({
  funnel,
  shardScans,
  shardScanDetails,
  traceRows,
  traceVertical,
  selectedShardScanID,
  setTraceVertical,
  setSelectedShardScanID,
  traceVerticalFlow,
  loadShardScanDetail,
  shardAction,
}) {
  return useMemo(() => ({
    state: {
      funnel,
      shardScans,
      shardScanDetails,
      traceRows,
      traceVertical,
      selectedShardScanID,
    },
    actions: {
      setTraceVertical,
      setSelectedShardScanID,
      traceVerticalFlow,
      loadShardScanDetail,
      shardAction,
    },
  }), [
    funnel,
    loadShardScanDetail,
    selectedShardScanID,
    setSelectedShardScanID,
    setTraceVertical,
    shardAction,
    shardScanDetails,
    shardScans,
    traceRows,
    traceVertical,
    traceVerticalFlow,
  ]);
}
