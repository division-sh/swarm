import { useMemo } from "react";
import type { FunnelResponse } from "../../types/portfolio.ts";

type StringSetter = (value: string) => void;
type PipelineControllerInput = {
  funnel: FunnelResponse;
  shardScans: Record<string, any>[];
  shardScanDetails: Record<string, any>;
  traceRows: Record<string, any>[];
  traceVertical: string;
  selectedShardScanID: string;
  setTraceVertical: StringSetter;
  setSelectedShardScanID: StringSetter;
  traceVerticalFlow: (vertical?: string) => Promise<unknown>;
  loadShardScanDetail: (scanID?: string) => Promise<unknown>;
  shardAction: (scanID: string, shardID: string, action: string) => Promise<void>;
};

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
}: PipelineControllerInput) {
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
