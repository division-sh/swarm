import { useEffect, useMemo } from "react";
import { useQuery, useQueryClient, type QueryObserverResult } from "@tanstack/react-query";
import {
  fetchFunnel,
  fetchHolding,
  fetchHoldingVerticalDetail,
  fetchShardScanDetail,
  fetchShardScans,
  fetchTrace,
  shardActionRequest,
} from "../api/dashboardPortfolio.ts";
import { dashboardQueryKeys } from "./dashboardQueryKeys.ts";
import type { FunnelResponse, HoldingResponse, HoldingVerticalDetail, ShardDetailRecord, ShardScanRecord, TraceRecord, VerticalRecord } from "../types/portfolio.ts";

type HoldingDetailModalState = {
  open: boolean;
  loading: boolean;
  id: string;
  error: string;
  data: HoldingVerticalDetail | null;
};

async function runRefetch<T>(refetch: () => Promise<QueryObserverResult<T, Error>>): Promise<T | undefined> {
  const result = await refetch();
  if (result.error) throw result.error;
  return result.data;
}

export function useDashboardPortfolioQueries({
  selectedShardScanID,
  setSelectedShardScanID,
  traceVertical,
  graphVertical,
  setGraphVertical,
  flowVertical,
  setFlowVertical,
  setHoldingDetailModal,
  addToast,
}: {
  selectedShardScanID: string;
  setSelectedShardScanID: (value: string) => void;
  traceVertical: string;
  graphVertical: string;
  setGraphVertical: (value: string) => void;
  flowVertical: string;
  setFlowVertical: (value: string) => void;
  setHoldingDetailModal: (value: HoldingDetailModalState) => void;
  addToast: (message: string, type?: string) => void;
}) {
  const queryClient = useQueryClient();

  const funnelQuery = useQuery<FunnelResponse>({
    queryKey: dashboardQueryKeys.funnel(),
    queryFn: fetchFunnel,
    refetchInterval: 22000,
  });
  const shardScansQuery = useQuery<ShardScanRecord[]>({
    queryKey: dashboardQueryKeys.shardScans(),
    queryFn: fetchShardScans,
    refetchInterval: 22000,
  });
  const shardScanDetailQuery = useQuery<ShardDetailRecord[]>({
    queryKey: dashboardQueryKeys.shardScanDetail(selectedShardScanID),
    queryFn: () => fetchShardScanDetail(selectedShardScanID),
    enabled: !!selectedShardScanID,
  });
  const traceQuery = useQuery<TraceRecord[]>({
    queryKey: dashboardQueryKeys.trace(traceVertical),
    queryFn: () => fetchTrace(traceVertical),
    enabled: !!traceVertical,
  });
  const holdingQuery = useQuery<HoldingResponse>({
    queryKey: dashboardQueryKeys.holding(),
    queryFn: fetchHolding,
    refetchInterval: 22000,
  });
  const holdingVerticals = holdingQuery.data?.verticals || [];

  useEffect(() => {
    const scans = shardScansQuery.data || [];
    if (!selectedShardScanID) return;
    if (!scans.some((scan) => scan.scan_id === selectedShardScanID)) {
      setSelectedShardScanID("");
    }
  }, [selectedShardScanID, setSelectedShardScanID, shardScansQuery.data]);

  useEffect(() => {
    const items = holdingVerticals;
    if (!graphVertical && items.length > 0) {
      setGraphVertical(items[0].slug || items[0].id);
    }
    if (!flowVertical && items.length > 0) {
      setFlowVertical(items[0].slug || items[0].id);
    }
  }, [flowVertical, graphVertical, holdingVerticals, setFlowVertical, setGraphVertical]);

  const loaders = useMemo(() => ({
    loadFunnel: () => runRefetch(funnelQuery.refetch),
    loadShardScans: () => runRefetch(shardScansQuery.refetch),
    loadShardScanDetail: async (scanID?: string) => {
      const id = String(scanID || selectedShardScanID || "").trim();
      if (!id) return [];
      setSelectedShardScanID(id);
      return queryClient.fetchQuery({
        queryKey: dashboardQueryKeys.shardScanDetail(id),
        queryFn: () => fetchShardScanDetail(id),
      });
    },
    loadTrace: async (vertical?: string) => {
      const value = String(vertical || traceVertical || "").trim();
      if (!value) return [];
      return queryClient.fetchQuery({
        queryKey: dashboardQueryKeys.trace(value),
        queryFn: () => fetchTrace(value),
      });
    },
    loadHolding: () => runRefetch(holdingQuery.refetch),
    loadVerticals: () => runRefetch(holdingQuery.refetch).then((value) => value?.verticals || []),
    shardAction: async (scanID: string, shardID: string, action: string) => {
      await shardActionRequest(scanID, shardID, action);
      addToast(`Shard ${action} queued`, "info");
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.shardScans() }),
        queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.shardScanDetail(scanID) }),
      ]);
    },
    openHoldingVerticalDetail: async (verticalID: string) => {
      const id = String(verticalID || "").trim();
      if (!id) return;
      setHoldingDetailModal({ open: true, loading: true, id, error: "", data: null });
      try {
        const data = await queryClient.fetchQuery({
          queryKey: ["dashboard", "holding-detail", id],
          queryFn: () => fetchHoldingVerticalDetail(id),
        });
        setHoldingDetailModal({ open: true, loading: false, id, error: "", data: data || null });
      } catch (err: unknown) {
        setHoldingDetailModal({
          open: true,
          loading: false,
          id,
          error: err instanceof Error ? err.message : "failed to load vertical detail",
          data: null,
        });
      }
    },
  }), [
    addToast,
    funnelQuery.refetch,
    holdingQuery.refetch,
    queryClient,
    selectedShardScanID,
    setHoldingDetailModal,
    setSelectedShardScanID,
    shardScansQuery.refetch,
    traceVertical,
    holdingQuery.refetch,
  ]);

  return useMemo(() => ({
    data: {
      funnel: funnelQuery.data || { throughput: {}, stuck: [] },
      shardScans: shardScansQuery.data || [],
      shardScanDetails: selectedShardScanID
        ? { [selectedShardScanID]: shardScanDetailQuery.data || [] }
        : {},
      traceRows: traceQuery.data || [],
      holdingData: holdingQuery.data || { campaigns: [], verticals: [], agent_counts: {}, summary: {}, workflow_summary: {} },
      verticals: holdingVerticals,
    },
    queries: {
      funnel: funnelQuery,
      shardScans: shardScansQuery,
      shardScanDetail: shardScanDetailQuery,
      trace: traceQuery,
      holding: holdingQuery,
      verticals: holdingQuery,
    },
    loaders,
  }), [
    funnelQuery,
    holdingQuery,
    loaders,
    selectedShardScanID,
    shardScanDetailQuery,
    shardScansQuery,
    traceQuery,
    holdingVerticals,
  ]);
}
