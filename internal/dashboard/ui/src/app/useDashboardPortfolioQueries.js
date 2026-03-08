import { useEffect, useMemo } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  fetchFunnel,
  fetchHolding,
  fetchHoldingVerticalDetail,
  fetchShardScanDetail,
  fetchShardScans,
  fetchTrace,
  fetchVerticals,
  shardActionRequest,
} from "../api/dashboardPortfolio.js";
import { dashboardQueryKeys } from "./dashboardQueryKeys.js";

async function runRefetch(refetch) {
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
}) {
  const queryClient = useQueryClient();

  const funnelQuery = useQuery({
    queryKey: dashboardQueryKeys.funnel(),
    queryFn: fetchFunnel,
    refetchInterval: 22000,
  });
  const shardScansQuery = useQuery({
    queryKey: dashboardQueryKeys.shardScans(),
    queryFn: fetchShardScans,
    refetchInterval: 22000,
  });
  const shardScanDetailQuery = useQuery({
    queryKey: dashboardQueryKeys.shardScanDetail(selectedShardScanID),
    queryFn: () => fetchShardScanDetail(selectedShardScanID),
    enabled: !!selectedShardScanID,
  });
  const traceQuery = useQuery({
    queryKey: dashboardQueryKeys.trace(traceVertical),
    queryFn: () => fetchTrace(traceVertical),
    enabled: !!traceVertical,
  });
  const holdingQuery = useQuery({
    queryKey: dashboardQueryKeys.holding(),
    queryFn: fetchHolding,
    refetchInterval: 22000,
  });
  const verticalsQuery = useQuery({
    queryKey: dashboardQueryKeys.verticals(),
    queryFn: fetchVerticals,
    refetchInterval: 22000,
  });

  useEffect(() => {
    const scans = shardScansQuery.data || [];
    if (!selectedShardScanID) return;
    if (!scans.some((scan) => scan.scan_id === selectedShardScanID)) {
      setSelectedShardScanID("");
    }
  }, [selectedShardScanID, setSelectedShardScanID, shardScansQuery.data]);

  useEffect(() => {
    const items = verticalsQuery.data || [];
    if (!graphVertical && items.length > 0) {
      setGraphVertical(items[0].slug || items[0].id);
    }
    if (!flowVertical && items.length > 0) {
      setFlowVertical(items[0].slug || items[0].id);
    }
  }, [flowVertical, graphVertical, setFlowVertical, setGraphVertical, verticalsQuery.data]);

  const loaders = useMemo(() => ({
    loadFunnel: () => runRefetch(funnelQuery.refetch),
    loadShardScans: () => runRefetch(shardScansQuery.refetch),
    loadShardScanDetail: async (scanID) => {
      const id = String(scanID || selectedShardScanID || "").trim();
      if (!id) return [];
      setSelectedShardScanID(id);
      return queryClient.fetchQuery({
        queryKey: dashboardQueryKeys.shardScanDetail(id),
        queryFn: () => fetchShardScanDetail(id),
      });
    },
    loadTrace: async (vertical) => {
      const value = String(vertical || traceVertical || "").trim();
      if (!value) return [];
      return queryClient.fetchQuery({
        queryKey: dashboardQueryKeys.trace(value),
        queryFn: () => fetchTrace(value),
      });
    },
    loadHolding: () => runRefetch(holdingQuery.refetch),
    loadVerticals: () => runRefetch(verticalsQuery.refetch),
    shardAction: async (scanID, shardID, action) => {
      await shardActionRequest(scanID, shardID, action);
      addToast(`Shard ${action} queued`, "info");
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.shardScans() }),
        queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.shardScanDetail(scanID) }),
      ]);
    },
    openHoldingVerticalDetail: async (verticalID) => {
      const id = String(verticalID || "").trim();
      if (!id) return;
      setHoldingDetailModal({ open: true, loading: true, id, error: "", data: null });
      try {
        const data = await queryClient.fetchQuery({
          queryKey: ["dashboard", "holding-detail", id],
          queryFn: () => fetchHoldingVerticalDetail(id),
        });
        setHoldingDetailModal({ open: true, loading: false, id, error: "", data: data || null });
      } catch (err) {
        setHoldingDetailModal({
          open: true,
          loading: false,
          id,
          error: err?.message || "failed to load vertical detail",
          data: null,
        });
      }
    },
  }), [
    addToast,
    flowVertical,
    funnelQuery.refetch,
    holdingQuery.refetch,
    queryClient,
    selectedShardScanID,
    setHoldingDetailModal,
    setSelectedShardScanID,
    shardScansQuery.refetch,
    traceVertical,
    verticalsQuery.refetch,
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
      verticals: verticalsQuery.data || [],
    },
    queries: {
      funnel: funnelQuery,
      shardScans: shardScansQuery,
      shardScanDetail: shardScanDetailQuery,
      trace: traceQuery,
      holding: holdingQuery,
      verticals: verticalsQuery,
    },
    loaders,
  }), [
    funnelQuery,
    holdingQuery,
    loaders,
    selectedShardScanID,
    shardScanDetailQuery.data,
    shardScanDetailQuery,
    shardScansQuery,
    traceQuery,
    verticalsQuery,
  ]);
}
