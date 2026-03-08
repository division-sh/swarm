import { useCallback, useEffect } from "react";
import { fetchJSON, postJSON } from "../api/client.js";
import { fetchPipelineFlow } from "../api/flow.js";
import { fetchHolding, fetchHoldingVerticalDetail } from "../api/holding.js";

export function useDashboardPipelineData({
  activeView,
  addToast,
  setFunnel,
  setShardScans,
  setShardScanDetails,
  selectedShardScanID,
  setSelectedShardScanID,
  shardScans,
  setTraceRows,
  setHoldingData,
  setHoldingDetailModal,
  graphMode,
  graphVertical,
  setGraphVertical,
  setVerticals,
  flowVertical,
  setFlowVertical,
  selectedGraphNodeID,
  setSelectedGraphNodeID,
  selectedGraphEdgeID,
  setSelectedGraphEdgeID,
  setGraph,
  flowView,
  flowStart,
  flowEnd,
  selectedFlowNodeID,
  setSelectedFlowNodeID,
  selectedFlowEdgeID,
  setSelectedFlowEdgeID,
  setFlowGraph,
  setFlowGraphMeta,
  setFlowEvents,
  setFlowReplayIndex,
  setFlowReplayOn,
}) {
  const loadFunnel = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/funnel");
    setFunnel({ throughput: d.throughput || {}, stuck: d.stuck || [] });
  }, [setFunnel]);

  const loadShardScans = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/pipeline/shards?limit=30");
    setShardScans(d.scans || []);
  }, [setShardScans]);

  const loadShardScanDetail = useCallback(async (scanID) => {
    const id = (scanID || "").trim();
    if (!id) return;
    const d = await fetchJSON(`/dashboard/api/pipeline/shards/${encodeURIComponent(id)}`);
    setShardScanDetails((prev) => ({ ...prev, [id]: d.shards || [] }));
  }, [setShardScanDetails]);

  const shardAction = useCallback(async (scanID, shardID, action) => {
    const sid = (scanID || "").trim();
    const hid = (shardID || "").trim();
    if (!sid || !hid) return;
    await postJSON(`/api/pipeline/shards/${encodeURIComponent(hid)}/${encodeURIComponent(action)}`, {});
    addToast(`Shard ${action} queued`, "info");
    await Promise.all([loadShardScans(), loadShardScanDetail(sid)]);
  }, [addToast, loadShardScanDetail, loadShardScans]);

  const loadTrace = useCallback(async (vertical) => {
    if (!vertical) return;
    const d = await fetchJSON(`/dashboard/api/verticals/${encodeURIComponent(vertical)}/trace`);
    setTraceRows(d.trace || []);
  }, [setTraceRows]);

  const loadHolding = useCallback(async () => {
    setHoldingData(await fetchHolding());
  }, [setHoldingData]);

  const openHoldingVerticalDetail = useCallback(async (verticalID) => {
    const id = (verticalID || "").trim();
    if (!id) return;
    setHoldingDetailModal({ open: true, loading: true, id, error: "", data: null });
    try {
      const d = await fetchHoldingVerticalDetail(id);
      setHoldingDetailModal({ open: true, loading: false, id, error: "", data: d || null });
    } catch (err) {
      setHoldingDetailModal({
        open: true,
        loading: false,
        id,
        error: (err && err.message) ? err.message : "failed to load vertical detail",
        data: null,
      });
    }
  }, [setHoldingDetailModal]);

  const loadVerticals = useCallback(async () => {
    const d = await fetchJSON("/api/verticals");
    const items = d.verticals || [];
    setVerticals(items);
    if (!graphVertical && items.length > 0) {
      setGraphVertical(items[0].slug || items[0].id);
    }
    if (!flowVertical && items.length > 0) {
      setFlowVertical(items[0].slug || items[0].id);
    }
  }, [flowVertical, graphVertical, setFlowVertical, setGraphVertical, setVerticals]);

  const loadGraph = useCallback(async () => {
    const p = new URLSearchParams();
    p.set("mode", graphMode);
    if (graphMode === "opco") {
      if (!graphVertical) return;
      p.set("vertical", graphVertical);
    }
    const d = await fetchJSON(`/api/graph?${p.toString()}`);
    setGraph(d || { nodes: [], edges: [] });
    if (selectedGraphNodeID) {
      const exists = (d.nodes || []).some((n) => n.id === selectedGraphNodeID);
      if (!exists) setSelectedGraphNodeID("");
    }
    if (selectedGraphEdgeID) {
      const exists = (d.edges || []).some((e, i) => `${e.kind}:${e.from}->${e.to}:${i}` === selectedGraphEdgeID);
      if (!exists) setSelectedGraphEdgeID("");
    }
  }, [graphMode, graphVertical, selectedGraphEdgeID, selectedGraphNodeID, setGraph, setSelectedGraphEdgeID, setSelectedGraphNodeID]);

  const loadPipelineFlow = useCallback(async () => {
    const d = await fetchPipelineFlow({ flowView, flowVertical, flowStart, flowEnd });
    setFlowGraph({ nodes: d.nodes || [], edges: d.edges || [] });
    setFlowGraphMeta(d.meta || {});
    setFlowEvents(d.flow_events || []);
    setFlowReplayIndex(0);
    setFlowReplayOn(false);
    if (selectedFlowNodeID) {
      const exists = (d.nodes || []).some((n) => n.id === selectedFlowNodeID);
      if (!exists) setSelectedFlowNodeID("");
    }
    if (selectedFlowEdgeID) {
      const exists = (d.edges || []).some((e, i) => `${e.kind}:${e.from}->${e.to}:${i}` === selectedFlowEdgeID);
      if (!exists) setSelectedFlowEdgeID("");
    }
  }, [
    flowEnd,
    flowStart,
    flowVertical,
    flowView,
    selectedFlowEdgeID,
    selectedFlowNodeID,
    setFlowEvents,
    setFlowGraph,
    setFlowGraphMeta,
    setFlowReplayIndex,
    setFlowReplayOn,
    setSelectedFlowEdgeID,
    setSelectedFlowNodeID,
  ]);

  useEffect(() => {
    if (activeView !== "graph") return;
    Promise.all([loadVerticals(), loadGraph()]).catch(() => {});
  }, [activeView, loadVerticals, loadGraph]);

  useEffect(() => {
    if (activeView !== "flow") return;
    Promise.all([loadVerticals(), loadPipelineFlow()]).catch(() => {});
  }, [activeView, loadVerticals, loadPipelineFlow]);

  useEffect(() => {
    if (!selectedShardScanID) return;
    const exists = (shardScans || []).some((s) => s.scan_id === selectedShardScanID);
    if (!exists) setSelectedShardScanID("");
  }, [selectedShardScanID, setSelectedShardScanID, shardScans]);

  return {
    loadFunnel,
    loadShardScans,
    loadShardScanDetail,
    shardAction,
    loadTrace,
    loadHolding,
    openHoldingVerticalDetail,
    loadVerticals,
    loadGraph,
    loadPipelineFlow,
  };
}
