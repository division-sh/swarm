import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ViewportPortal,
  useEdgesState,
  useNodesState,
} from "@xyflow/react";
import { AgentNode, ControlNode, EventNode } from "./GraphNodes.jsx";
import { buildEdgePresentation, StraightClippedEdge } from "./GraphEdges.jsx";
import { buildLaneRects, getFocusedNodeIDs, laneGroupForNode, nodeMatches } from "./graphFocus.js";
import { edgeSelectionID } from "./graphInspectorUtils.jsx";
import { buildGraphLayout, deriveGraphForView } from "./graphLayout.js";
import { readSavedPositions, readSavedView, writeSavedPositions, writeSavedView } from "./graphPersistence.js";
import { GraphFlowToolbar, GraphLegendPanel } from "./GraphToolbar.jsx";
import { isEventLinkedEdgeKind } from "./graphTypes.js";

function hashToken(value) {
  let hash = 0;
  const text = String(value || "");
  for (let index = 0; index < text.length; index += 1) {
    hash = ((hash << 5) - hash) + text.charCodeAt(index);
    hash |= 0;
  }
  return Math.abs(hash).toString(36);
}

export default function GraphView({ graph, graphKey, selectedNodeID, selectedEdgeID, onSelectNode, onSelectEdge, onDerivedGraph, runtimeAgents, isFullscreen, onToggleFullscreen, activeEdgeKeys, stageFilter = "all", rubricFilter = "all", mode = "workflow" }) {
  const [collapseEvents, setCollapseEvents] = useState(true);
  const [hideOrphans, setHideOrphans] = useState(false);
  const [q, setQ] = useState("");
  const [layoutDir, setLayoutDir] = useState("LR");
  const [hoverNodeID, setHoverNodeID] = useState("");
  const [focusMode, setFocusMode] = useState("all");
  const [fadeUnrelated, setFadeUnrelated] = useState(true);

  const derived = useMemo(() => deriveGraphForView(graph, { collapseEvents, hideOrphans, stageFilter, rubricFilter }), [graph, collapseEvents, hideOrphans, stageFilter, rubricFilter]);
  const layout = useMemo(() => buildGraphLayout(derived, { direction: layoutDir, mode }), [derived, layoutDir, mode]);
  const isLargeGraph = (layout.nodes || []).length > 140 || (layout.edges || []).length > 240;

  const agentRuntime = useMemo(() => {
    const m = new Map();
    for (const a of runtimeAgents || []) m.set(a.id, a);
    return m;
  }, [runtimeAgents]);

  const subscriberCounts = useMemo(() => {
    const m = new Map();
    for (const e of derived.edges || []) {
      if (isEventLinkedEdgeKind(e.kind) && e.kind !== "producer") {
        m.set(e.from, (m.get(e.from) || 0) + 1);
      }
    }
    return m;
  }, [derived]);

  useEffect(() => {
    if (onDerivedGraph) onDerivedGraph(derived);
  }, [derived, onDerivedGraph]);

  const topologyFingerprint = useMemo(() => {
    const nodeToken = (layout.nodes || []).map((node) => `${node.id}:${node.kind || ""}:${node.group || ""}`).sort().join(",");
    const edgeToken = (layout.edges || []).map((edge) => edgeSelectionID(edge, layout.edges)).sort().join(",");
    return `${(layout.nodes || []).length}:${(layout.edges || []).length}:${hashToken(`${nodeToken}|${edgeToken}`)}`;
  }, [layout.edges, layout.nodes]);
  const storageKey = useMemo(
    () => `empire_graph_pos:${layout.layoutVersion || "layout"}:${graphKey || "graph"}:${layoutDir}${collapseEvents ? ":collapse" : ""}:${topologyFingerprint}`,
    [graphKey, collapseEvents, layout.layoutVersion, layoutDir, topologyFingerprint],
  );
  const viewStorageKey = useMemo(
    () => `empire_graph_view:${graphKey || "graph"}`,
    [graphKey],
  );

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [viewSlots, setViewSlots] = useState({});
  const nodesRef = useRef([]);
  const rfRef = useRef(null);
  const viewLoadedRef = useRef(false);

  useEffect(() => {
    nodesRef.current = nodes;
  }, [nodes]);

  useEffect(() => {
    viewLoadedRef.current = false;
    setQ("");
    setHoverNodeID("");
  }, [viewStorageKey]);

  useEffect(() => {
    if (viewLoadedRef.current) return;
    const savedView = readSavedView(viewStorageKey);
    setCollapseEvents(true);
    setHideOrphans(false);
    setLayoutDir("LR");
    setFocusMode("all");
    setFadeUnrelated(true);
    if (savedView && typeof savedView === "object") {
      if (typeof savedView.collapseEvents === "boolean") setCollapseEvents(savedView.collapseEvents);
      if (typeof savedView.hideOrphans === "boolean") setHideOrphans(savedView.hideOrphans);
      if (typeof savedView.layoutDir === "string") setLayoutDir(savedView.layoutDir);
      if (typeof savedView.focusMode === "string") setFocusMode(savedView.focusMode);
      if (typeof savedView.fadeUnrelated === "boolean") setFadeUnrelated(savedView.fadeUnrelated);
    }
    const savedSlots = readSavedView(`${viewStorageKey}:slots`);
    setViewSlots(savedSlots && typeof savedSlots === "object" ? savedSlots : {});
    viewLoadedRef.current = true;
  }, [viewStorageKey]);

  const focusedNodeIDs = useMemo(() => {
    return getFocusedNodeIDs({
      edges: layout.edges,
      nodes: layout.nodes,
      focusMode,
      selectedNodeID,
      activeEdgeKeys,
      agentRuntime,
    });
  }, [activeEdgeKeys, agentRuntime, focusMode, layout.edges, layout.nodes, selectedNodeID]);

  useEffect(() => {
    const saved = readSavedPositions(storageKey);
    setNodes((prev) => {
      const prevPos = new Map();
      for (const n of prev || []) prevPos.set(n.id, n.position);

      return (layout.nodes || []).map((n) => {
        const base = layout.pos.get(n.id) || { x: 0, y: 0 };
        const p = saved.get(n.id) || prevPos.get(n.id) || base;
        const fl = layout.forceLayout || false;
        const nodeData = n.kind === "agent"
          ? { ...n, runtime: agentRuntime.get(n.id) || null, layoutDir, forceLayout: fl, dimmed: false, highlighted: false, laneGroup: laneGroupForNode(n) }
          : { ...n, subscriberCount: subscriberCounts.get(n.id) || 0, layoutDir, forceLayout: fl, dimmed: false, highlighted: false, laneGroup: laneGroupForNode(n) };
        const nodeType = n.kind === "event" ? "event" : (n.kind === "mailbox" || n.kind === "human" || n.kind === "system" ? "control" : "agent");
        return {
          id: n.id,
          type: nodeType,
          position: { x: p.x, y: p.y },
          data: nodeData,
          draggable: true,
          selectable: true,
          hidden: false,
          selected: false,
        };
      });
    });

    setEdges((layout.edges || []).map((edge) => buildEdgePresentation(
      edge,
      edgeSelectionID(edge, layout.edges),
      null,
      "",
      "",
      layout.forceLayout,
      false,
    )));
  }, [storageKey, agentRuntime, subscriberCounts, layoutDir, layout.nodes, layout.edges, layout.pos, layout.forceLayout, setEdges, setNodes]);

  useEffect(() => {
    setNodes((nds) => (nds || []).map((n) => ({ ...n, selected: n.id === selectedNodeID })));
  }, [selectedNodeID, setNodes]);

  useEffect(() => {
    const term = (q || "").trim();
    setNodes((nds) => (nds || []).map((n) => {
      const highlighted = focusedNodeIDs.has(n.id);
      const hiddenBySearch = term ? !nodeMatches(n, term) : false;
      const hiddenByFocus = !fadeUnrelated && focusMode !== "all" && focusedNodeIDs.size > 0 && !highlighted;
      return {
        ...n,
        hidden: hiddenBySearch || hiddenByFocus,
        data: {
          ...(n.data || {}),
          dimmed: fadeUnrelated && focusMode !== "all" && focusedNodeIDs.size > 0 && !highlighted,
          highlighted,
        },
      };
    }));
  }, [q, setNodes, fadeUnrelated, focusMode, focusedNodeIDs]);

  useEffect(() => {
    const hidden = new Map();
    for (const n of nodes || []) hidden.set(n.id, !!n.hidden);
    setEdges((eds) => {
      const source = layout.edges || [];
      return source.map((edge) => buildEdgePresentation(
        {
          ...edge,
          dimmed: fadeUnrelated && focusMode !== "all" && focusedNodeIDs.size > 0 && !(focusedNodeIDs.has(edge.from) && focusedNodeIDs.has(edge.to)),
        },
        edgeSelectionID(edge, source),
        activeEdgeKeys,
        isLargeGraph ? "" : hoverNodeID,
        selectedEdgeID,
        layout.forceLayout,
        !isLargeGraph,
      )).map((edge) => ({
        ...edge,
        hidden: hidden.get(edge.source) || hidden.get(edge.target),
      }));
    });
  }, [nodes, setEdges, layout.edges, fadeUnrelated, focusMode, focusedNodeIDs, activeEdgeKeys, hoverNodeID, selectedEdgeID, layout.forceLayout, isLargeGraph]);

  useEffect(() => {
    if (!rfRef.current) return;
    const term = (q || "").trim();
    if (!term) return;
    const matched = (nodes || []).filter((node) => !node.hidden && nodeMatches(node, term)).map((node) => node.id);
    if (matched.length === 0) return;
    rfRef.current.fitView({ nodes: matched.map((id) => ({ id })), padding: 0.24, duration: 220 });
  }, [nodes, q]);

  const laneRects = useMemo(() => buildLaneRects(nodes), [nodes]);

  const persistPositions = useCallback(() => {
    writeSavedPositions(storageKey, nodesRef.current);
  }, [storageKey]);

  const onNodeDragStop = useCallback(() => {
    persistPositions();
  }, [persistPositions]);

  const persistView = useCallback((next) => {
    writeSavedView(viewStorageKey, next);
  }, [viewStorageKey]);

  useEffect(() => {
    persistView({ collapseEvents, hideOrphans, layoutDir, focusMode, fadeUnrelated });
  }, [collapseEvents, hideOrphans, layoutDir, focusMode, fadeUnrelated, persistView]);

  const jumpToIDs = useCallback((ids) => {
    if (!rfRef.current || !Array.isArray(ids) || ids.length === 0) return;
    rfRef.current.fitView({ nodes: ids.map((id) => ({ id })), padding: 0.24, duration: 220 });
  }, []);

  const onJumpToSelection = useCallback(() => {
    const ids = [selectedNodeID].filter(Boolean);
    jumpToIDs(ids);
  }, [jumpToIDs, selectedNodeID]);

  const onJumpToStuck = useCallback(() => {
    const ids = (layout.nodes || []).filter((node) => agentRuntime.get(node.id)?.state === "stuck").map((node) => node.id);
    jumpToIDs(ids);
  }, [agentRuntime, jumpToIDs, layout.nodes]);

  const onJumpToHumans = useCallback(() => {
    const ids = (layout.nodes || []).filter((node) => node.kind === "human" || node.kind === "mailbox" || node.kind === "system").map((node) => node.id);
    jumpToIDs(ids);
  }, [jumpToIDs, layout.nodes]);

  const onSaveView = useCallback((slot) => {
    const slots = readSavedView(`${viewStorageKey}:slots`);
    slots[slot] = { collapseEvents, hideOrphans, layoutDir, focusMode, fadeUnrelated };
    writeSavedView(`${viewStorageKey}:slots`, slots);
    setViewSlots({ ...slots });
  }, [collapseEvents, fadeUnrelated, focusMode, hideOrphans, layoutDir, viewStorageKey]);

  const onLoadView = useCallback((slot) => {
    const slots = readSavedView(`${viewStorageKey}:slots`);
    const view = slots?.[slot];
    if (!view) return;
    setViewSlots(slots);
    if (typeof view.collapseEvents === "boolean") setCollapseEvents(view.collapseEvents);
    if (typeof view.hideOrphans === "boolean") setHideOrphans(view.hideOrphans);
    if (typeof view.layoutDir === "string") setLayoutDir(view.layoutDir);
    if (typeof view.focusMode === "string") setFocusMode(view.focusMode);
    if (typeof view.fadeUnrelated === "boolean") setFadeUnrelated(view.fadeUnrelated);
  }, [viewStorageKey]);

  const resetLayout = useCallback(() => {
    try {
      localStorage.removeItem(storageKey);
    } catch {}
    setNodes((nds) => (nds || []).map((n) => {
      const p = layout.pos.get(n.id) || { x: 0, y: 0 };
      return { ...n, position: { x: p.x, y: p.y } };
    }));
  }, [layout.pos, setNodes, storageKey]);

  useEffect(() => {
    function handleKeydown(event) {
      const target = event.target;
      const tag = target && typeof target.tagName === "string" ? target.tagName.toLowerCase() : "";
      const isEditable = tag === "input" || tag === "textarea" || tag === "select" || target?.isContentEditable;
      if (isEditable || event.metaKey || event.ctrlKey || event.altKey) return;
      if (event.key === "f") {
        event.preventDefault();
        rfRef.current?.fitView({ padding: 0.18, duration: 220 });
      } else if (event.key === "r") {
        event.preventDefault();
        resetLayout();
      } else if (event.key === "s") {
        event.preventDefault();
        onJumpToSelection();
      } else if (event.key === "k") {
        event.preventDefault();
        onJumpToStuck();
      } else if (event.key === "h") {
        event.preventDefault();
        onJumpToHumans();
      }
    }
    window.addEventListener("keydown", handleKeydown);
    return () => window.removeEventListener("keydown", handleKeydown);
  }, [onJumpToHumans, onJumpToSelection, onJumpToStuck, resetLayout]);

  const nodeTypes = useMemo(() => ({ agent: AgentNode, event: EventNode, control: ControlNode }), []);
  const edgeTypes = useMemo(() => ({ straightClipped: StraightClippedEdge }), []);

  return (
    <div className={`graph-wrap ${isFullscreen ? "graph-fullscreen" : ""}`} aria-label="Workflow graph view">
      <div className="graph-flow">
        <ReactFlow
          onInit={(instance) => { rfRef.current = instance; }}
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeDragStop={onNodeDragStop}
          onNodeClick={(_, n) => {
            onSelectNode(n && n.id ? n.id : "");
            if (onSelectEdge) onSelectEdge("");
          }}
          onNodeMouseEnter={(_, n) => { if (!isLargeGraph) setHoverNodeID(n && n.id ? n.id : ""); }}
          onNodeMouseLeave={() => { if (!isLargeGraph) setHoverNodeID(""); }}
          onEdgeClick={(_, e) => {
            onSelectNode("");
            if (onSelectEdge) onSelectEdge(e && e.id ? e.id : "");
          }}
          onPaneClick={() => {
            onSelectNode("");
            if (onSelectEdge) onSelectEdge("");
          }}
          fitView
          fitViewOptions={{ padding: 0.18 }}
          proOptions={{ hideAttribution: true }}
        >
          {!isLargeGraph ? (
            <ViewportPortal>
              <div className="graph-band-layer">
                {laneRects.map((lane) => (
                  <div
                    key={lane.group}
                    className={`graph-band graph-band-${lane.group}`}
                    style={{
                      transform: `translate(${lane.x1}px, ${lane.y1}px)`,
                      width: `${Math.max(120, lane.x2 - lane.x1)}px`,
                      height: `${Math.max(80, lane.y2 - lane.y1)}px`,
                    }}
                  >
                    <div className="graph-band-label">{lane.group}</div>
                  </div>
                ))}
              </div>
            </ViewportPortal>
          ) : null}
          <Background gap={22} size={1} color="rgba(255, 255, 255, 0.05)" />
          {!isLargeGraph ? (
            <MiniMap
              pannable
              zoomable
              className="rf-minimap"
              nodeColor={(n) => {
                const rt = n.data && n.data.runtime;
                if (!rt) return n.data && n.data.kind === "event" ? "rgba(255,255,255,0.12)" : "rgba(255,255,255,0.22)";
                if (rt.state === "stuck") return "#f87171";
                if (rt.state === "running") return "#34d399";
                return "rgba(255,255,255,0.18)";
              }}
            />
          ) : null}
          <Controls position="bottom-right" />
          <GraphFlowToolbar
            collapseEvents={collapseEvents}
            setCollapseEvents={setCollapseEvents}
            hideOrphans={hideOrphans}
            setHideOrphans={setHideOrphans}
            q={q}
            setQ={setQ}
            onResetLayout={resetLayout}
            nodeCount={(nodes || []).filter((n) => !n.hidden).length}
            edgeCount={(edges || []).filter((e) => !e.hidden).length}
            stuckCount={(nodes || []).filter((n) => n.data && n.data.runtime && n.data.runtime.state === "stuck").length}
            layoutDir={layoutDir}
            setLayoutDir={(d) => {
              try {
                localStorage.removeItem(storageKey);
              } catch {}
              setLayoutDir(d);
            }}
            isFullscreen={isFullscreen}
            onToggleFullscreen={onToggleFullscreen}
            focusMode={focusMode}
            setFocusMode={setFocusMode}
            fadeUnrelated={fadeUnrelated}
            setFadeUnrelated={setFadeUnrelated}
            onJumpToSelection={onJumpToSelection}
            onJumpToStuck={onJumpToStuck}
            onJumpToHumans={onJumpToHumans}
            viewSlots={viewSlots}
            onSaveView={onSaveView}
            onLoadView={onLoadView}
            performanceMode={isLargeGraph}
          />
          <GraphLegendPanel />
        </ReactFlow>
      </div>
    </div>
  );
}
