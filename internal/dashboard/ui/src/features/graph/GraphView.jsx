import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  useEdgesState,
  useNodesState,
} from "@xyflow/react";
import { AgentNode, ControlNode, EventNode } from "./GraphNodes.jsx";
import { buildEdgePresentation, StraightClippedEdge } from "./GraphEdges.jsx";
import { buildGraphLayout, deriveGraphForView } from "./graphLayout.js";
import { readSavedPositions, writeSavedPositions } from "./graphPersistence.js";
import { GraphFlowToolbar, GraphLegendPanel } from "./GraphToolbar.jsx";
import { isEventLinkedEdgeKind } from "./graphTypes.js";

export default function GraphView({ graph, graphKey, selectedNodeID, selectedEdgeID, onSelectNode, onSelectEdge, onDerivedGraph, runtimeAgents, isFullscreen, onToggleFullscreen, activeEdgeKeys, stageFilter = "all", rubricFilter = "all" }) {
  const [collapseEvents, setCollapseEvents] = useState(true);
  const [hideOrphans, setHideOrphans] = useState(false);
  const [q, setQ] = useState("");
  const [layoutDir, setLayoutDir] = useState("LR");
  const [hoverNodeID, setHoverNodeID] = useState("");

  const derived = useMemo(() => deriveGraphForView(graph, { collapseEvents, hideOrphans, stageFilter, rubricFilter }), [graph, collapseEvents, hideOrphans, stageFilter, rubricFilter]);
  const layout = useMemo(() => buildGraphLayout(derived, layoutDir), [derived, layoutDir]);

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

  const storageKey = useMemo(
    () => `empire_graph_pos:${graphKey || "graph"}:${layoutDir}${collapseEvents ? ":collapse" : ""}`,
    [graphKey, collapseEvents, layoutDir],
  );

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const nodesRef = useRef([]);

  useEffect(() => {
    nodesRef.current = nodes;
  }, [nodes]);

  function nodeMatches(n, term) {
    const t = (term || "").trim().toLowerCase();
    if (!t) return true;
    const d = n && n.data ? n.data : n;
    const hay = `${d.id || ""} ${d.label || ""} ${d.role || ""}`.toLowerCase();
    return hay.includes(t);
  }

  useEffect(() => {
    const saved = readSavedPositions(storageKey);
    setNodes((prev) => {
      const prevPos = new Map();
      for (const n of prev || []) prevPos.set(n.id, n.position);

      return (layout.nodes || []).map((n) => {
        const base = layout.pos.get(n.id) || { x: 0, y: 0 };
        const p = saved.get(n.id) || prevPos.get(n.id) || base;
        const term = (q || "").trim();
        const fl = layout.forceLayout || false;
        const nodeData = n.kind === "agent"
          ? { ...n, runtime: agentRuntime.get(n.id) || null, layoutDir, forceLayout: fl }
          : { ...n, subscriberCount: subscriberCounts.get(n.id) || 0, layoutDir, forceLayout: fl };
        const nodeType = n.kind === "event" ? "event" : (n.kind === "mailbox" || n.kind === "human" || n.kind === "system" ? "control" : "agent");
        return {
          id: n.id,
          type: nodeType,
          position: { x: p.x, y: p.y },
          data: nodeData,
          draggable: true,
          selectable: true,
          hidden: term ? !nodeMatches(n, term) : false,
          selected: selectedNodeID === n.id,
        };
      });
    });

    setEdges((layout.edges || []).map((edge, index) => buildEdgePresentation(
      edge,
      index,
      activeEdgeKeys,
      hoverNodeID,
      selectedEdgeID,
      layout.forceLayout,
    )));
  }, [derived, storageKey, agentRuntime, subscriberCounts, layoutDir, activeEdgeKeys, selectedEdgeID, hoverNodeID, layout.nodes, layout.edges, layout.pos, layout.forceLayout, q, selectedNodeID, setEdges, setNodes]);

  useEffect(() => {
    setNodes((nds) => (nds || []).map((n) => ({ ...n, selected: n.id === selectedNodeID })));
  }, [selectedNodeID, setNodes]);

  useEffect(() => {
    const term = (q || "").trim();
    setNodes((nds) => (nds || []).map((n) => ({ ...n, hidden: term ? !nodeMatches(n, term) : false })));
  }, [q, setNodes]);

  useEffect(() => {
    const hidden = new Map();
    for (const n of nodes || []) hidden.set(n.id, !!n.hidden);
    setEdges((eds) => (eds || []).map((e) => ({ ...e, hidden: hidden.get(e.source) || hidden.get(e.target) })));
  }, [nodes, setEdges]);

  const persistPositions = useCallback(() => {
    writeSavedPositions(storageKey, nodesRef.current);
  }, [storageKey]);

  const onNodeDragStop = useCallback(() => {
    persistPositions();
  }, [persistPositions]);

  const resetLayout = useCallback(() => {
    try {
      localStorage.removeItem(storageKey);
    } catch {}
    setNodes((nds) => (nds || []).map((n) => {
      const p = layout.pos.get(n.id) || { x: 0, y: 0 };
      return { ...n, position: { x: p.x, y: p.y } };
    }));
  }, [layout.pos, setNodes, storageKey]);

  const nodeTypes = useMemo(() => ({ agent: AgentNode, event: EventNode, control: ControlNode }), []);
  const edgeTypes = useMemo(() => ({ straightClipped: StraightClippedEdge }), []);

  return (
    <div className={`graph-wrap ${isFullscreen ? "graph-fullscreen" : ""}`}>
      <div className="graph-flow">
        <ReactFlow
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
          onNodeMouseEnter={(_, n) => setHoverNodeID(n && n.id ? n.id : "")}
          onNodeMouseLeave={() => setHoverNodeID("")}
          onEdgeClick={(_, e) => {
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
          <Background gap={22} size={1} color="rgba(255, 255, 255, 0.05)" />
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
          />
          <GraphLegendPanel />
        </ReactFlow>
      </div>
    </div>
  );
}
