export function laneGroupForNode(node) {
  const kind = String(node?.data?.kind || node?.kind || "");
  if (kind === "system" || kind === "human" || kind === "mailbox") return "system";
  return node?.data?.group || (kind === "event" ? "events" : "system");
}

export function collectNeighbors(edges, nodeID) {
  const keep = new Set(nodeID ? [nodeID] : []);
  if (!nodeID) return keep;
  for (const edge of edges || []) {
    if (edge.from === nodeID) keep.add(edge.to);
    if (edge.to === nodeID) keep.add(edge.from);
  }
  return keep;
}

export function nodeMatches(node, term) {
  const t = String(term || "").trim().toLowerCase();
  if (!t) return true;
  const data = node && node.data ? node.data : node;
  const hay = `${data?.id || ""} ${data?.label || ""} ${data?.role || ""}`.toLowerCase();
  return hay.includes(t);
}

export function getFocusedNodeIDs({ edges, nodes, focusMode, selectedNodeID, activeEdgeKeys, agentRuntime }) {
  const baseEdges = edges || [];
  const graphNodes = nodes || [];
  const nodeIDs = new Set(graphNodes.map((node) => node.id));

  if (focusMode === "selected") return collectNeighbors(baseEdges, selectedNodeID);

  if (focusMode === "active") {
    const keep = new Set();
    for (const edge of baseEdges) {
      const edgeKey = `${edge.from}->${edge.to}|${edge.label || ""}`;
      if (activeEdgeKeys && activeEdgeKeys.has(edgeKey)) {
        keep.add(edge.from);
        keep.add(edge.to);
      }
    }
    return keep;
  }

  if (focusMode === "stuck") {
    const keep = new Set();
    for (const node of graphNodes) {
      const runtime = agentRuntime.get(node.id);
      if (runtime?.state === "stuck") {
        keep.add(node.id);
        for (const edge of baseEdges) {
          if (edge.from === node.id) keep.add(edge.to);
          if (edge.to === node.id) keep.add(edge.from);
        }
      }
    }
    return keep;
  }

  if (focusMode === "humans") {
    const keep = new Set();
    for (const node of graphNodes) {
      if (node.kind === "human" || node.kind === "mailbox") {
        keep.add(node.id);
        for (const edge of baseEdges) {
          if (edge.from === node.id) keep.add(edge.to);
          if (edge.to === node.id) keep.add(edge.from);
        }
      }
    }
    return keep;
  }

  if (focusMode === "system") {
    const keep = new Set();
    for (const node of graphNodes) {
      if (node.kind === "human" || node.kind === "mailbox" || node.kind === "system") {
        keep.add(node.id);
        for (const edge of baseEdges) {
          if (edge.from === node.id) keep.add(edge.to);
          if (edge.to === node.id) keep.add(edge.from);
        }
      }
    }
    return keep;
  }

  if (focusMode === "disconnected") {
    const connected = new Set();
    for (const edge of baseEdges) {
      connected.add(edge.from);
      connected.add(edge.to);
    }
    return new Set(Array.from(nodeIDs).filter((id) => !connected.has(id)));
  }

  return nodeIDs;
}

export function buildLaneRects(nodes) {
  const groups = new Map();
  for (const node of nodes || []) {
    if (!node || node.hidden) continue;
    const group = laneGroupForNode(node);
    const width = node.measured?.width || node.width || 220;
    const height = node.measured?.height || node.height || 80;
    const x1 = node.position.x - 28;
    const y1 = node.position.y - 22;
    const x2 = node.position.x + width + 28;
    const y2 = node.position.y + height + 22;
    const current = groups.get(group);
    if (!current) {
      groups.set(group, { group, x1, y1, x2, y2 });
      continue;
    }
    current.x1 = Math.min(current.x1, x1);
    current.y1 = Math.min(current.y1, y1);
    current.x2 = Math.max(current.x2, x2);
    current.y2 = Math.max(current.y2, y2);
  }
  return Array.from(groups.values());
}
