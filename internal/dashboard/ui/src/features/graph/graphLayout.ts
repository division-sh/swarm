import dagre from "dagre";

const LAYOUT_VERSION = "dagre-v1";

function layoutProfile(mode, direction, nodes) {
  const isLR = direction !== "TB";
  const eventCount = nodes.filter((node) => node.kind === "event").length;
  switch (mode) {
  case "holding":
    return {
      rankdir: isLR ? "LR" : "TB",
      ranksep: isLR ? 260 : 210,
      nodesep: isLR ? 110 : 120,
      edgesep: 36,
      marginx: 64,
      marginy: 56,
    };
  case "template":
    return {
      rankdir: isLR ? "LR" : "TB",
      ranksep: isLR ? 220 : 190,
      nodesep: isLR ? 96 : 108,
      edgesep: 34,
      marginx: 60,
      marginy: 52,
    };
  case "opco":
    return {
      rankdir: isLR ? "LR" : "TB",
      ranksep: isLR ? 280 : 220,
      nodesep: isLR ? 124 : 132,
      edgesep: 40,
      marginx: 70,
      marginy: 64,
    };
  default:
    return {
      rankdir: isLR ? "LR" : "TB",
      ranksep: isLR ? 180 + Math.min(80, eventCount * 4) : 160 + Math.min(60, eventCount * 3),
      nodesep: isLR ? 72 : 92,
      edgesep: 32,
      marginx: 56,
      marginy: 48,
    };
  }
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function estimateNodeBox(node) {
  const label = String(node?.label || node?.id || "");
  const role = String(node?.role || "");
  const longestLine = Math.max(label.length, role.length, 8);

  if (node?.kind === "agent") {
    return {
      width: clamp(188 + longestLine * 4, 188, 280),
      height: 108,
    };
  }

  if (node?.kind === "event") {
    return {
      width: clamp(132 + longestLine * 3, 132, 220),
      height: 58,
    };
  }

  return {
    width: clamp(148 + longestLine * 3, 148, 230),
    height: 72,
  };
}

function buildDagreLayout(nodes, edges, direction, mode) {
  const graph = new dagre.graphlib.Graph({ multigraph: true });
  const profile = layoutProfile(mode, direction, nodes);
  graph.setGraph(profile);
  graph.setDefaultEdgeLabel(() => ({}));

  for (const node of nodes) {
    const box = estimateNodeBox(node);
    graph.setNode(node.id, {
      width: box.width,
      height: box.height,
      kind: node.kind,
      rank: node.kind === "event" ? "min" : undefined,
    });
  }

  edges.forEach((edge, index) => {
    let weight = 1;
    if (edge.kind === "management") weight = 5;
    else if (edge.kind === "mailbox" || edge.kind === "message") weight = 3;
    else if (edge.kind === "routing" && edge.source === "bootstrap") weight = 4;
    else if (edge.kind === "routing" || edge.kind === "subscription") weight = 2;
    graph.setEdge(edge.from, edge.to, { weight }, `${edge.kind || "edge"}:${index}`);
  });

  dagre.layout(graph);

  const pos = new Map();
  let minX = Number.POSITIVE_INFINITY;
  let minY = Number.POSITIVE_INFINITY;
  let maxX = Number.NEGATIVE_INFINITY;
  let maxY = Number.NEGATIVE_INFINITY;

  for (const node of nodes) {
    const nodeWithPos = graph.node(node.id);
    if (!nodeWithPos) continue;
    const point = {
      x: Math.round(nodeWithPos.x - nodeWithPos.width / 2),
      y: Math.round(nodeWithPos.y - nodeWithPos.height / 2),
    };
    pos.set(node.id, point);
    minX = Math.min(minX, point.x);
    minY = Math.min(minY, point.y);
    maxX = Math.max(maxX, point.x + nodeWithPos.width);
    maxY = Math.max(maxY, point.y + nodeWithPos.height);
  }

  if (!Number.isFinite(minX)) {
    minX = 0;
    minY = 0;
    maxX = 1200;
    maxY = 700;
  }

  return {
    pos,
    bounds: { minX: minX - 120, minY: minY - 80, maxX: maxX + 180, maxY: maxY + 120 },
  };
}

export function buildGraphLayout(graph, options) {
  const nodes = (graph && graph.nodes) || [];
  const edges = (graph && graph.edges) || [];
  const dir = (typeof options === "string" ? options : options?.direction) || "LR";
  const mode = (typeof options === "object" && options?.mode) ? options.mode : "workflow";

  const byID = new Map();
  for (const node of nodes) byID.set(node.id, node);

  if (nodes.length === 0) {
    return {
      nodes: [],
      edges: [],
      pos: new Map(),
      bounds: { minX: 0, minY: 0, maxX: 1200, maxY: 700 },
      byID,
      forceLayout: false,
      layoutVersion: LAYOUT_VERSION,
    };
  }

  const dagreLayout = buildDagreLayout(nodes, edges, dir, mode);

  return {
    nodes,
    edges,
    pos: dagreLayout.pos,
    bounds: dagreLayout.bounds,
    byID,
    forceLayout: false,
    layoutVersion: LAYOUT_VERSION,
    profileMode: mode,
  };
}

export function deriveGraphForView(graph, opts) {
  let nextGraph = graph || { nodes: [], edges: [] };
  const collapseEvents = !!(opts && opts.collapseEvents);
  const hideOrphans = !!(opts && opts.hideOrphans);
  const stageFilter = opts && opts.stageFilter ? String(opts.stageFilter) : "all";
  const rubricFilter = opts && opts.rubricFilter ? String(opts.rubricFilter) : "all";

  if (stageFilter !== "all" || rubricFilter !== "all") {
    const edgePass = (edge) => {
      const stages = Array.isArray(edge && edge.stages) ? edge.stages : [];
      const rubrics = Array.isArray(edge && edge.rubrics) ? edge.rubrics : [];
      if (stageFilter !== "all" && stages.length > 0 && !stages.includes(stageFilter)) return false;
      if (rubricFilter !== "all" && rubrics.length > 0 && !rubrics.includes(rubricFilter)) return false;
      return true;
    };
    const nextEdges = (nextGraph.edges || []).filter(edgePass);
    const keepNodes = new Set();
    for (const edge of nextEdges) {
      keepNodes.add(edge.from);
      keepNodes.add(edge.to);
    }
    const nextNodes = (nextGraph.nodes || []).filter((node) => keepNodes.has(node.id) || node.kind === "human" || node.kind === "mailbox" || node.kind === "system");
    nextGraph = { ...nextGraph, nodes: nextNodes, edges: nextEdges };
  }

  if (hideOrphans && !collapseEvents) {
    const hasProducer = new Set();
    const hasSubscriber = new Set();
    const nodeKind = new Map();
    for (const node of nextGraph.nodes || []) nodeKind.set(node.id, node.kind);
    for (const edge of nextGraph.edges || []) {
      if (edge.kind === "producer" && nodeKind.get(edge.to) === "event") hasProducer.add(edge.to);
      if ((edge.kind === "subscription" || edge.kind === "routing") && nodeKind.get(edge.from) === "event") hasSubscriber.add(edge.from);
    }
    const filteredNodes = (nextGraph.nodes || []).filter((node) => node.kind !== "event" || (hasProducer.has(node.id) && hasSubscriber.has(node.id)));
    if (filteredNodes.length !== (nextGraph.nodes || []).length) nextGraph = { ...nextGraph, nodes: filteredNodes };
  }

  if (!collapseEvents) return nextGraph;

  const byID = new Map();
  for (const node of nextGraph.nodes || []) byID.set(node.id, node);

  const eventProducers = new Map();
  const eventSubscribers = new Map();
  const passthrough = [];

  for (const edge of nextGraph.edges || []) {
    if (edge.kind === "producer") {
      const toNode = byID.get(edge.to);
      if (toNode && toNode.kind === "event") {
        if (!eventProducers.has(edge.to)) eventProducers.set(edge.to, []);
        eventProducers.get(edge.to).push({ agent: edge.from, edge });
      } else {
        passthrough.push(edge);
      }
    } else if (edge.kind === "routing" || edge.kind === "subscription") {
      const fromNode = byID.get(edge.from);
      if (fromNode && fromNode.kind === "event") {
        if (!eventSubscribers.has(edge.from)) eventSubscribers.set(edge.from, []);
        eventSubscribers.get(edge.from).push({ agent: edge.to, edge });
      } else {
        passthrough.push(edge);
      }
    } else {
      passthrough.push(edge);
    }
  }

  const directEdges = [...passthrough];
  const seen = new Set();
  for (const [eventID, producers] of eventProducers) {
    const subscribers = eventSubscribers.get(eventID) || [];
    const eventNode = byID.get(eventID);
    const eventLabel = (eventNode && eventNode.label) || eventID.replace(/^evt:/, "");
    for (const producer of producers) {
      for (const subscriber of subscribers) {
        if (producer.agent === subscriber.agent) continue;
        const key = `${producer.agent}->${subscriber.agent}:${eventLabel}`;
        if (seen.has(key)) continue;
        seen.add(key);
        directEdges.push({
          from: producer.agent,
          to: subscriber.agent,
          kind: "routing",
          label: eventLabel,
          event_type: (producer.edge && producer.edge.event_type) || (subscriber.edge && subscriber.edge.event_type) || eventLabel,
          schema_required: (producer.edge && producer.edge.schema_required) || (subscriber.edge && subscriber.edge.schema_required) || [],
          schema_properties: (producer.edge && producer.edge.schema_properties) || (subscriber.edge && subscriber.edge.schema_properties) || [],
          interceptor_handler: (producer.edge && producer.edge.interceptor_handler) || (subscriber.edge && subscriber.edge.interceptor_handler) || "",
          intercepted: !!((producer.edge && producer.edge.intercepted) || (subscriber.edge && subscriber.edge.intercepted)),
          passthrough: !!((producer.edge && producer.edge.passthrough) || (subscriber.edge && subscriber.edge.passthrough)),
          producers: [producer.agent],
          consumers: [subscriber.agent],
          stages: (producer.edge && producer.edge.stages) || (subscriber.edge && subscriber.edge.stages) || [],
          rubrics: (producer.edge && producer.edge.rubrics) || (subscriber.edge && subscriber.edge.rubrics) || [],
          status: "active",
          source: producer.edge.source || subscriber.edge.source || "",
        });
      }
    }
  }

  const nextNodes = (nextGraph.nodes || []).filter((node) => node.kind !== "event");
  return { ...nextGraph, nodes: nextNodes, edges: directEdges };
}

export function getGraphLayoutVersion() {
  return LAYOUT_VERSION;
}
