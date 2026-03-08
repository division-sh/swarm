import { isEventLinkedEdgeKind } from "./graphTypes.js";

function forceDirectedLayout(nodes, edges, cx, cy) {
  const count = nodes.length;
  if (count === 0) return new Map();

  const area = 800;
  const spacing = Math.sqrt((area * area) / count) * 1.6;
  const iterations = 120;
  const cooling = 0.97;

  const positions = new Map();
  nodes.forEach((node, index) => {
    const angle = (2 * Math.PI * index) / count;
    const radius = spacing * 1.2;
    positions.set(node.id, { x: cx + radius * Math.cos(angle), y: cy + radius * Math.sin(angle) });
  });

  const nodeSet = new Set(nodes.map((node) => node.id));
  let temperature = spacing * 0.6;

  for (let iteration = 0; iteration < iterations; iteration++) {
    const displacement = new Map();
    for (const node of nodes) displacement.set(node.id, { dx: 0, dy: 0 });

    for (let i = 0; i < count; i++) {
      for (let j = i + 1; j < count; j++) {
        const a = nodes[i].id;
        const b = nodes[j].id;
        const pointA = positions.get(a);
        const pointB = positions.get(b);
        let dx = pointA.x - pointB.x;
        let dy = pointA.y - pointB.y;
        const distance = Math.sqrt(dx * dx + dy * dy) || 0.01;
        const force = (spacing * spacing) / distance;
        const fx = (dx / distance) * force;
        const fy = (dy / distance) * force;
        const dispA = displacement.get(a);
        const dispB = displacement.get(b);
        dispA.dx += fx;
        dispA.dy += fy;
        dispB.dx -= fx;
        dispB.dy -= fy;
      }
    }

    for (const edge of edges) {
      if (!nodeSet.has(edge.from) || !nodeSet.has(edge.to)) continue;
      const pointA = positions.get(edge.from);
      const pointB = positions.get(edge.to);
      let dx = pointA.x - pointB.x;
      let dy = pointA.y - pointB.y;
      const distance = Math.sqrt(dx * dx + dy * dy) || 0.01;
      const force = (distance * distance) / spacing;
      const fx = (dx / distance) * force;
      const fy = (dy / distance) * force;
      const dispA = displacement.get(edge.from);
      const dispB = displacement.get(edge.to);
      dispA.dx -= fx;
      dispA.dy -= fy;
      dispB.dx += fx;
      dispB.dy += fy;
    }

    for (const node of nodes) {
      const disp = displacement.get(node.id);
      const magnitude = Math.sqrt(disp.dx * disp.dx + disp.dy * disp.dy) || 0.01;
      const clampedMagnitude = Math.min(magnitude, temperature);
      const point = positions.get(node.id);
      point.x += (disp.dx / magnitude) * clampedMagnitude;
      point.y += (disp.dy / magnitude) * clampedMagnitude;
    }

    temperature *= cooling;
  }

  return positions;
}

export function buildGraphLayout(graph, direction) {
  const nodes = (graph && graph.nodes) || [];
  const edges = (graph && graph.edges) || [];
  const dir = direction || "LR";

  const byID = new Map();
  for (const node of nodes) byID.set(node.id, node);

  if (nodes.length === 0) {
    return { nodes: [], edges: [], pos: new Map(), bounds: { minX: 0, minY: 0, maxX: 1200, maxY: 700 }, byID };
  }

  const agents = nodes.filter((node) => node.kind === "agent");
  const events = nodes.filter((node) => node.kind === "event");
  const nonEvents = nodes.filter((node) => node.kind !== "event");
  const pos = new Map();

  if (events.length === 0 || (nonEvents.length > 0 && events.length <= nonEvents.length * 0.3)) {
    const positions = forceDirectedLayout(nodes, edges, 400, 300);
    for (const [id, point] of positions) pos.set(id, point);
  } else if (events.length > 0) {
    const agentSubCount = new Map();
    const eventToAgents = new Map();
    for (const edge of edges) {
      if (!isEventLinkedEdgeKind(edge.kind)) continue;
      const fromNode = byID.get(edge.from);
      const toNode = byID.get(edge.to);
      if (fromNode && fromNode.kind === "event" && toNode && toNode.kind === "agent" && edge.kind !== "producer") {
        agentSubCount.set(edge.to, (agentSubCount.get(edge.to) || 0) + 1);
        if (!eventToAgents.has(edge.from)) eventToAgents.set(edge.from, []);
        eventToAgents.get(edge.from).push(edge.to);
      }
    }

    const agentGap = 140;
    const eventRowGap = 28;
    const sortedAgents = [...agents].sort((a, b) => (agentSubCount.get(b.id) || 0) - (agentSubCount.get(a.id) || 0));

    const agentEventGroups = new Map();
    const unassignedEvents = [];
    for (const eventNode of events) {
      const targets = eventToAgents.get(eventNode.id) || [];
      if (targets.length > 0) {
        const primary = [...targets].sort((a, b) => (agentSubCount.get(b) || 0) - (agentSubCount.get(a) || 0))[0];
        if (!agentEventGroups.has(primary)) agentEventGroups.set(primary, []);
        agentEventGroups.get(primary).push(eventNode);
      } else {
        unassignedEvents.push(eventNode);
      }
    }

    if (dir === "LR") {
      let agentY = 60;
      let eventY = 60;
      for (const agent of sortedAgents) {
        const groupEvents = (agentEventGroups.get(agent.id) || []).sort((a, b) => (a.label || "").localeCompare(b.label || ""));
        const groupStartY = eventY;
        for (const eventNode of groupEvents) {
          pos.set(eventNode.id, { x: 40, y: eventY });
          eventY += eventRowGap;
        }
        if (groupEvents.length > 0) eventY += 12;
        const groupCenterY = groupEvents.length > 0 ? groupStartY + ((groupEvents.length - 1) * eventRowGap) / 2 : agentY;
        const finalAgentY = Math.max(agentY, groupCenterY);
        pos.set(agent.id, { x: 700, y: finalAgentY });
        agentY = finalAgentY + agentGap;
      }
      for (const eventNode of unassignedEvents) {
        pos.set(eventNode.id, { x: 40, y: eventY });
        eventY += eventRowGap;
      }
    } else {
      let agentX = 60;
      let eventX = 60;
      for (const agent of sortedAgents) {
        const groupEvents = (agentEventGroups.get(agent.id) || []).sort((a, b) => (a.label || "").localeCompare(b.label || ""));
        const groupStartX = eventX;
        for (const eventNode of groupEvents) {
          pos.set(eventNode.id, { x: eventX, y: 40 });
          eventX += 200;
        }
        if (groupEvents.length > 0) eventX += 20;
        const groupCenterX = groupEvents.length > 0 ? groupStartX + ((groupEvents.length - 1) * 200) / 2 : agentX;
        const finalAgentX = Math.max(agentX, groupCenterX);
        pos.set(agent.id, { x: finalAgentX, y: 500 });
        agentX = finalAgentX + 280;
      }
      for (const eventNode of unassignedEvents) {
        pos.set(eventNode.id, { x: eventX, y: 40 });
        eventX += 200;
      }
    }

    const otherNodes = nodes.filter((node) => node.kind !== "agent" && node.kind !== "event");
    otherNodes.forEach((node, index) => {
      if (!pos.has(node.id)) pos.set(node.id, { x: 40, y: 40 + index * 80 });
    });
  }

  let minX = 1e9;
  let minY = 1e9;
  let maxX = -1e9;
  let maxY = -1e9;
  for (const node of nodes) {
    const point = pos.get(node.id) || { x: 0, y: 0 };
    minX = Math.min(minX, point.x);
    minY = Math.min(minY, point.y);
    maxX = Math.max(maxX, point.x);
    maxY = Math.max(maxY, point.y);
  }
  if (!Number.isFinite(minX)) {
    minX = 0;
    minY = 0;
    maxX = 1200;
    maxY = 700;
  }

  const usedForce = events.length === 0 || (nonEvents.length > 0 && events.length <= nonEvents.length * 0.3);

  return {
    nodes,
    edges,
    pos,
    bounds: { minX: minX - 180, minY: minY - 120, maxX: maxX + 260, maxY: maxY + 160 },
    byID,
    forceLayout: usedForce,
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
