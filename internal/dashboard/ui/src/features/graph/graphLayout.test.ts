import test from "node:test";
import assert from "node:assert/strict";
import { buildGraphLayout, deriveGraphForView, getGraphLayoutVersion } from "./graphLayout.ts";

test("buildGraphLayout assigns positions for all nodes", () => {
  const graph = {
    nodes: [
      { id: "a", kind: "agent", group: "holding", role: "scout" },
      { id: "b", kind: "agent", group: "holding", role: "writer" },
      { id: "evt:scan", kind: "event", label: "scan.completed" },
    ],
    edges: [
      { from: "a", to: "evt:scan", kind: "producer", label: "scan.completed" },
      { from: "evt:scan", to: "b", kind: "routing", label: "scan.completed" },
    ],
  };

  const layout = buildGraphLayout(graph, { direction: "LR", mode: "holding" });

  assert.equal(layout.layoutVersion, getGraphLayoutVersion());
  assert.equal(layout.pos.size, 3);
  assert.ok(layout.pos.get("a"));
  assert.ok(layout.bounds.maxX > layout.bounds.minX);
});

test("deriveGraphForView collapses event nodes into direct routing edges", () => {
  const graph = {
    nodes: [
      { id: "a", kind: "agent" },
      { id: "b", kind: "agent" },
      { id: "evt:scan", kind: "event", label: "scan.completed" },
    ],
    edges: [
      { from: "a", to: "evt:scan", kind: "producer", event_type: "scan.completed" },
      { from: "evt:scan", to: "b", kind: "routing", event_type: "scan.completed" },
    ],
  };

  const derived = deriveGraphForView(graph, { collapseEvents: true, hideOrphans: false, stageFilter: "all", rubricFilter: "all" });

  assert.equal(derived.nodes.some((node) => node.kind === "event"), false);
  assert.equal(derived.edges.length, 1);
  assert.equal(derived.edges[0].from, "a");
  assert.equal(derived.edges[0].to, "b");
});

