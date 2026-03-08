import test from "node:test";
import assert from "node:assert/strict";
import { buildLaneRects, collectNeighbors, getFocusedNodeIDs, laneGroupForNode, nodeMatches } from "./graphFocus.js";

test("collectNeighbors includes selected node and adjacent nodes", () => {
  const result = collectNeighbors([
    { from: "a", to: "b" },
    { from: "b", to: "c" },
  ], "b");

  assert.deepEqual(Array.from(result).sort(), ["a", "b", "c"]);
});

test("nodeMatches checks id, label, and role case-insensitively", () => {
  assert.equal(nodeMatches({ id: "agent-1", label: "Scout", role: "research" }, "scout"), true);
  assert.equal(nodeMatches({ id: "agent-1", label: "Scout", role: "research" }, "RESEARCH"), true);
  assert.equal(nodeMatches({ id: "agent-1", label: "Scout", role: "research" }, "missing"), false);
});

test("getFocusedNodeIDs returns active edge path", () => {
  const result = getFocusedNodeIDs({
    edges: [
      { from: "a", to: "b", label: "scan.completed" },
      { from: "b", to: "c", label: "ignored" },
    ],
    nodes: [{ id: "a" }, { id: "b" }, { id: "c" }],
    focusMode: "active",
    selectedNodeID: "",
    activeEdgeKeys: new Set(["a->b|scan.completed"]),
    agentRuntime: new Map(),
  });

  assert.deepEqual(Array.from(result).sort(), ["a", "b"]);
});

test("getFocusedNodeIDs returns disconnected nodes", () => {
  const result = getFocusedNodeIDs({
    edges: [{ from: "a", to: "b", label: "edge" }],
    nodes: [{ id: "a" }, { id: "b" }, { id: "c" }],
    focusMode: "disconnected",
    selectedNodeID: "",
    activeEdgeKeys: new Set(),
    agentRuntime: new Map(),
  });

  assert.deepEqual(Array.from(result), ["c"]);
});

test("getFocusedNodeIDs returns system layer nodes and neighbors", () => {
  const result = getFocusedNodeIDs({
    edges: [{ from: "sys", to: "agent", label: "mailbox" }],
    nodes: [{ id: "sys", kind: "system" }, { id: "agent", kind: "agent" }],
    focusMode: "system",
    selectedNodeID: "",
    activeEdgeKeys: new Set(),
    agentRuntime: new Map(),
  });

  assert.deepEqual(Array.from(result).sort(), ["agent", "sys"]);
});

test("laneGroupForNode maps system-family nodes to system lane", () => {
  assert.equal(laneGroupForNode({ kind: "system" }), "system");
  assert.equal(laneGroupForNode({ kind: "human" }), "system");
  assert.equal(laneGroupForNode({ kind: "mailbox" }), "system");
  assert.equal(laneGroupForNode({ kind: "event" }), "events");
});

test("buildLaneRects groups visible nodes into bounding bands", () => {
  const lanes = buildLaneRects([
    { position: { x: 10, y: 20 }, width: 100, height: 50, hidden: false, data: { group: "holding" } },
    { position: { x: 180, y: 40 }, width: 120, height: 60, hidden: false, data: { group: "holding" } },
    { position: { x: 20, y: 180 }, width: 90, height: 40, hidden: false, data: { group: "template" } },
  ]);

  assert.equal(lanes.length, 2);
  const holding = lanes.find((lane) => lane.group === "holding");
  assert.ok(holding);
  assert.ok(holding.x2 > holding.x1);
  assert.ok(holding.y2 > holding.y1);
});
