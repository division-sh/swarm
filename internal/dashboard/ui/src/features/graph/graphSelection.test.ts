import test from "node:test";
import assert from "node:assert/strict";
import { edgeSelectionBase, edgeSelectionID, findEdgeBySelectionID } from "./graphSelection.ts";

test("edgeSelectionID is stable across edge reordering", () => {
  const first = { from: "a", to: "b", kind: "routing", event_type: "scan.completed", transition_ids: ["t1"] };
  const second = { from: "b", to: "c", kind: "routing", event_type: "draft.ready", transition_ids: ["t2"] };

  assert.equal(edgeSelectionID(first, [first, second]), edgeSelectionID(first, [second, first]));
});

test("edgeSelectionID differentiates repeated identical edges", () => {
  const one = { from: "a", to: "b", kind: "routing", event_type: "scan.completed" };
  const two = { from: "a", to: "b", kind: "routing", event_type: "scan.completed" };

  assert.notEqual(edgeSelectionID(one, [one, two]), edgeSelectionID(two, [one, two]));
});

test("findEdgeBySelectionID returns the matching edge", () => {
  const one = { from: "a", to: "b", kind: "routing", event_type: "scan.completed" };
  const two = { from: "b", to: "c", kind: "mailbox", event_type: "handoff" };
  const selectedID = edgeSelectionID(two, [one, two]);

  assert.equal(findEdgeBySelectionID([one, two], selectedID), two);
});

test("edgeSelectionBase prefers explicit edge ids", () => {
  assert.equal(edgeSelectionBase({ id: "edge-123", from: "a", to: "b" }), "id:edge-123");
});
