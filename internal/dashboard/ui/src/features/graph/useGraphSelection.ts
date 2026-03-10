import { useEffect } from "react";
import { edgeSelectionID } from "./graphInspectorUtils.tsx";

export function useGraphSelection({
  graph,
  graphViewGraph,
  selectedGraphNodeID,
  setSelectedGraphNodeID,
  selectedGraphEdgeID,
  setSelectedGraphEdgeID,
}) {
  useEffect(() => {
    if (!selectedGraphNodeID) return;
    const currentGraph = graphViewGraph || graph;
    const exists = ((currentGraph && currentGraph.nodes) || []).some((node) => node.id === selectedGraphNodeID);
    if (!exists) setSelectedGraphNodeID("");
  }, [graph, graphViewGraph, selectedGraphNodeID, setSelectedGraphNodeID]);

  useEffect(() => {
    if (!selectedGraphEdgeID) return;
    const currentGraph = graphViewGraph || graph;
    const currentEdges = ((currentGraph && currentGraph.edges) || []);
    const exists = currentEdges.some((edge) => edgeSelectionID(edge, currentEdges) === selectedGraphEdgeID);
    if (!exists) setSelectedGraphEdgeID("");
  }, [graph, graphViewGraph, selectedGraphEdgeID, setSelectedGraphEdgeID]);
}
