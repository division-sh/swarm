import { useEffect } from "react";

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
    const exists = ((currentGraph && currentGraph.edges) || []).some((edge, index) => `${edge.kind}:${edge.from}->${edge.to}:${index}` === selectedGraphEdgeID);
    if (!exists) setSelectedGraphEdgeID("");
  }, [graph, graphViewGraph, selectedGraphEdgeID, setSelectedGraphEdgeID]);
}
