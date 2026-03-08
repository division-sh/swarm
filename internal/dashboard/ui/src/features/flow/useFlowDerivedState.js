import { useEffect, useMemo } from "react";
import {
  getFlowActiveEdgeKeys,
  getFlowEventStageMap,
  getFlowRubricOptions,
  getFlowStageOptions,
  getVisibleFlowEvents,
  summarizeFlowEvents,
} from "./helpers.js";

export function useFlowDerivedState({
  flowGraphMeta,
  flowEvents,
  flowView,
  flowReplayIndex,
  flowStage,
  flowRubric,
  flowGraph,
  flowViewGraph,
  selectedFlowNodeID,
  setSelectedFlowNodeID,
  selectedFlowEdgeID,
  setSelectedFlowEdgeID,
}) {
  const flowStageOptions = useMemo(() => getFlowStageOptions(flowGraphMeta), [flowGraphMeta]);
  const flowRubricOptions = useMemo(() => getFlowRubricOptions(flowGraphMeta), [flowGraphMeta]);
  const flowEventStageMap = useMemo(() => getFlowEventStageMap(flowGraphMeta), [flowGraphMeta]);
  const visibleFlowEvents = useMemo(
    () => getVisibleFlowEvents(flowEvents, flowView, flowReplayIndex, flowStage, flowRubric, flowEventStageMap),
    [flowEvents, flowView, flowReplayIndex, flowStage, flowRubric, flowEventStageMap],
  );
  const selectedFlowSummary = useMemo(
    () => summarizeFlowEvents(visibleFlowEvents, flowEventStageMap),
    [flowEventStageMap, visibleFlowEvents],
  );
  const flowActiveEdgeKeys = useMemo(() => getFlowActiveEdgeKeys(visibleFlowEvents), [visibleFlowEvents]);

  useEffect(() => {
    if (!selectedFlowNodeID) return;
    const currentGraph = flowViewGraph || flowGraph;
    const exists = ((currentGraph && currentGraph.nodes) || []).some((node) => node.id === selectedFlowNodeID);
    if (!exists) setSelectedFlowNodeID("");
  }, [flowGraph, flowViewGraph, selectedFlowNodeID, setSelectedFlowNodeID]);

  useEffect(() => {
    if (!selectedFlowEdgeID) return;
    const currentGraph = flowViewGraph || flowGraph;
    const exists = ((currentGraph && currentGraph.edges) || []).some((edge, index) => `${edge.kind}:${edge.from}->${edge.to}:${index}` === selectedFlowEdgeID);
    if (!exists) setSelectedFlowEdgeID("");
  }, [flowGraph, flowViewGraph, selectedFlowEdgeID, setSelectedFlowEdgeID]);

  return {
    flowStageOptions,
    flowRubricOptions,
    visibleFlowEvents,
    selectedFlowSummary,
    flowActiveEdgeKeys,
  };
}
