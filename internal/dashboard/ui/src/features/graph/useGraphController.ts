import { useMemo } from "react";
import type { AgentRecord } from "../../types/core.ts";
import type { GraphResponse } from "../../types/workflow.ts";

type StringSetter = (value: string) => void;
type BoolSetter = (value: boolean) => void;
type AsyncAction = () => Promise<unknown>;
type GraphControllerInput = {
  verticals: Record<string, any>[];
  graph: GraphResponse;
  graphViewGraph: GraphResponse;
  agents: AgentRecord[];
  graphMode: string;
  graphVertical: string;
  selectedGraphNodeID: string;
  selectedGraphEdgeID: string;
  graphFullscreen: boolean;
  setGraphMode: StringSetter;
  setGraphVertical: StringSetter;
  setSelectedGraphNodeID: StringSetter;
  setSelectedGraphEdgeID: StringSetter;
  setGraphFullscreen: BoolSetter;
  refreshGraph: AsyncAction;
  setGraphViewGraph: (value: GraphResponse) => void;
  restartAgent: (agentID: string) => void;
  openControl: (agentID: string) => void;
  inspectAgent: (agentID: string) => void;
  navigateToTask: (taskID: string) => void;
};

export function useGraphController({
  verticals,
  graph,
  graphViewGraph,
  agents,
  graphMode,
  graphVertical,
  selectedGraphNodeID,
  selectedGraphEdgeID,
  graphFullscreen,
  setGraphMode,
  setGraphVertical,
  setSelectedGraphNodeID,
  setSelectedGraphEdgeID,
  setGraphFullscreen,
  refreshGraph,
  setGraphViewGraph,
  restartAgent,
  openControl,
  inspectAgent,
  navigateToTask,
}: GraphControllerInput) {
  return useMemo(() => ({
    state: {
      verticals,
      graph,
      graphViewGraph,
      agents,
      graphMode,
      graphVertical,
      selectedGraphNodeID,
      selectedGraphEdgeID,
      graphFullscreen,
    },
    actions: {
      setGraphMode,
      setGraphVertical,
      setSelectedGraphNodeID,
      setSelectedGraphEdgeID,
      setGraphFullscreen,
      refreshGraph,
      setGraphViewGraph,
      restartAgent,
      openControl,
      inspectAgent,
      navigateToTask,
    },
  }), [
    agents,
    graph,
    graphFullscreen,
    graphMode,
    graphVertical,
    graphViewGraph,
    inspectAgent,
    navigateToTask,
    openControl,
    refreshGraph,
    restartAgent,
    selectedGraphEdgeID,
    selectedGraphNodeID,
    setGraphFullscreen,
    setGraphMode,
    setGraphVertical,
    setGraphViewGraph,
    setSelectedGraphEdgeID,
    setSelectedGraphNodeID,
    verticals,
  ]);
}
