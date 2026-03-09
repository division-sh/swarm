import { useMemo } from "react";
import type { AgentRecord } from "../../types/core.ts";
import type { GraphResponse, WorkflowFlowResponse } from "../../types/workflow.ts";

type AsyncAction = () => Promise<unknown>;
type StringSetter = (value: string) => void;
type BoolSetter = (value: boolean) => void;
type NumberSetter = (value: number) => void;
type ReplayIndexSetter = (value: number | ((prev: number) => number)) => void;

type FlowControllerInput = {
  verticals: Record<string, any>[];
  visibleFlowEvents: Record<string, any>[];
  flowEvents: WorkflowFlowResponse["flow_events"];
  flowGraph: GraphResponse;
  flowGraphMeta: Record<string, any>;
  flowActiveEdgeKeys: Set<string>;
  selectedFlowSummary: Record<string, any> | null;
  agents: AgentRecord[];
  flowView: string;
  flowStage: string;
  flowStageOptions: string[];
  flowRubric: string;
  flowRubricOptions: string[];
  flowVertical: string;
  flowStart: string;
  flowEnd: string;
  flowReplaySpeed: number;
  flowReplayOn: boolean;
  flowReplayIndex: number;
  selectedFlowNodeID: string;
  selectedFlowEdgeID: string;
  flowViewGraph: GraphResponse;
  graphFullscreen: boolean;
  setFlowView: StringSetter;
  setFlowStage: StringSetter;
  setFlowRubric: StringSetter;
  setFlowVertical: StringSetter;
  setFlowStart: StringSetter;
  setFlowEnd: StringSetter;
  setFlowReplaySpeed: NumberSetter;
  setFlowReplayOn: BoolSetter;
  setFlowReplayIndex: ReplayIndexSetter;
  refresh: AsyncAction;
  addToast: (message: string, type?: string) => void;
  setSelectedFlowNodeID: StringSetter;
  setSelectedFlowEdgeID: StringSetter;
  setFlowViewGraph: (value: GraphResponse) => void;
  setGraphFullscreen: BoolSetter;
};

export function useFlowController({
  verticals,
  visibleFlowEvents,
  flowEvents,
  flowGraph,
  flowGraphMeta,
  flowActiveEdgeKeys,
  selectedFlowSummary,
  agents,
  flowView,
  flowStage,
  flowStageOptions,
  flowRubric,
  flowRubricOptions,
  flowVertical,
  flowStart,
  flowEnd,
  flowReplaySpeed,
  flowReplayOn,
  flowReplayIndex,
  selectedFlowNodeID,
  selectedFlowEdgeID,
  flowViewGraph,
  graphFullscreen,
  setFlowView,
  setFlowStage,
  setFlowRubric,
  setFlowVertical,
  setFlowStart,
  setFlowEnd,
  setFlowReplaySpeed,
  setFlowReplayOn,
  setFlowReplayIndex,
  refresh,
  addToast,
  setSelectedFlowNodeID,
  setSelectedFlowEdgeID,
  setFlowViewGraph,
  setGraphFullscreen,
}: FlowControllerInput) {
  return useMemo(() => ({
    state: {
      verticals,
      visibleFlowEvents,
      flowEvents,
      flowGraph,
      flowGraphMeta,
      flowActiveEdgeKeys,
      selectedFlowSummary,
      agents,
      flowView,
      flowStage,
      flowStageOptions,
      flowRubric,
      flowRubricOptions,
      flowVertical,
      flowStart,
      flowEnd,
      flowReplaySpeed,
      flowReplayOn,
      flowReplayIndex,
      selectedFlowNodeID,
      selectedFlowEdgeID,
      flowViewGraph,
      graphFullscreen,
    },
    actions: {
      setFlowView,
      setFlowStage,
      setFlowRubric,
      setFlowVertical,
      setFlowStart,
      setFlowEnd,
      setFlowReplaySpeed,
      setFlowReplayOn,
      setFlowReplayIndex,
      refresh,
      addToast,
      setSelectedFlowNodeID,
      setSelectedFlowEdgeID,
      setFlowViewGraph,
      setGraphFullscreen,
    },
  }), [
    addToast,
    agents,
    flowActiveEdgeKeys,
    flowEnd,
    flowEvents,
    flowGraph,
    flowGraphMeta,
    flowReplayIndex,
    flowReplayOn,
    flowReplaySpeed,
    flowRubric,
    flowRubricOptions,
    flowStage,
    flowStageOptions,
    flowStart,
    flowVertical,
    flowView,
    flowViewGraph,
    graphFullscreen,
    refresh,
    selectedFlowEdgeID,
    selectedFlowNodeID,
    selectedFlowSummary,
    setFlowEnd,
    setFlowReplayIndex,
    setFlowReplayOn,
    setFlowReplaySpeed,
    setFlowRubric,
    setFlowStage,
    setFlowStart,
    setFlowVertical,
    setFlowView,
    setFlowViewGraph,
    setGraphFullscreen,
    setSelectedFlowEdgeID,
    setSelectedFlowNodeID,
    verticals,
    visibleFlowEvents,
  ]);
}
