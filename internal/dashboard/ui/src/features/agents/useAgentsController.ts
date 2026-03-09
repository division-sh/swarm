import { useMemo } from "react";
import type { AgentsResponse, AgentRecord } from "../../types/core.ts";

type AgentsGroup = {
  slug?: string;
  agents: AgentRecord[];
};

type GroupedAgents = {
  holding: AgentRecord[];
  opcos: AgentsGroup[];
};

type AgentsControllerInput = {
  agentsResp: AgentsResponse;
  groupedAgents: GroupedAgents;
  agentSearch: string;
  selectedAgentID: string;
  setAgentSearch: (value: string) => void;
  setSelectedAgentID: (value: string) => void;
  renderAgentDropdown: (agent: AgentRecord) => unknown;
  navigateToTask: (taskID: string) => void;
};

export function useAgentsController({
  agentsResp,
  groupedAgents,
  agentSearch,
  selectedAgentID,
  setAgentSearch,
  setSelectedAgentID,
  renderAgentDropdown,
  navigateToTask,
}: AgentsControllerInput) {
  return useMemo(() => ({
    state: { agentsResp, groupedAgents, agentSearch, selectedAgentID },
    actions: { setAgentSearch, setSelectedAgentID, renderAgentDropdown, navigateToTask },
  }), [
    agentSearch,
    agentsResp,
    groupedAgents,
    navigateToTask,
    renderAgentDropdown,
    selectedAgentID,
    setAgentSearch,
    setSelectedAgentID,
  ]);
}
