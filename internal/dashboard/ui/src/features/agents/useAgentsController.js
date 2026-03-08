import { useMemo } from "react";

export function useAgentsController({
  agentsResp,
  groupedAgents,
  agentSearch,
  selectedAgentID,
  setAgentSearch,
  setSelectedAgentID,
  renderAgentDropdown,
  navigateToTask,
}) {
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
