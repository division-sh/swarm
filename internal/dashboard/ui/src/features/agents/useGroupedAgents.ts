import { useEffect, useMemo } from "react";
import type { AgentRecord } from "../../types/core.ts";

type GroupedAgents = {
  holding: AgentRecord[];
  opcos: Array<{ slug: string; agents: AgentRecord[] }>;
};

type GroupedAgentsInput = {
  agents: AgentRecord[];
  agentSearch: string;
  selectedAgentID: string;
  setSelectedAgentID: (value: string) => void;
};

export function useGroupedAgents({
  agents,
  agentSearch,
  selectedAgentID,
  setSelectedAgentID,
}: GroupedAgentsInput) {
  const groupedAgents = useMemo<GroupedAgents>(() => {
    const query = (agentSearch || "").trim().toLowerCase();
    const filtered = (agents || []).filter((agent) => {
      if (!query) return true;
      return `${agent.id} ${agent.role || ""} ${agent.state || ""} ${agent.vertical_slug || ""}`.toLowerCase().includes(query);
    });
    const holding = [];
    const opco = new Map();
    for (const agent of filtered) {
      const isHolding = !(agent.vertical_slug || agent.vertical_id) || agent.mode !== "operating";
      if (isHolding) {
        holding.push(agent);
      } else {
        const key = agent.vertical_slug || agent.vertical_id || "unknown";
        if (!opco.has(key)) opco.set(key, []);
        opco.get(key).push(agent);
      }
    }
    const opcos = Array.from(opco.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([slug, grouped]) => ({ slug, agents: grouped }));
    return { holding, opcos };
  }, [agentSearch, agents]);

  useEffect(() => {
    if (!selectedAgentID) return;
    const exists = (agents || []).some((agent) => agent.id === selectedAgentID);
    if (!exists) setSelectedAgentID("");
  }, [agents, selectedAgentID, setSelectedAgentID]);

  return groupedAgents;
}
