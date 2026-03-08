import { useCallback, useMemo } from "react";
import { usePersistentState } from "../../hooks/usePersistentState.js";
import { isAttentionAgent, sortAttentionAgents } from "./triage.js";

function parsePinned(raw) {
  if (!raw) return [];
  try {
    const items = JSON.parse(raw);
    if (!Array.isArray(items)) return [];
    return items.filter((item) => typeof item === "string" && item.trim() !== "");
  } catch {
    return [];
  }
}

function matchesStateFilter(agent, stateFilter) {
  if (stateFilter === "all") return true;
  return (agent.state || "idle") === stateFilter;
}

function matchesFocus(agent, focus, pinnedSet) {
  if (focus === "all") return true;
  if (focus === "attention") return isAttentionAgent(agent);
  if (focus === "pinned") return pinnedSet.has(agent.id);
  return true;
}

export function useAgentTriageState({ groupedAgents, agentsResp }) {
  const [focus, setFocus] = usePersistentState("dashboard_agents_focus", "all");
  const [stateFilter, setStateFilter] = usePersistentState("dashboard_agents_state_filter", "all");
  const [pinnedRaw, setPinnedRaw] = usePersistentState("dashboard_agents_pins", "[]");

  const pinnedAgentIDs = useMemo(() => parsePinned(pinnedRaw), [pinnedRaw]);
  const pinnedSet = useMemo(() => new Set(pinnedAgentIDs), [pinnedAgentIDs]);

  const allAgents = useMemo(
    () => [...(groupedAgents.holding || []), ...(groupedAgents.opcos || []).flatMap((group) => group.agents || [])],
    [groupedAgents],
  );

  const attentionAgents = useMemo(
    () => sortAttentionAgents(allAgents.filter(isAttentionAgent)),
    [allAgents],
  );

  const pinnedAgents = useMemo(
    () => sortAttentionAgents(allAgents.filter((agent) => pinnedSet.has(agent.id))),
    [allAgents, pinnedSet],
  );

  const filterAgents = useCallback(
    (agents) => (agents || []).filter((agent) => matchesStateFilter(agent, stateFilter) && matchesFocus(agent, focus, pinnedSet)),
    [focus, pinnedSet, stateFilter],
  );

  const visibleHolding = useMemo(() => filterAgents(groupedAgents.holding), [filterAgents, groupedAgents.holding]);
  const visibleOpcos = useMemo(
    () => (groupedAgents.opcos || [])
      .map((group) => ({ ...group, agents: filterAgents(group.agents) }))
      .filter((group) => group.agents.length > 0),
    [filterAgents, groupedAgents.opcos],
  );
  const visibleAttentionAgents = useMemo(
    () => attentionAgents.filter((agent) => matchesStateFilter(agent, stateFilter)),
    [attentionAgents, stateFilter],
  );
  const visiblePinnedAgents = useMemo(
    () => pinnedAgents.filter((agent) => matchesStateFilter(agent, stateFilter)),
    [pinnedAgents, stateFilter],
  );

  const summary = useMemo(() => ({
    total: allAgents.length,
    attention: attentionAgents.length,
    pinned: pinnedAgents.length,
    pending: allAgents.filter((agent) => (agent.pending_events || 0) > 0).length,
    leases: allAgents.filter((agent) => !!agent.lock_owner || !!agent.in_flight_turn).length,
    breaker: allAgents.filter((agent) => !!agent.near_breaker).length,
    failedTools: allAgents.filter((agent) => agent.last_tool && agent.last_tool.ok === false).length,
    stuck: agentsResp.states?.stuck || 0,
  }), [agentsResp.states?.stuck, allAgents, attentionAgents.length, pinnedAgents.length]);

  const togglePinned = useCallback((agentID) => {
    const next = new Set(pinnedSet);
    if (next.has(agentID)) next.delete(agentID);
    else next.add(agentID);
    setPinnedRaw(JSON.stringify(Array.from(next).sort()));
  }, [pinnedSet, setPinnedRaw]);

  return {
    focus,
    setFocus,
    stateFilter,
    setStateFilter,
    pinnedAgentIDs,
    attentionAgents: visibleAttentionAgents,
    pinnedAgents: visiblePinnedAgents,
    visibleHolding,
    visibleOpcos,
    summary,
    togglePinned,
  };
}
