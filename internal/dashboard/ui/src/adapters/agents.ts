import type { AgentRecord, AgentsResponse } from "../types/core.ts";
import type { GenericAgent } from "../types/server.ts";

function inferAgentState(agent: GenericAgent): string {
  const explicit = String(agent.state || "").trim().toLowerCase();
  if (explicit) return explicit;
  const status = String(agent.status || "").trim().toLowerCase();
  switch (status) {
    case "terminated":
      return "terminated";
    case "paused":
      return "idle";
    case "active":
      return "idle";
    default:
      return "idle";
  }
}

export function adaptAgent(agent: GenericAgent): AgentRecord {
  return {
    id: agent.id,
    agent_id: agent.id,
    role: agent.role,
    family: agent.type,
    status: agent.status,
    state: inferAgentState(agent),
    runtime_mode: agent.mode,
    vertical_id: agent.entity_id,
    pending_events: agent.pending_events,
    oldest_pending_age_sec: agent.oldest_pending_age_sec,
    in_flight_turn: agent.in_flight_turn,
    in_flight_seconds: agent.in_flight_seconds,
    lock_owner: agent.lock_owner,
    lock_expires_at: agent.lock_expires_at,
    failures_24h: agent.failures_24h,
    dead_letters_24h: agent.dead_letters_24h,
    turn_count: agent.turn_count,
    turn_limit: agent.turn_limit,
    turns_24h: agent.turns_24h,
    total_tokens_24h: agent.total_tokens_24h,
    near_breaker: agent.near_breaker,
    current_task_id: agent.current_task_id,
    last_tool: agent.last_tool,
    started_at: agent.started_at,
    permissions: agent.permissions,
    subscriptions: agent.subscriptions,
    parent_agent_id: agent.parent_agent_id,
    coordinator_id: agent.coordinator_id,
    hired_by: agent.hired_by,
    template_version: agent.template_version,
    budget_envelope: agent.budget_envelope,
  };
}

export function adaptAgents(agents: GenericAgent[]): AgentsResponse {
  const items = agents.map(adaptAgent);
  const states: Record<string, number> = {};
  for (const agent of items) {
    const key = String(agent.state || "idle");
    states[key] = Number(states[key] || 0) + 1;
  }
  return { agents: items, states };
}
