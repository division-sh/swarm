import type { AgentRecord } from "../../types/core.ts";

function num(value: unknown): number {
  return Number(value || 0);
}

export function isAttentionAgent(agent: AgentRecord | null | undefined) {
  if (!agent || typeof agent !== "object") return false;
  return (
    agent.state === "stuck"
    || num(agent.pending_events) > 0
    || !!agent.in_flight_turn
    || !!agent.near_breaker
    || num(agent.failures_24h) > 0
    || num(agent.dead_letters_24h) > 0
    || (agent.last_tool && agent.last_tool.ok === false)
  );
}

export function attentionScore(agent: AgentRecord | null | undefined) {
  if (!agent || typeof agent !== "object") return 0;
  let score = 0;
  if (agent.state === "stuck") score += 1000;
  if (num(agent.pending_events) > 0) score += 200 + Math.min(num(agent.pending_events), 20) * 5;
  if (num(agent.oldest_pending_age_sec) > 0) score += Math.min(num(agent.oldest_pending_age_sec), 86400) / 300;
  if (agent.in_flight_turn) score += 120 + Math.min(num(agent.in_flight_seconds), 3600) / 30;
  if (agent.near_breaker) score += 150;
  if (num(agent.failures_24h) > 0) score += num(agent.failures_24h) * 40;
  if (num(agent.dead_letters_24h) > 0) score += num(agent.dead_letters_24h) * 60;
  if (agent.last_tool && agent.last_tool.ok === false) score += 90;
  return score;
}

export function sortAttentionAgents(agents: AgentRecord[] | null | undefined) {
  return [...(agents || [])].sort((a, b) => (
    attentionScore(b) - attentionScore(a)
      || (num(b.pending_events) - num(a.pending_events))
      || (num(b.failures_24h) - num(a.failures_24h))
      || ((a.id || "").localeCompare(b.id || ""))
  ));
}
