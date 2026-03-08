export function isAttentionAgent(agent) {
  if (!agent || typeof agent !== "object") return false;
  return (
    agent.state === "stuck"
    || (agent.pending_events || 0) > 0
    || !!agent.in_flight_turn
    || !!agent.near_breaker
    || (agent.failures_24h || 0) > 0
    || (agent.dead_letters_24h || 0) > 0
    || (agent.last_tool && agent.last_tool.ok === false)
  );
}

export function attentionScore(agent) {
  if (!agent || typeof agent !== "object") return 0;
  let score = 0;
  if (agent.state === "stuck") score += 1000;
  if ((agent.pending_events || 0) > 0) score += 200 + Math.min(agent.pending_events || 0, 20) * 5;
  if ((agent.oldest_pending_age_sec || 0) > 0) score += Math.min(agent.oldest_pending_age_sec || 0, 86400) / 300;
  if (agent.in_flight_turn) score += 120 + Math.min(agent.in_flight_seconds || 0, 3600) / 30;
  if (agent.near_breaker) score += 150;
  if ((agent.failures_24h || 0) > 0) score += (agent.failures_24h || 0) * 40;
  if ((agent.dead_letters_24h || 0) > 0) score += (agent.dead_letters_24h || 0) * 60;
  if (agent.last_tool && agent.last_tool.ok === false) score += 90;
  return score;
}

export function sortAttentionAgents(agents) {
  return [...(agents || [])].sort((a, b) => (
    attentionScore(b) - attentionScore(a)
      || ((b.pending_events || 0) - (a.pending_events || 0))
      || ((b.failures_24h || 0) - (a.failures_24h || 0))
      || ((a.id || "").localeCompare(b.id || ""))
  ));
}
