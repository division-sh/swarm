export const VALID_TABS = ["agents", "digest", "events", "logs", "incidents", "flow", "convos", "graph", "control", "tasks", "pipeline", "holding", "health"];

export const DASHBOARD_TABS = [
  ["agents", "Agents"],
  ["digest", "Digest"],
  ["events", "Events"],
  ["logs", "Logs"],
  ["incidents", "Incidents"],
  ["flow", "Flow"],
  ["convos", "Convos"],
  ["graph", "Graph"],
  ["control", "Control + Mailbox"],
  ["tasks", "Tasks"],
  ["pipeline", "Pipeline"],
  ["holding", "Holding"],
  ["health", "Health"],
];

export function buildTabBadges({
  agentsResp,
  mailbox,
  funnel,
  holdingData,
  incidentsData,
  flowEvents,
}) {
  return {
    agents: (agentsResp.states.stuck || 0) > 0 ? { n: agentsResp.states.stuck, type: "danger" } : null,
    control: (mailbox.summary.pending || 0) > 0 ? { n: mailbox.summary.pending, type: "warn" } : null,
    pipeline: (funnel.stuck || []).length > 0 ? { n: funnel.stuck.length, type: "warn" } : null,
    holding: (() => {
      const count = (holdingData.verticals || []).filter((vertical) => vertical.stage === "ready_for_review").length;
      return count > 0 ? { n: count, type: "warn" } : null;
    })(),
    incidents: (incidentsData || []).length > 0 ? { n: incidentsData.length, type: "danger" } : null,
    flow: (flowEvents || []).length > 0 ? { n: Math.min((flowEvents || []).length, 999), type: "warn" } : null,
  };
}
