export const VALID_TABS = ["overview", "agents", "observability", "workflow", "portfolio", "operations", "digest", "events", "logs", "incidents", "flow", "convos", "graph", "control", "tasks", "pipeline", "holding", "health"];

export const DASHBOARD_TABS = [
  ["overview", "Overview"],
  ["agents", "Agents"],
  ["observability", "Observability"],
  ["workflow", "Workflow"],
  ["portfolio", "Portfolio"],
  ["operations", "Operations"],
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
  const overviewCount = (agentsResp.states.stuck || 0)
    + ((mailbox.summary.pending || 0) > 0 ? 1 : 0)
    + ((incidentsData || []).length > 0 ? 1 : 0)
    + (((holdingData.workflow_summary || {}).drift || 0) > 0 ? 1 : 0);

  return {
    overview: overviewCount > 0 ? { n: Math.min(overviewCount, 99), type: incidentsData.length > 0 || (agentsResp.states.stuck || 0) > 0 ? "danger" : "warn" } : null,
    observability: (incidentsData || []).length > 0 ? { n: Math.min((incidentsData || []).length, 99), type: "danger" } : null,
    workflow: (flowEvents || []).length > 0 ? { n: Math.min((flowEvents || []).length, 999), type: "warn" } : null,
    portfolio: Math.max((funnel.stuck || []).length, (holdingData.verticals || []).filter((vertical) => vertical.stage === "ready_for_review").length) > 0
      ? {
          n: Math.min(
            Math.max(
              (funnel.stuck || []).length,
              (holdingData.verticals || []).filter((vertical) => vertical.stage === "ready_for_review").length,
            ),
            99,
          ),
          type: "warn",
        }
      : null,
    agents: (agentsResp.states.stuck || 0) > 0 ? { n: agentsResp.states.stuck, type: "danger" } : null,
    operations: (mailbox.summary.pending || 0) > 0 ? { n: mailbox.summary.pending, type: "warn" } : null,
    pipeline: (funnel.stuck || []).length > 0 ? { n: funnel.stuck.length, type: "warn" } : null,
    holding: (() => {
      const count = (holdingData.verticals || []).filter((vertical) => vertical.stage === "ready_for_review").length;
      return count > 0 ? { n: count, type: "warn" } : null;
    })(),
    incidents: (incidentsData || []).length > 0 ? { n: incidentsData.length, type: "danger" } : null,
    flow: (flowEvents || []).length > 0 ? { n: Math.min((flowEvents || []).length, 999), type: "warn" } : null,
  };
}
