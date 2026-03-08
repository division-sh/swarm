import { useOverviewController } from "../features/overview/useOverviewController.js";

export function useDashboardOverviewAssembly({
  overview,
  digestResp,
  agentsResp,
  incidentsData,
  mailbox,
  tasksResp,
  health,
  funnel,
  holdingData,
  setActiveView,
}) {
  return useOverviewController({
    overview,
    digestResp,
    agentsResp,
    incidentsData,
    mailbox,
    tasksResp,
    health,
    funnel,
    holdingData,
    setActiveView,
  });
}
