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
  openView,
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
    openView,
  });
}
