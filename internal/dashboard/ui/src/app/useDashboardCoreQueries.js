import { useMemo, useEffect } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  fetchDashboardAgents,
  fetchDashboardHealth,
  fetchDigest,
  fetchMailbox,
  fetchOverview,
  fetchTargets,
  fetchTasks,
  fetchTaskStats,
} from "../api/dashboardCore.js";
import { relTime } from "../lib/format.js";
import { dashboardQueryKeys } from "./dashboardQueryKeys.js";

async function runRefetch(refetch) {
  const result = await refetch();
  if (result.error) throw result.error;
  return result.data;
}

export function useDashboardCoreQueries({
  taskStatus,
  mailStatus,
  controlTarget,
  setControlTarget,
  setStatusText,
}) {
  const queryClient = useQueryClient();

  const overviewQuery = useQuery({
    queryKey: dashboardQueryKeys.overview(),
    queryFn: fetchOverview,
    refetchInterval: 15000,
  });
  const agentsQuery = useQuery({
    queryKey: dashboardQueryKeys.agents(),
    queryFn: fetchDashboardAgents,
    refetchInterval: 15000,
  });
  const digestQuery = useQuery({
    queryKey: dashboardQueryKeys.digest(),
    queryFn: () => fetchDigest(10),
    refetchInterval: 15000,
  });
  const tasksQuery = useQuery({
    queryKey: dashboardQueryKeys.tasks(taskStatus),
    queryFn: () => fetchTasks(taskStatus),
    refetchInterval: 15000,
  });
  const taskStatsQuery = useQuery({
    queryKey: dashboardQueryKeys.taskStats(),
    queryFn: fetchTaskStats,
    enabled: false,
  });
  const mailboxQuery = useQuery({
    queryKey: dashboardQueryKeys.mailbox(mailStatus),
    queryFn: () => fetchMailbox(mailStatus),
    refetchInterval: 15000,
  });
  const healthQuery = useQuery({
    queryKey: dashboardQueryKeys.health(),
    queryFn: fetchDashboardHealth,
    refetchInterval: 15000,
  });
  const targetsQuery = useQuery({
    queryKey: dashboardQueryKeys.targets(),
    queryFn: fetchTargets,
    refetchInterval: 22000,
  });

  useEffect(() => {
    if (!overviewQuery.data?.generated_at) return;
    setStatusText(`Updated ${relTime(overviewQuery.data.generated_at)}`);
  }, [overviewQuery.data?.generated_at, setStatusText]);

  useEffect(() => {
    if (controlTarget || !Array.isArray(targetsQuery.data) || targetsQuery.data.length === 0) return;
    setControlTarget(targetsQuery.data[0].agent_id);
  }, [controlTarget, setControlTarget, targetsQuery.data]);

  const invalidate = useMemo(() => ({
    overview: () => queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.overview() }),
    agents: () => queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.agents() }),
    digest: () => queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.digest() }),
    tasks: () => queryClient.invalidateQueries({ queryKey: ["dashboard", "tasks"] }),
    mailbox: () => queryClient.invalidateQueries({ queryKey: ["dashboard", "mailbox"] }),
    health: () => queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.health() }),
    targets: () => queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.targets() }),
    taskStats: () => queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.taskStats() }),
  }), [queryClient]);

  return useMemo(() => ({
    data: {
      overview: overviewQuery.data || {},
      agentsResp: agentsQuery.data || { agents: [], states: {} },
      digestResp: digestQuery.data || null,
      tasksResp: tasksQuery.data || { tasks: [] },
      tasksStats: taskStatsQuery.data || null,
      mailbox: mailboxQuery.data || { summary: {}, items: [] },
      health: healthQuery.data || {},
      targets: targetsQuery.data || [],
    },
    queries: {
      overview: overviewQuery,
      agents: agentsQuery,
      digest: digestQuery,
      tasks: tasksQuery,
      taskStats: taskStatsQuery,
      mailbox: mailboxQuery,
      health: healthQuery,
      targets: targetsQuery,
    },
    loaders: {
      loadOverview: () => runRefetch(overviewQuery.refetch),
      loadAgents: () => runRefetch(agentsQuery.refetch),
      loadDigest: () => runRefetch(digestQuery.refetch),
      loadTasks: () => runRefetch(tasksQuery.refetch),
      loadTaskStats: () => runRefetch(taskStatsQuery.refetch),
      loadMailbox: () => runRefetch(mailboxQuery.refetch),
      loadHealth: () => runRefetch(healthQuery.refetch),
      loadTargets: () => runRefetch(targetsQuery.refetch),
    },
    invalidate,
    isInitialLoading: [
      overviewQuery,
      agentsQuery,
      digestQuery,
      tasksQuery,
      mailboxQuery,
      healthQuery,
      targetsQuery,
    ].some((query) => query.isLoading),
  }), [
    agentsQuery,
    digestQuery,
    healthQuery,
    invalidate,
    mailboxQuery,
    overviewQuery,
    targetsQuery,
    taskStatsQuery,
    tasksQuery,
  ]);
}
