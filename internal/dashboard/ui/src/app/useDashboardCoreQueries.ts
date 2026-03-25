import { useMemo, useEffect, useState } from "react";
import { useQuery, useQueryClient, type QueryObserverResult } from "@tanstack/react-query";
import {
  fetchDashboardAgents,
  fetchDashboardHealth,
  fetchDigest,
  fetchMailbox,
  fetchOverview,
  fetchTargets,
  fetchTasks,
  fetchTaskStats,
} from "../api/dashboardCore.ts";
import { relTime } from "../lib/format.ts";
import { dashboardQueryKeys } from "./dashboardQueryKeys.ts";
import type { AgentsResponse, DigestResponse, HealthResponse, MailboxResponse, OverviewResponse, TargetRecord, TaskStats, TasksResponse } from "../types/core.ts";

async function runRefetch<T>(refetch: () => Promise<QueryObserverResult<T, Error>>): Promise<T | undefined> {
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
}: {
  taskStatus: string;
  mailStatus: string;
  controlTarget: string;
  setControlTarget: (value: string) => void;
  setStatusText: (value: string) => void;
}) {
  const queryClient = useQueryClient();
  const [bootstrapped, setBootstrapped] = useState(false);

  const overviewQuery = useQuery<OverviewResponse>({
    queryKey: dashboardQueryKeys.overview(),
    queryFn: fetchOverview,
    refetchInterval: 15000,
  });
  const agentsQuery = useQuery<AgentsResponse>({
    queryKey: dashboardQueryKeys.agents(),
    queryFn: fetchDashboardAgents,
    refetchInterval: 15000,
  });
  const digestQuery = useQuery<DigestResponse>({
    queryKey: dashboardQueryKeys.digest(),
    queryFn: () => fetchDigest(10),
    refetchInterval: 15000,
  });
  const tasksQuery = useQuery<TasksResponse>({
    queryKey: dashboardQueryKeys.tasks(taskStatus),
    queryFn: () => fetchTasks(taskStatus),
    refetchInterval: 15000,
  });
  const taskStatsQuery = useQuery<TaskStats | null>({
    queryKey: dashboardQueryKeys.taskStats(),
    queryFn: fetchTaskStats,
    enabled: false,
  });
  const mailboxQuery = useQuery<MailboxResponse>({
    queryKey: dashboardQueryKeys.mailbox(mailStatus),
    queryFn: () => fetchMailbox(mailStatus),
    refetchInterval: 15000,
  });
  const healthQuery = useQuery<HealthResponse>({
    queryKey: dashboardQueryKeys.health(),
    queryFn: fetchDashboardHealth,
    refetchInterval: 15000,
  });
  const targetsQuery = useQuery<TargetRecord[]>({
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
    setControlTarget(String(targetsQuery.data[0].agent_id || ""));
  }, [controlTarget, setControlTarget, targetsQuery.data]);

  useEffect(() => {
    if (bootstrapped) return;
    const bootstrapQueries = [
      overviewQuery,
      agentsQuery,
      digestQuery,
      healthQuery,
      targetsQuery,
    ];
    const settled = bootstrapQueries.every((query) => !query.isLoading);
    if (settled) setBootstrapped(true);
  }, [agentsQuery, bootstrapped, digestQuery, healthQuery, overviewQuery, targetsQuery]);

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
      tasksResp: tasksQuery.data || { tasks: [], weekly_budget: {} },
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
    isInitialLoading: !bootstrapped,
  }), [
    agentsQuery,
    bootstrapped,
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
