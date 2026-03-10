import { useMemo } from "react";
import { useQuery, useQueryClient, type QueryObserverResult } from "@tanstack/react-query";
import { fetchGraph, fetchWorkflowFlow } from "../api/dashboardWorkflow.ts";
import { dashboardQueryKeys } from "./dashboardQueryKeys.ts";
import type { FlowEventRecord, WorkflowFlowResponse } from "../types/workflow.ts";

async function runRefetch<T>(refetch: () => Promise<QueryObserverResult<T, Error>>): Promise<T | undefined> {
  const result = await refetch();
  if (result.error) throw result.error;
  return result.data;
}

export function useDashboardWorkflowQueries({
  activeView,
  activeSubview,
  graphMode,
  graphVertical,
  flowView,
  flowVertical,
  flowStart,
  flowEnd,
}: {
  activeView: string;
  activeSubview: string;
  graphMode: string;
  graphVertical: string;
  flowView: string;
  flowVertical: string;
  flowStart: string;
  flowEnd: string;
}) {
  const queryClient = useQueryClient();
  const workflowSubview = activeView === "workflow" ? (activeSubview || "flow") : activeView;
  const graphEnabled = workflowSubview === "graph";
  const flowEnabled = workflowSubview === "flow";

  const graphQuery = useQuery({
    queryKey: dashboardQueryKeys.graph(graphMode, graphVertical),
    queryFn: () => fetchGraph({ graphMode, graphVertical }),
    enabled: graphEnabled && (graphMode !== "opco" || !!graphVertical),
    refetchInterval: 22000,
  });

  const flowQuery = useQuery<WorkflowFlowResponse>({
    queryKey: dashboardQueryKeys.workflowFlow(flowView, flowVertical, flowStart, flowEnd),
    queryFn: () => fetchWorkflowFlow({
      flowView,
      flowVertical,
      flowStart,
      flowEnd,
    }),
    enabled: flowEnabled,
    refetchInterval: flowView === "runtime" ? false : 22000,
  });

  const patchRuntimeFlowEvent = useMemo(() => (item: FlowEventRecord) => {
    queryClient.setQueryData<WorkflowFlowResponse | undefined>(
      dashboardQueryKeys.workflowFlow(flowView, flowVertical, flowStart, flowEnd),
      (prev) => {
        const current = prev || { nodes: [], edges: [], meta: {}, flow_events: [] };
        const rows = [item, ...(current.flow_events || []).filter((entry) => entry.event_id !== item.event_id)];
        return {
          ...current,
          flow_events: rows.slice(0, 500),
        };
      },
    );
  }, [flowEnd, flowStart, flowVertical, flowView, queryClient]);

  return useMemo(() => ({
    data: {
      graph: graphQuery.data || { nodes: [], edges: [] },
      flowGraph: {
        nodes: flowQuery.data?.nodes || [],
        edges: flowQuery.data?.edges || [],
      },
      flowGraphMeta: flowQuery.data?.meta || {},
      flowEvents: flowQuery.data?.flow_events || [],
    },
    queries: {
      graph: graphQuery,
      flow: flowQuery,
    },
    loaders: {
      loadGraph: () => runRefetch(graphQuery.refetch),
      loadPipelineFlow: () => runRefetch(flowQuery.refetch),
    },
    patchRuntimeFlowEvent,
  }), [flowQuery, graphQuery, patchRuntimeFlowEvent]);
}
