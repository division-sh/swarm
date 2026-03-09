import { useMemo } from "react";
import {
  useDashboardOpsState,
  useDashboardPipelineState,
  useDashboardRuntimeState,
  useDashboardTaskState,
} from "./useDashboardStateBuckets.ts";

export function useDashboardDomainState() {
  const taskState = useDashboardTaskState();
  const runtimeState = useDashboardRuntimeState();
  const opsState = useDashboardOpsState();
  const pipelineState = useDashboardPipelineState();

  return useMemo(() => ({
    taskState,
    runtimeState,
    opsState,
    pipelineState,
  }), [
    taskState,
    runtimeState,
    opsState,
    pipelineState,
  ]);
}
