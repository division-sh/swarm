import { useState } from "react";
import {
  useDashboardOpsState,
  useDashboardPipelineState,
  useDashboardRuntimeState,
  useDashboardTaskState,
} from "./useDashboardStateBuckets.js";

export function useDashboardDomainState() {
  const [overview, setOverview] = useState({});
  const [agentsResp, setAgentsResp] = useState({ agents: [], states: {} });
  const [digestResp, setDigestResp] = useState(null);
  const taskState = useDashboardTaskState();
  const runtimeState = useDashboardRuntimeState();
  const opsState = useDashboardOpsState();
  const pipelineState = useDashboardPipelineState();

  return {
    overview,
    setOverview,
    agentsResp,
    setAgentsResp,
    digestResp,
    setDigestResp,
    taskState,
    runtimeState,
    opsState,
    pipelineState,
  };
}
