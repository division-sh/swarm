import { fetchJSON } from "../client.ts";
import type { GenericInstance, GenericInstanceTimer } from "../../types/server.ts";

type RawInstance = {
  InstanceID?: string;
  StorageRef?: string;
  WorkflowName?: string;
  WorkflowVersion?: string;
  CurrentState?: string;
  Config?: Record<string, unknown>;
  EnteredStageAt?: string;
  TransitionHistory?: unknown[];
  StateBuckets?: Record<string, unknown>;
  TimerState?: Array<{
    TimerID?: string;
    EventType?: string;
    CreatedAt?: string;
    FiresAt?: string;
    StartedBy?: string;
    Recurring?: boolean;
    Cancelled?: boolean;
  }>;
  Metadata?: Record<string, unknown>;
  CreatedAt?: string;
  UpdatedAt?: string;
};

function normalizeTimer(timer: NonNullable<RawInstance["TimerState"]>[number]): GenericInstanceTimer {
  return {
    timer_id: timer?.TimerID,
    event_type: timer?.EventType,
    created_at: timer?.CreatedAt,
    fires_at: timer?.FiresAt,
    started_by: timer?.StartedBy,
    recurring: timer?.Recurring,
    cancelled: timer?.Cancelled,
  };
}

function normalizeInstance(row: RawInstance): GenericInstance {
  return {
    instance_id: row.InstanceID || "",
    storage_ref: row.StorageRef,
    workflow_name: row.WorkflowName,
    workflow_version: row.WorkflowVersion,
    current_state: row.CurrentState,
    config: row.Config || {},
    entered_stage_at: row.EnteredStageAt,
    transition_history: row.TransitionHistory || [],
    state_buckets: row.StateBuckets || {},
    timer_state: Array.isArray(row.TimerState) ? row.TimerState.map(normalizeTimer) : [],
    metadata: row.Metadata || {},
    created_at: row.CreatedAt,
    updated_at: row.UpdatedAt,
  };
}

export async function fetchGenericInstances(): Promise<GenericInstance[]> {
  const d = await fetchJSON<{ instances?: RawInstance[] }>("/api/instances");
  return Array.isArray(d.instances) ? d.instances.map(normalizeInstance) : [];
}

export async function fetchGenericInstanceDetail(instanceID: string): Promise<GenericInstance> {
  const row = await fetchJSON<RawInstance>(`/api/instances/${encodeURIComponent(instanceID)}`);
  return normalizeInstance(row);
}
