import { fetchJSON } from "./client.ts";
import { adaptHolding } from "../adapters/holding.ts";
import { adaptHoldingDetail } from "../adapters/holdingDetail.ts";
import { fetchEvents } from "./dashboardRuntime.ts";
import { fetchGenericAgents } from "./resources/agents.ts";
import { fetchGenericMailbox } from "./resources/mailbox.ts";
import { fetchGenericInstanceDetail, fetchGenericInstances } from "./resources/instances.ts";
import type { HoldingResponse, HoldingVerticalDetail, VerticalRecord } from "../types/portfolio.ts";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function mergeHoldingDetail(base: HoldingVerticalDetail, supplement: HoldingVerticalDetail | null): HoldingVerticalDetail {
  if (!supplement) return base;
  const merged: HoldingVerticalDetail = {
    ...supplement,
    ...base,
    vertical: {
      ...asRecord(supplement.vertical),
      ...asRecord(base.vertical),
    } as VerticalRecord & { [key: string]: unknown },
    workflow_state: {
      ...asRecord(supplement.workflow_state),
      ...asRecord(base.workflow_state),
    },
  };
  if (!Array.isArray(merged.events) || merged.events.length === 0) merged.events = supplement.events || [];
  if (!Array.isArray(merged.mailbox) || merged.mailbox.length === 0) merged.mailbox = supplement.mailbox || [];
  if (!Array.isArray(merged.agents) || merged.agents.length === 0) merged.agents = supplement.agents || [];
  if (!Array.isArray(merged.team) || merged.team.length === 0) merged.team = supplement.team || merged.agents || [];
  if (!Array.isArray(merged.artifacts) || merged.artifacts.length === 0) merged.artifacts = supplement.artifacts || [];
  if (!merged.spend && supplement.spend) merged.spend = supplement.spend;
  if (!merged.workflow_state_error && supplement.workflow_state_error) merged.workflow_state_error = supplement.workflow_state_error;
  return merged;
}

export async function fetchHolding(): Promise<HoldingResponse> {
  const [instances, agents] = await Promise.all([
    fetchGenericInstances(),
    fetchGenericAgents(),
  ]);
  return adaptHolding(instances, agents);
}

export async function fetchHoldingVerticalDetail(verticalID: string): Promise<HoldingVerticalDetail | null> {
  const id = String(verticalID || "").trim();
  if (!id) return null;
  const [instance, agents, pendingMailbox, approvedMailbox, rejectedMailbox, deferredMailbox, events] = await Promise.all([
    fetchGenericInstanceDetail(id),
    fetchGenericAgents(),
    fetchGenericMailbox("pending", 150),
    fetchGenericMailbox("approved", 150),
    fetchGenericMailbox("rejected", 150),
    fetchGenericMailbox("deferred", 150),
    fetchEvents({ vertical: id }),
  ]);
  const scopedAgents = agents.filter((agent) => String(agent.entity_id || "").trim() === id);
  const scopedMailbox = [...pendingMailbox, ...approvedMailbox, ...rejectedMailbox, ...deferredMailbox]
    .filter((item) => String(item.entity_id || "").trim() === id);
  const generic = adaptHoldingDetail({
    instance,
    agents: scopedAgents,
    events,
    mailbox: scopedMailbox,
  });
  try {
    const legacy = await fetchJSON<HoldingVerticalDetail>(`/dashboard/api/holding/vertical?id=${encodeURIComponent(id)}`);
    return mergeHoldingDetail(generic, legacy);
  } catch {
    return generic;
  }
}
