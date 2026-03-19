import type { HealthResponse } from "../types/core.ts";
import type { GenericHealthResponse } from "../types/server.ts";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? { ...(value as Record<string, unknown>) } : {};
}

function asBoolean(value: unknown): boolean {
  return value === true;
}

function asNumber(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

export function adaptHealth(resp: GenericHealthResponse): HealthResponse {
  const checks = asRecord(resp.checks);
  const runtime = asRecord(checks.runtime);
  const database = asRecord(checks.database);
  const out: HealthResponse = {
    ok: resp.ok,
    timestamp: resp.timestamp,
    runtime: {
      running: asBoolean(runtime.ready),
      loaded_agents: asNumber(runtime.agents),
      flow_count: asNumber(runtime.flows),
      node_count: asNumber(runtime.nodes),
      event_catalog_count: asNumber(runtime.events),
    },
    postgres: {
      active_connections: 0,
      max_connections: 0,
    },
    workflow_audit: {
      warnings: [],
    },
  };
  if (Object.keys(database).length > 0) {
    out.database = database;
  }
  return out;
}
