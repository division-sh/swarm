import { fetchJSON } from "./client.ts";
import { adaptHealth } from "../adapters/health.ts";
import { fetchGenericHealth } from "./resources/health.ts";
import type { HealthResponse } from "../types/core.ts";

export async function fetchHealth(): Promise<HealthResponse> {
  const generic = await fetchGenericHealth();
  return adaptHealth(generic);
}
