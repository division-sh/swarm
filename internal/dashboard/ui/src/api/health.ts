import { fetchJSON } from "./client.ts";
import { adaptHealth } from "../adapters/health.ts";
import { fetchGenericHealth } from "./resources/health.ts";
import type { HealthResponse } from "../types/core.ts";

function isGenericEndpointUnavailable(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return error.message === "HTTP 404" || error.message === "HTTP 405" || error.message === "HTTP 501";
}

export async function fetchHealth(): Promise<HealthResponse> {
  try {
    const generic = await fetchGenericHealth();
    return adaptHealth(generic);
  } catch (err) {
    if (!isGenericEndpointUnavailable(err)) throw err;
  }
  return fetchJSON<HealthResponse>("/dashboard/api/health");
}
