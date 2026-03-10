import { fetchJSON } from "./client.ts";
import type { HealthResponse } from "../types/core.ts";

export async function fetchHealth(): Promise<HealthResponse> {
  return fetchJSON<HealthResponse>("/dashboard/api/health");
}
