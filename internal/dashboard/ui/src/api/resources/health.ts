import { fetchJSON } from "../client.ts";
import type { GenericHealthResponse } from "../../types/server.ts";

export async function fetchGenericHealth(): Promise<GenericHealthResponse> {
  return fetchJSON<GenericHealthResponse>("/api/health");
}
