import { fetchJSON } from "./client.js";

export async function fetchHealth() {
  return fetchJSON("/dashboard/api/health");
}
