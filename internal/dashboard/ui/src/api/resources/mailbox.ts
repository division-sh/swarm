import { fetchJSON } from "../client.ts";
import type { GenericMailboxItem } from "../../types/server.ts";

export async function fetchGenericMailbox(status = "all", limit = 150): Promise<GenericMailboxItem[]> {
  const d = await fetchJSON<{ items?: GenericMailboxItem[] }>(`/api/mailbox?status=${encodeURIComponent(status)}&limit=${encodeURIComponent(limit)}`);
  return d.items || [];
}
