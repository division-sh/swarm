import type { MailboxItem, MailboxResponse, MailboxSummary } from "../types/core.ts";
import type { GenericMailboxItem } from "../types/server.ts";

function normalizeStatus(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function normalizePriority(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

export function adaptMailboxItems(items: GenericMailboxItem[]): MailboxItem[] {
  return items.map((item) => ({ ...item }));
}

export function summarizeMailbox(items: MailboxItem[]): MailboxSummary {
  const summary: MailboxSummary = {
    pending: 0,
    approved: 0,
    rejected: 0,
    deferred: 0,
    decided: 0,
  };
  for (const item of items) {
    const status = normalizeStatus(item.status);
    if (status === "pending") summary.pending = Number(summary.pending || 0) + 1;
    if (status === "approved") summary.approved = Number(summary.approved || 0) + 1;
    if (status === "rejected") summary.rejected = Number(summary.rejected || 0) + 1;
    if (status === "deferred") summary.deferred = Number(summary.deferred || 0) + 1;
    if (status === "approved" || status === "rejected" || status === "deferred") {
      summary.decided = Number(summary.decided || 0) + 1;
    }
  }
  return summary;
}

export function adaptMailbox(items: GenericMailboxItem[]): MailboxResponse {
  const normalized = adaptMailboxItems(items).sort((a, b) => {
    const statusA = normalizeStatus(a.status);
    const statusB = normalizeStatus(b.status);
    if (statusA != statusB) return statusA.localeCompare(statusB);
    const priorityA = normalizePriority(a.priority);
    const priorityB = normalizePriority(b.priority);
    if (priorityA !== priorityB) return priorityB.localeCompare(priorityA);
    return String(b.created_at || "").localeCompare(String(a.created_at || ""));
  });
  return {
    summary: summarizeMailbox(normalized),
    items: normalized,
  };
}
