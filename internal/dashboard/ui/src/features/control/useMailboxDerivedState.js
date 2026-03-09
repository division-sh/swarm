function normalizePriority(value) {
  const v = String(value || "").toLowerCase();
  if (v === "critical") return 4;
  if (v === "high") return 3;
  if (v === "medium") return 2;
  if (v === "low") return 1;
  return 0;
}

function mailboxSort(a, b) {
  const pri = normalizePriority(b.priority) - normalizePriority(a.priority);
  if (pri !== 0) return pri;
  return new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime();
}

export function deriveMailboxDerivedState({ mailbox, selectedMailboxItem }) {
  const items = Array.isArray(mailbox?.items) ? mailbox.items : [];
  const pending = items.filter((item) => String(item.status || "").toLowerCase() === "pending").sort(mailboxSort);
  const critical = pending.filter((item) => normalizePriority(item.priority) >= 4);
  const decided = items
    .filter((item) => String(item.status || "").toLowerCase() !== "pending")
    .sort((a, b) => new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime());
  const selected = items.find((item) => item.id === selectedMailboxItem) || null;

  return {
    summary: {
      loaded: items.length,
      pending: pending.length,
      critical: critical.length,
      decided: decided.length,
    },
    queue: {
      pending: pending.slice(0, 5),
      critical: critical.slice(0, 5),
      decided: decided.slice(0, 5),
    },
    selected,
  };
}
