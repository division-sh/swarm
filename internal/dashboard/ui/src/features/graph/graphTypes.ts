export function isEventLinkedEdgeKind(kind) {
  return kind === "routing" || kind === "subscription" || kind === "producer";
}

export function roleKeyFromAgentID(id, role) {
  if (role) return role;
  const s = (id || "").trim();
  const idx = s.lastIndexOf("-");
  if (idx > 0) return s.slice(0, idx);
  return s;
}
