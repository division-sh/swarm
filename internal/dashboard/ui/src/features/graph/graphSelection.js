function compactScalar(value) {
  if (value == null || value === "") return "-";
  if (Array.isArray(value)) {
    if (value.length === 0) return "-";
    return value.every((item) => item == null || ["string", "number", "boolean"].includes(typeof item))
      ? value.join(", ")
      : `${value.length} items`;
  }
  if (typeof value === "object") {
    const keys = Object.keys(value);
    return keys.length ? keys.join(", ") : "-";
  }
  return String(value);
}

function stableArrayToken(items) {
  if (!Array.isArray(items) || items.length === 0) return "";
  return items.map((item) => String(item || "").trim()).filter(Boolean).sort().join(",");
}

function stableObjectToken(value) {
  if (!value || typeof value !== "object") return "";
  return Object.keys(value).sort().map((key) => `${key}:${compactScalar(value[key])}`).join(",");
}

export function edgeSelectionBase(edge) {
  if (!edge || typeof edge !== "object") return "";
  if (edge.id) return `id:${edge.id}`;
  return [
    `kind:${edge.kind || ""}`,
    `from:${edge.from || ""}`,
    `to:${edge.to || ""}`,
    `event:${edge.event_type || edge.label || ""}`,
    `source:${edge.source || ""}`,
    `status:${edge.status || ""}`,
    `transitions:${stableArrayToken(edge.transition_ids)}`,
    `timers:${stableArrayToken(edge.timer_ids)}`,
    `required:${stableArrayToken(edge.schema_required)}`,
    `props:${stableArrayToken(edge.schema_properties)}`,
    `meta:${stableObjectToken(edge.constraints)}`,
  ].join("|");
}

export function edgeSelectionID(edge, edges) {
  if (!edge) return "";
  const base = edgeSelectionBase(edge);
  let occurrence = 0;
  for (const candidate of edges || []) {
    if (edgeSelectionBase(candidate) !== base) continue;
    if (candidate === edge) break;
    occurrence += 1;
  }
  return occurrence > 0 ? `edge:${base}#${occurrence}` : `edge:${base}`;
}

export function findEdgeBySelectionID(edges, selectedID) {
  if (!selectedID) return null;
  return (edges || []).find((edge) => edgeSelectionID(edge, edges) === selectedID) || null;
}
