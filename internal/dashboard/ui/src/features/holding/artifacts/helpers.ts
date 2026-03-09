export function artifactIsScalar(value) {
  return value == null || typeof value === "string" || typeof value === "number" || typeof value === "boolean";
}

export function prettyArtifactKey(key) {
  return String(key || "")
    .replace(/[_.]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

export function formatArtifactScalar(value) {
  if (value == null) return "-";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return String(value);
    return Number(value).toFixed(2);
  }
  const s = String(value).trim();
  return s || "-";
}

function truncateText(text, max = 280) {
  const s = String(text || "").trim();
  if (!s) return "";
  return s.length > max ? s.slice(0, max) + "\u2026" : s;
}

export function hasArtifactValue(value) {
  if (value == null) return false;
  if (typeof value === "string") return value.trim() !== "";
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "object") return Object.keys(value).length > 0;
  return true;
}

export function normalizeScoreRows(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) {
    return raw
      .map((item, index) => {
        if (!item || typeof item !== "object") return null;
        const record = item as Record<string, any>;
        const dimension = record.dimension || record.name || record.key || `Dimension ${index + 1}`;
        const score = record.score ?? record.resolved_score ?? record.value ?? record.points;
        const notes = record.evidence || record.reason || record.rationale || record.comment || "";
        return {
          dimension: String(dimension),
          score,
          notes: truncateText(notes, 220),
        };
      })
      .filter(Boolean);
  }
  if (typeof raw === "object") {
    return Object.entries(raw).map(([dimension, value]) => {
      if (artifactIsScalar(value)) {
        return { dimension: prettyArtifactKey(dimension), score: value, notes: "" };
      }
      if (value && typeof value === "object") {
        const record = value as Record<string, any>;
        return {
          dimension: prettyArtifactKey(dimension),
          score: record.score ?? record.resolved_score ?? record.value ?? "",
          notes: truncateText(record.evidence || record.reason || record.rationale || record.comment || "", 220),
        };
      }
      return { dimension: prettyArtifactKey(dimension), score: "", notes: "" };
    });
  }
  return [];
}
