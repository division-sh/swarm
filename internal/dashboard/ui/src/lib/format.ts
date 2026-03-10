export function fmtTime(v: string | number | Date | null | undefined): string {
  if (!v) return "-";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleString();
}

export function relTime(v: string | number | Date | null | undefined): string {
  if (!v) return "-";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "-";
  const sec = Math.floor((Date.now() - d.getTime()) / 1000);
  if (sec < 0) return "just now";
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

export function formatDollars(cents: number | null | undefined): string {
  if (!cents && cents !== 0) return "$0.00";
  return "$" + (cents / 100).toFixed(2);
}

export function formatDurationMs(ms: number | string | null | undefined): string {
  const n = Number(ms || 0);
  if (!Number.isFinite(n) || n <= 0) return "-";
  if (n < 1000) return `${Math.round(n)}ms`;
  const sec = n / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const min = Math.floor(sec / 60);
  const rem = Math.round(sec % 60);
  return `${min}m ${rem}s`;
}

export function readPath(obj: Record<string, unknown> | null | undefined, path: string[]): string {
  let cur: unknown = obj;
  for (const key of path) {
    if (!cur || typeof cur !== "object" || !(key in cur)) return "";
    cur = (cur as Record<string, unknown>)[key];
  }
  if (typeof cur === "string") return cur.trim();
  if (typeof cur === "number" || typeof cur === "boolean") return String(cur);
  return "";
}

export function firstNonEmptyText(values: unknown[] | null | undefined): string {
  for (const v of values || []) {
    const s = typeof v === "string" ? v.trim() : "";
    if (s) return s;
  }
  return "";
}

export function shardScopeSummary(scope: Record<string, unknown> | null | undefined): string {
  if (!scope || typeof scope !== "object") return "-";
  const tax = Array.isArray(scope.taxonomy_categories) ? scope.taxonomy_categories : [];
  if (tax.length > 0) return tax.join(", ");
  const trends = Array.isArray(scope.trend_categories) ? scope.trend_categories : [];
  if (trends.length > 0) return trends.join(", ");
  return "-";
}
