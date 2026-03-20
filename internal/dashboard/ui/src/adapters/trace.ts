import type { EventRecord } from "../types/runtime.ts";
import type { TraceRecord } from "../types/portfolio.ts";

function trimString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

export function adaptTrace(events: EventRecord[]): TraceRecord[] {
  return events.map((event) => ({
    id: event.id,
    kind: "event",
    event_type: trimString(event.type),
    created_at: trimString(event.created_at),
    vertical_id: trimString(event.vertical_id),
    vertical_slug: trimString(event.vertical_slug),
    source_agent: trimString(event.source_agent),
    pending_count: Number(event.pending_count || 0),
    error_count: Number(event.error_count || 0),
    dead_count: Number(event.dead_count || 0),
    ...event,
  }));
}
