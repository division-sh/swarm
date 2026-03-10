import { useMemo } from "react";
import type { EventRecord, RuntimeLogRecord } from "../../types/runtime.ts";

function hasRuntimeError(item: RuntimeLogRecord | null | undefined) {
  if (!item || typeof item !== "object") return false;
  if ((item.error_code || "").trim() !== "") return true;
  const level = (item.level || "").toLowerCase();
  if (level === "error") return true;
  return (item.error || "").trim() !== "";
}

function hasEventError(item: EventRecord | null | undefined) {
  if (!item || typeof item !== "object") return false;
  return Number(item.error_count || 0) > 0 || Number(item.dead_count || 0) > 0;
}

export function useEventsState({
  events,
  runtimeLogs,
  eventsRuntimeErrorsOnly,
}: {
  events: EventRecord[];
  runtimeLogs: RuntimeLogRecord[];
  eventsRuntimeErrorsOnly: boolean;
}) {
  const filteredEvents = useMemo(
    () => (eventsRuntimeErrorsOnly ? (events || []).filter(hasEventError) : (events || [])),
    [events, eventsRuntimeErrorsOnly],
  );

  const filteredRuntimeLogs = useMemo(
    () => (eventsRuntimeErrorsOnly ? (runtimeLogs || []).filter(hasRuntimeError) : (runtimeLogs || [])),
    [eventsRuntimeErrorsOnly, runtimeLogs],
  );

  return { filteredEvents, filteredRuntimeLogs };
}
