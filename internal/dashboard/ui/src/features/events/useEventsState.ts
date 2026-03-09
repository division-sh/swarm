import { useMemo } from "react";

function hasRuntimeError(item) {
  if (!item || typeof item !== "object") return false;
  if ((item.error_code || "").trim() !== "") return true;
  const level = (item.level || "").toLowerCase();
  if (level === "error") return true;
  return (item.error || "").trim() !== "";
}

function hasEventError(item) {
  if (!item || typeof item !== "object") return false;
  return Number(item.error_count || 0) > 0 || Number(item.dead_count || 0) > 0;
}

export function useEventsState({ events, runtimeLogs, eventsRuntimeErrorsOnly }) {
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
