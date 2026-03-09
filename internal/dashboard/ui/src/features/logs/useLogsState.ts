import { useEffect, useMemo } from "react";

function hasRuntimeError(item) {
  if (!item || typeof item !== "object") return false;
  if ((item.error_code || "").trim() !== "") return true;
  const level = (item.level || "").toLowerCase();
  if (level === "error") return true;
  return (item.error || "").trim() !== "";
}

export function useLogsState({ logsData, logsRuntimeErrorsOnly, selectedLogID, setSelectedLogID }) {
  const filteredLogsData = useMemo(
    () => (logsRuntimeErrorsOnly ? (logsData || []).filter(hasRuntimeError) : (logsData || [])),
    [logsData, logsRuntimeErrorsOnly],
  );

  useEffect(() => {
    if (!selectedLogID) return;
    const exists = (filteredLogsData || []).some((log) => log.id === selectedLogID);
    if (!exists) setSelectedLogID(null);
  }, [filteredLogsData, selectedLogID, setSelectedLogID]);

  const selectedLog = useMemo(
    () => (filteredLogsData || []).find((log) => log.id === selectedLogID) || null,
    [filteredLogsData, selectedLogID],
  );

  return { filteredLogsData, selectedLog };
}
