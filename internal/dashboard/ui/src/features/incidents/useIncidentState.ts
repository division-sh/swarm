import { useMemo } from "react";
import type { IncidentRecord } from "../../types/runtime.ts";

export function useIncidentState({
  incidentsData,
  selectedIncidentCode,
}: {
  incidentsData: IncidentRecord[];
  selectedIncidentCode: string;
}) {
  return useMemo(
    () => (incidentsData || []).find((item) => item.code === selectedIncidentCode) || null,
    [incidentsData, selectedIncidentCode],
  );
}
