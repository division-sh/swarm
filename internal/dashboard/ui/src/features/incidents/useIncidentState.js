import { useMemo } from "react";

export function useIncidentState({ incidentsData, selectedIncidentCode }) {
  return useMemo(
    () => (incidentsData || []).find((item) => item.code === selectedIncidentCode) || null,
    [incidentsData, selectedIncidentCode],
  );
}
