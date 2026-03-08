import { useMemo } from "react";

export function useSelectedTask({ tasks, selectedTaskID }) {
  return useMemo(
    () => (tasks || []).find((task) => task.id === selectedTaskID) || null,
    [selectedTaskID, tasks],
  );
}
