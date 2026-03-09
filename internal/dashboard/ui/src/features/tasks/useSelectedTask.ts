import { useMemo } from "react";

type SelectedTaskInput = {
  tasks: Record<string, any>[];
  selectedTaskID: string;
};

export function useSelectedTask({ tasks, selectedTaskID }: SelectedTaskInput) {
  return useMemo(
    () => (tasks || []).find((task) => task.id === selectedTaskID) || null,
    [selectedTaskID, tasks],
  );
}
