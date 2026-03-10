import { useMemo } from "react";
import type { TaskRecord } from "../../types/core.ts";

type SelectedTaskInput = {
  tasks: TaskRecord[];
  selectedTaskID: string;
};

export function useSelectedTask({ tasks, selectedTaskID }: SelectedTaskInput) {
  return useMemo(
    () => (tasks || []).find((task) => task.id === selectedTaskID) || null,
    [selectedTaskID, tasks],
  );
}
