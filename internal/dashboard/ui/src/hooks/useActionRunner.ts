import { useCallback } from "react";
import type { ControlResult } from "../types/core.ts";

type ActionRunnerInput = {
  addToast: (message: string, type?: string) => void;
  setControlOutput: (value: ControlResult) => void;
  refreshAfterControl: () => Promise<unknown>;
};

export function useActionRunner({
  addToast,
  setControlOutput,
  refreshAfterControl,
}: ActionRunnerInput) {
  const runControl = useCallback(async (fn: () => Promise<ControlResult>) => {
    try {
      const out = await fn();
      setControlOutput(out);
      addToast(String(out.message || "Action completed"), "success");
      await refreshAfterControl();
      return out;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setControlOutput({ error: message });
      addToast(message, "error");
      throw err;
    }
  }, [addToast, refreshAfterControl, setControlOutput]);

  return { runControl };
}
