import { useCallback } from "react";

type ActionRunnerInput = {
  addToast: (message: string, type?: string) => void;
  setControlOutput: (value: Record<string, any>) => void;
  refreshAfterControl: () => Promise<unknown>;
};

export function useActionRunner({
  addToast,
  setControlOutput,
  refreshAfterControl,
}: ActionRunnerInput) {
  const runControl = useCallback(async (fn: () => Promise<Record<string, any>>) => {
    try {
      const out = await fn();
      setControlOutput(out);
      addToast(out.message || "Action completed", "success");
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
