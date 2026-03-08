import { useCallback } from "react";

export function useActionRunner({
  addToast,
  setControlOutput,
  refreshAfterControl,
}) {
  const runControl = useCallback(async (fn) => {
    try {
      const out = await fn();
      setControlOutput(out);
      addToast(out.message || "Action completed", "success");
      await refreshAfterControl();
      return out;
    } catch (err) {
      setControlOutput({ error: err.message });
      addToast(err.message, "error");
      throw err;
    }
  }, [addToast, refreshAfterControl, setControlOutput]);

  return { runControl };
}
