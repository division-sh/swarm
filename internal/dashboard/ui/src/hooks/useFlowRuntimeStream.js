import { useEffect } from "react";

export function useFlowRuntimeStream({ activeView, activeSubview, flowView, flowVertical, getKey, patchFlowEvent }) {
  useEffect(() => {
    const showingRuntimeFlow = ((activeView === "workflow" && (activeSubview || "flow") === "flow") || activeView === "flow")
      && flowView === "runtime";
    if (!showingRuntimeFlow) return undefined;
    let stream = null;
    let retryTimer = null;
    let retryCount = 0;

    function connect() {
      const p = new URLSearchParams();
      p.set("stream", "true");
      p.set("limit", "200");
      if (flowVertical) p.set("vertical", flowVertical);
      const key = getKey();
      if (key) p.set("key", key);
      stream = new EventSource(`/api/events/flow?${p.toString()}`);
      stream.addEventListener("flow", (ev) => {
        retryCount = 0;
        try {
          const item = JSON.parse(ev.data || "{}");
          if (!item || !item.event_id) return;
          patchFlowEvent(item);
        } catch {}
      });
      stream.addEventListener("open", () => { retryCount = 0; });
      stream.onerror = () => {
        if (stream) stream.close();
        retryCount++;
        const delay = Math.min(5000 * retryCount, 30000);
        retryTimer = setTimeout(connect, delay);
      };
    }

    connect();
    return () => {
      if (stream) stream.close();
      if (retryTimer) clearTimeout(retryTimer);
    };
  }, [activeSubview, activeView, flowView, flowVertical, getKey, patchFlowEvent]);
}
