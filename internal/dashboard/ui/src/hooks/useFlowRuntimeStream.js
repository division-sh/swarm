import { useEffect } from "react";

export function useFlowRuntimeStream({ activeView, flowView, flowVertical, getKey, setFlowEvents }) {
  useEffect(() => {
    if (activeView !== "flow" || flowView !== "runtime") return undefined;
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
          setFlowEvents((prev) => {
            const rows = [item, ...(prev || []).filter((x) => x.event_id !== item.event_id)];
            return rows.slice(0, 500);
          });
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
  }, [activeView, flowView, flowVertical, getKey, setFlowEvents]);
}
