import { useEffect } from "react";
import type { FlowEventRecord } from "../types/workflow.ts";

type FlowRuntimeStreamInput = {
  activeView: string;
  activeSubview: string;
  flowView: string;
  flowVertical: string;
  getKey: () => string;
  patchFlowEvent: (item: FlowEventRecord) => void;
};

export function useFlowRuntimeStream({ activeView, activeSubview, flowView, flowVertical, getKey, patchFlowEvent }: FlowRuntimeStreamInput) {
  useEffect(() => {
    const showingRuntimeFlow = ((activeView === "workflow" && (activeSubview || "flow") === "flow") || activeView === "flow")
      && flowView === "runtime";
    if (!showingRuntimeFlow) return undefined;
    let stream: EventSource | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;
    let retryCount = 0;

    function connect() {
      const p = new URLSearchParams();
      p.set("stream", "true");
      p.set("limit", "200");
      if (flowVertical) p.set("entity_id", flowVertical);
      const key = getKey();
      if (key) p.set("key", key);
      stream = new EventSource(`/api/events/flow?${p.toString()}`);
      stream.addEventListener("flow", (ev) => {
        retryCount = 0;
        try {
          const item = JSON.parse((ev as MessageEvent).data || "{}") as FlowEventRecord & { payload?: Record<string, unknown> };
          if (!item || !item.event_id) return;
          const payload = item.payload && typeof item.payload === "object" ? item.payload : {};
          const entityID = typeof item.entity_id === "string" ? item.entity_id.trim() : "";
          if (!item.vertical_id) item.vertical_id = entityID || (typeof payload.entity_id === "string" ? payload.entity_id.trim() : "");
          if (!item.vertical_slug) {
            const payloadSlug = typeof payload.vertical_slug === "string" ? payload.vertical_slug.trim() : "";
            const payloadVertical = typeof payload.vertical === "string" ? payload.vertical.trim() : "";
            item.vertical_slug = payloadSlug || payloadVertical || item.vertical_id;
          }
          patchFlowEvent(item);
        } catch {}
      });
      stream.addEventListener("open", () => { retryCount = 0; });
      stream.onerror = () => {
        stream?.close();
        retryCount++;
        const delay = Math.min(5000 * retryCount, 30000);
        retryTimer = setTimeout(connect, delay);
      };
    }

    connect();
    return () => {
      stream?.close();
      if (retryTimer) clearTimeout(retryTimer);
    };
  }, [activeSubview, activeView, flowView, flowVertical, getKey, patchFlowEvent]);
}
