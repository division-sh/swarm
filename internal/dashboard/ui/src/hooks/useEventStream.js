import { useEffect } from "react";

export function useEventStream({ eventsFilter, eventsIncludeRuntime, eventsRuntimeErrorsOnly, getKey, loadEvents, loadRuntimeLogs, addToast }) {
  useEffect(() => {
    let stream = null;
    let retryTimer = null;
    let retryCount = 0;

    function connect() {
      const p = new URLSearchParams();
      if (eventsFilter.type) p.set("type", eventsFilter.type);
      if (eventsFilter.source) p.set("source", eventsFilter.source);
      if (eventsFilter.vertical) p.set("vertical", eventsFilter.vertical);
      if (eventsFilter.component) p.set("component", eventsFilter.component);
      if (eventsFilter.level) p.set("level", eventsFilter.level);
      else if (eventsRuntimeErrorsOnly) p.set("level", "error");
      if (eventsFilter.subscriber) p.set("subscriber", eventsFilter.subscriber);
      p.set("include_runtime", eventsIncludeRuntime ? "true" : "false");
      const key = getKey();
      if (key) p.set("key", key);
      stream = new EventSource(`/api/events?stream=true&${p.toString()}`);
      stream.addEventListener("event", () => {
        retryCount = 0;
        loadEvents().catch(() => {});
      });
      stream.addEventListener("runtime_log", () => {
        retryCount = 0;
        loadRuntimeLogs().catch(() => {});
      });
      stream.addEventListener("open", () => { retryCount = 0; });
      stream.onerror = () => {
        stream.close();
        retryCount++;
        const delay = Math.min(5000 * retryCount, 30000);
        addToast(`Event stream disconnected, reconnecting in ${Math.round(delay / 1000)}s…`, "error");
        retryTimer = setTimeout(connect, delay);
      };
    }

    connect();
    return () => {
      if (stream) stream.close();
      if (retryTimer) clearTimeout(retryTimer);
    };
  }, [eventsFilter, eventsIncludeRuntime, eventsRuntimeErrorsOnly, getKey, loadEvents, loadRuntimeLogs, addToast]);
}
