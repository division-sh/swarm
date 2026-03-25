import { useEffect, useRef } from "react";
import type { EventFilter } from "../types/runtime.ts";

type EventStreamInput = {
  eventsFilter: EventFilter;
  eventsIncludeRuntime: boolean;
  eventsRuntimeErrorsOnly: boolean;
  getKey: () => string;
  loadEvents: () => Promise<unknown>;
  loadRuntimeLogs: () => Promise<unknown>;
  addToast: (message: string, type?: string) => void;
};

export function useEventStream({ eventsFilter, eventsIncludeRuntime, eventsRuntimeErrorsOnly, getKey, loadEvents, loadRuntimeLogs, addToast }: EventStreamInput) {
  const disconnectToastShownRef = useRef(false);
  const loadEventsRef = useRef(loadEvents);
  const loadRuntimeLogsRef = useRef(loadRuntimeLogs);
  const addToastRef = useRef(addToast);

  useEffect(() => {
    loadEventsRef.current = loadEvents;
  }, [loadEvents]);

  useEffect(() => {
    loadRuntimeLogsRef.current = loadRuntimeLogs;
  }, [loadRuntimeLogs]);

  useEffect(() => {
    addToastRef.current = addToast;
  }, [addToast]);

  useEffect(() => {
    let stream: EventSource | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;
    let retryCount = 0;
    let stopped = false;

    function connect() {
      if (stopped) return;
      const p = new URLSearchParams();
      if (eventsFilter.type) p.set("type", eventsFilter.type);
      if (eventsFilter.source) p.set("source", eventsFilter.source);
      if (eventsFilter.entity_id) p.set("entity_id", eventsFilter.entity_id);
      else if (eventsFilter.vertical) p.set("entity_id", eventsFilter.vertical);
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
        loadEventsRef.current().catch(() => {});
      });
      stream.addEventListener("runtime_log", () => {
        retryCount = 0;
        loadRuntimeLogsRef.current().catch(() => {});
      });
      stream.addEventListener("open", () => {
        retryCount = 0;
        disconnectToastShownRef.current = false;
      });
      stream.onerror = () => {
        stream?.close();
        stream = null;
        if (retryTimer) clearTimeout(retryTimer);
        retryCount++;
        const delay = Math.min(5000 * retryCount, 30000);
        if (!disconnectToastShownRef.current) {
          disconnectToastShownRef.current = true;
          addToastRef.current(`Event stream disconnected, reconnecting in ${Math.round(delay / 1000)}s…`, "error");
        }
        retryTimer = setTimeout(connect, delay);
      };
    }

    connect();
    return () => {
      stopped = true;
      stream?.close();
      if (retryTimer) clearTimeout(retryTimer);
    };
  }, [eventsFilter, eventsIncludeRuntime, eventsRuntimeErrorsOnly, getKey]);
}
