import { useEffect } from "react";

export function useReplayTicker({ flowView, flowReplayOn, flowReplaySpeed, flowEvents, setFlowReplayIndex, setFlowReplayOn }) {
  useEffect(() => {
    if (flowView !== "replay" || !flowReplayOn) return undefined;
    const step = flowReplaySpeed >= 100 ? 10 : flowReplaySpeed >= 50 ? 5 : 1;
    const t = setInterval(() => {
      setFlowReplayIndex((idx) => {
        const next = Math.min((flowEvents || []).length, idx + step);
        if (next >= (flowEvents || []).length) setFlowReplayOn(false);
        return next;
      });
    }, 280);
    return () => clearInterval(t);
  }, [flowView, flowReplayOn, flowReplaySpeed, flowEvents, setFlowReplayIndex, setFlowReplayOn]);
}
