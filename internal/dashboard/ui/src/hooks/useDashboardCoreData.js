import { useCallback } from "react";
import { fetchJSON } from "../api/client.js";
import { fetchHealth } from "../api/health.js";

export function useDashboardCoreData({
  setOverview,
  setStatusText,
  relTime,
  taskStatus,
  setTasksResp,
  mailStatus,
  setMailbox,
  setDigestResp,
  setHealth,
  setTargets,
  setControlTarget,
}) {
  const loadOverview = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/overview");
    setOverview(d || {});
    setStatusText(`Updated ${relTime(d.generated_at)}`);
  }, [relTime, setOverview, setStatusText]);

  const loadTasks = useCallback(async () => {
    const p = new URLSearchParams();
    p.set("status", taskStatus || "open");
    p.set("limit", "250");
    const d = await fetchJSON(`/api/tasks?${p.toString()}`);
    setTasksResp({ tasks: d.tasks || [], weekly_budget: d.weekly_budget || {} });
  }, [setTasksResp, taskStatus]);

  const loadMailbox = useCallback(async () => {
    const d = await fetchJSON(`/api/mailbox?status=${encodeURIComponent(mailStatus)}&limit=150`);
    setMailbox({ summary: d.summary || {}, items: d.items || [] });
  }, [mailStatus, setMailbox]);

  const loadDigest = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/digest?top=10");
    setDigestResp(d || null);
  }, [setDigestResp]);

  const loadHealth = useCallback(async () => {
    setHealth((await fetchHealth()) || {});
  }, [setHealth]);

  const loadTargets = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/control/targets");
    const items = d.targets || [];
    setTargets(items);
    if (items.length > 0) {
      setControlTarget((cur) => cur || items[0].agent_id);
    }
  }, [setControlTarget, setTargets]);

  return {
    loadOverview,
    loadTasks,
    loadMailbox,
    loadDigest,
    loadHealth,
    loadTargets,
  };
}
