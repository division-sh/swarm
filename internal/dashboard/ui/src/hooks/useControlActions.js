import { useCallback } from "react";
import { postJSON } from "../api/client.ts";

export function useControlActions({
  addToast,
  runControl,
  loadAgents,
  loadMailbox,
}) {
  const quickMailboxDecide = useCallback(async (id, action) => {
    try {
      await postJSON(`/api/mailbox/${encodeURIComponent(id)}/decide`, { action, notes: "" });
      addToast(`${action}: ${(id || "").slice(0, 8)}`, "success");
      await loadMailbox();
    } catch (err) {
      addToast(err.message, "error");
    }
  }, [addToast, loadMailbox]);

  const restartAgent = useCallback(async (agentID) => {
    await postJSON("/dashboard/api/control/agents/restart", { agent_id: agentID });
    addToast(`Restarted ${agentID}`, "success");
    await loadAgents();
  }, [addToast, loadAgents]);

  const sendDirective = useCallback(async (agentID, message) => {
    await runControl(() => postJSON("/dashboard/api/control/directive", { agent_id: agentID, message: (message || "").trim() }));
  }, [runControl]);

  const sendChat = useCallback(async (agentID, mode, message) => {
    await runControl(() => postJSON("/dashboard/api/control/chat", { agent_id: agentID, mode, message: (message || "").trim() }));
  }, [runControl]);

  const restartControlTarget = useCallback(async (agentID) => {
    await runControl(() => postJSON("/dashboard/api/control/agents/restart", { agent_id: agentID }));
  }, [runControl]);

  const replayControlTarget = useCallback(async (agentID) => {
    await runControl(() => postJSON("/dashboard/api/control/agents/replay", { agent_id: agentID }));
  }, [runControl]);

  const createVertical = useCallback(async (payload) => {
    await runControl(() => postJSON("/dashboard/api/control/verticals/create", {
      name: (payload && payload.name ? payload.name : "").trim(),
      geography: (payload && payload.geography ? payload.geography : "").trim(),
      slug: (payload && payload.slug ? payload.slug : "").trim() || undefined,
    }));
  }, [runControl]);

  const requeueEvent = useCallback(async (payload) => {
    await runControl(() => postJSON("/dashboard/api/control/events/requeue", {
      event_id: (payload && payload.eventID ? payload.eventID : "").trim(),
      agent_id: payload && payload.agentID ? payload.agentID : undefined,
    }));
  }, [runControl]);

  const seedOrg = useCallback(async () => {
    await runControl(() => postJSON("/dashboard/api/control/seed-org", {}));
  }, [runControl]);

  const pauseRuntime = useCallback(async () => {
    await runControl(() => postJSON("/dashboard/api/control/runtime", { action: "pause" }));
  }, [runControl]);

  const resumeRuntime = useCallback(async () => {
    await runControl(() => postJSON("/dashboard/api/control/runtime", { action: "resume" }));
  }, [runControl]);

  const resetDBAndSeed = useCallback(async (confirmText, clearConfirm) => {
    await runControl(async () => {
      const out = await postJSON("/dashboard/api/control/runtime", { action: "reset_db", confirm: (confirmText || "").trim(), seed_org: true });
      clearConfirm("");
      return out;
    });
  }, [runControl]);

  const wipeDB = useCallback(async (confirmText, clearConfirm) => {
    await runControl(async () => {
      const out = await postJSON("/dashboard/api/control/runtime", { action: "reset_state", confirm: (confirmText || "").trim() });
      clearConfirm("");
      return out;
    });
  }, [runControl]);

  const decideMailbox = useCallback(async (mailboxID, action, notes) => {
    await runControl(() => postJSON(`/api/mailbox/${encodeURIComponent((mailboxID || "").trim())}/decide`, { action, notes: (notes || "").trim() }));
  }, [runControl]);

  return {
    quickMailboxDecide,
    restartAgent,
    sendDirective,
    sendChat,
    restartControlTarget,
    replayControlTarget,
    createVertical,
    requeueEvent,
    seedOrg,
    pauseRuntime,
    resumeRuntime,
    resetDBAndSeed,
    wipeDB,
    decideMailbox,
  };
}
