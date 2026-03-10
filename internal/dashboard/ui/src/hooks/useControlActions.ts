import { useCallback } from "react";
import { postJSON } from "../api/client.ts";
import type { ControlResult } from "../types/core.ts";

type CreateVerticalPayload = {
  name?: string;
  geography?: string;
  slug?: string;
};

type RequeueEventPayload = {
  eventID?: string;
  agentID?: string;
};

type ControlActionsInput = {
  addToast: (message: string, type?: string) => void;
  runControl: (fn: () => Promise<ControlResult>) => Promise<ControlResult>;
  loadAgents: () => Promise<unknown>;
  loadMailbox: () => Promise<unknown>;
};

export function useControlActions({
  addToast,
  runControl,
  loadAgents,
  loadMailbox,
}: ControlActionsInput) {
  const quickMailboxDecide = useCallback(async (id: string, action: string) => {
    try {
      await postJSON(`/api/mailbox/${encodeURIComponent(id)}/decide`, { action, notes: "" });
      addToast(`${action}: ${(id || "").slice(0, 8)}`, "success");
      await loadMailbox();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      addToast(message, "error");
    }
  }, [addToast, loadMailbox]);

  const restartAgent = useCallback(async (agentID: string) => {
    await postJSON("/dashboard/api/control/agents/restart", { agent_id: agentID });
    addToast(`Restarted ${agentID}`, "success");
    await loadAgents();
  }, [addToast, loadAgents]);

  const sendDirective = useCallback(async (agentID: string, message: string) => {
    await runControl(() => postJSON("/dashboard/api/control/directive", { agent_id: agentID, message: (message || "").trim() }));
  }, [runControl]);

  const sendChat = useCallback(async (agentID: string, mode: string, message: string) => {
    await runControl(() => postJSON("/dashboard/api/control/chat", { agent_id: agentID, mode, message: (message || "").trim() }));
  }, [runControl]);

  const restartControlTarget = useCallback(async (agentID: string) => {
    await runControl(() => postJSON("/dashboard/api/control/agents/restart", { agent_id: agentID }));
  }, [runControl]);

  const replayControlTarget = useCallback(async (agentID: string) => {
    await runControl(() => postJSON("/dashboard/api/control/agents/replay", { agent_id: agentID }));
  }, [runControl]);

  const createVertical = useCallback(async (payload: CreateVerticalPayload) => {
    await runControl(() => postJSON("/dashboard/api/control/verticals/create", {
      name: (payload && payload.name ? payload.name : "").trim(),
      geography: (payload && payload.geography ? payload.geography : "").trim(),
      slug: (payload && payload.slug ? payload.slug : "").trim() || undefined,
    }));
  }, [runControl]);

  const requeueEvent = useCallback(async (payload: RequeueEventPayload) => {
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

  const resetDBAndSeed = useCallback(async (confirmText: string, clearConfirm: (value: string) => void) => {
    await runControl(async () => {
      const out = await postJSON<ControlResult>("/dashboard/api/control/runtime", { action: "reset_db", confirm: (confirmText || "").trim(), seed_org: true });
      clearConfirm("");
      return out;
    });
  }, [runControl]);

  const wipeDB = useCallback(async (confirmText: string, clearConfirm: (value: string) => void) => {
    await runControl(async () => {
      const out = await postJSON<ControlResult>("/dashboard/api/control/runtime", { action: "reset_state", confirm: (confirmText || "").trim() });
      clearConfirm("");
      return out;
    });
  }, [runControl]);

  const decideMailbox = useCallback(async (mailboxID: string, action: string, notes: string) => {
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
