import { useCallback, useEffect, useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import type { AgentRecord } from "../../types/core.ts";
import type { ConversationDetail } from "../../types/runtime.ts";
import {
  fetchAgentPrompt,
  fetchAgentPromptDiff,
  replayAgentRuntime,
  restartAgentRuntime,
  revertAgentPromptOverride,
  saveAgentPromptOverride,
  sendAgentChat,
  sendAgentDirective,
} from "../../api/dashboardAgentConsole.ts";
import { dashboardQueryKeys } from "../../app/dashboardQueryKeys.ts";
import { fetchConversationDetail } from "../../api/dashboardRuntime.ts";

type ToastFn = (message: string, type?: string) => void;
type AgentConsoleInput = {
  agent: AgentRecord;
  addToast: ToastFn;
  onAction?: () => void;
};

type BusyState = "" | "chat" | "directive" | "restart" | "replay" | "save-prompt" | "revert-prompt";

export function useAgentConsole({ agent, addToast, onAction }: AgentConsoleInput) {
  const queryClient = useQueryClient();
  const [chatMode, setChatMode] = useState("live");
  const [chatMessage, setChatMessage] = useState("");
  const [directiveMessage, setDirectiveMessage] = useState("");
  const [quickGeography, setQuickGeography] = useState("US");
  const [quickUseCorpus, setQuickUseCorpus] = useState(true);
  const [quickMode, setQuickMode] = useState("saas_gap");
  const [quickCorpusPath, setQuickCorpusPath] = useState("/data/test-signals-25.jsonl");
  const [busy, setBusy] = useState<BusyState>("");
  const [promptEdit, setPromptEdit] = useState("");
  const [promptNotes, setPromptNotes] = useState("");
  const [showDiff, setShowDiff] = useState(false);
  const [editingPrompt, setEditingPrompt] = useState(false);

  const conversationQuery = useQuery({
    queryKey: dashboardQueryKeys.conversationDetail(agent.id),
    queryFn: () => fetchConversationDetail(agent.id),
  });
  const promptQuery = useQuery({
    queryKey: dashboardQueryKeys.agentPrompt(agent.id),
    queryFn: () => fetchAgentPrompt(agent.id),
  });
  const promptDiffQuery = useQuery({
    queryKey: dashboardQueryKeys.agentPromptDiff(agent.id),
    queryFn: () => fetchAgentPromptDiff(agent.id),
    enabled: false,
  });
  const promptData = (promptQuery.data || null) as Record<string, any> | null;
  const promptDiffData = (promptDiffQuery.data || null) as Record<string, any> | null;

  useEffect(() => {
    if (!promptData || editingPrompt) return;
    setPromptEdit(promptData.effective_prompt || "");
  }, [editingPrompt, promptData]);

  const run = useCallback(async <T>(key: BusyState, fn: () => Promise<T>) => {
    setBusy(key);
    try {
      const out = await fn();
      addToast((out as Record<string, any>)?.message || "Done", "success");
      if (onAction) onAction();
      return out;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      addToast(message, "error");
      throw err;
    } finally {
      setBusy("");
    }
  }, [addToast, onAction]);

  const refreshAgentConsole = useCallback(async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.agentPrompt(agent.id) }),
      queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.conversationDetail(agent.id) }),
      queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.conversations() }),
      queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.agents() }),
      queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.targets() }),
    ]);
  }, [agent.id, queryClient]);

  const openPromptDiff = useCallback(() => {
    promptDiffQuery.refetch()
      .then((result) => {
        if (result.error) throw result.error;
        setShowDiff(true);
      })
      .catch((err) => addToast(err instanceof Error ? err.message : String(err), "error"));
  }, [addToast, promptDiffQuery]);

  const togglePromptEdit = useCallback(() => {
    setPromptEdit(promptData?.effective_prompt || "");
    setPromptNotes("");
    setEditingPrompt((value) => !value);
  }, [promptData]);

  const savePromptOverride = useCallback(() => run("save-prompt", async () => {
    const out = await saveAgentPromptOverride(agent.id, promptEdit, promptNotes || undefined);
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.agentPrompt(agent.id) }),
      queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.agentPromptDiff(agent.id) }),
    ]);
    setEditingPrompt(false);
    return out;
  }), [agent.id, promptEdit, promptNotes, queryClient, run]);

  const revertPromptOverride = useCallback(() => run("revert-prompt", async () => {
    const out = await revertAgentPromptOverride(agent.id);
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.agentPrompt(agent.id) }),
      queryClient.invalidateQueries({ queryKey: dashboardQueryKeys.agentPromptDiff(agent.id) }),
    ]);
    setEditingPrompt(false);
    return out;
  }), [agent.id, queryClient, run]);

  const sendChat = useCallback(() => {
    const message = chatMessage.trim();
    if (!message) return Promise.resolve(null);
    return run("chat", async () => {
      const out = await sendAgentChat(agent.id, chatMode, message);
      setChatMessage("");
      await refreshAgentConsole();
      return out;
    });
  }, [agent.id, chatMessage, chatMode, refreshAgentConsole, run]);

  const sendDirective = useCallback(() => {
    const message = directiveMessage.trim();
    if (!message) return Promise.resolve(null);
    return run("directive", async () => {
      const out = await sendAgentDirective(agent.id, message);
      setDirectiveMessage("");
      await refreshAgentConsole();
      return out;
    });
  }, [agent.id, directiveMessage, refreshAgentConsole, run]);

  const restartAgent = useCallback(() => run("restart", async () => {
    const out = await restartAgentRuntime(agent.id);
    await refreshAgentConsole();
    return out;
  }), [agent.id, refreshAgentConsole, run]);
  const replayAgent = useCallback(() => run("replay", async () => {
    const out = await replayAgentRuntime(agent.id);
    await refreshAgentConsole();
    return out;
  }), [agent.id, refreshAgentConsole, run]);

  const quickDirective = useMemo(() => {
    const geography = (quickGeography || "").trim() || "US";
    if (quickUseCorpus) {
      const corpusPath = (quickCorpusPath || "").trim() || "/data/test-signals-25.jsonl";
      return `run corpus in ${geography}, corpus_path=${corpusPath}`;
    }
    const mode = (quickMode || "").trim() || "saas_gap";
    return `run ${mode} in ${geography}`;
  }, [quickCorpusPath, quickGeography, quickMode, quickUseCorpus]);

  return {
    chat: {
      mode: chatMode,
      setMode: setChatMode,
      message: chatMessage,
      setMessage: setChatMessage,
      send: sendChat,
    },
    directive: {
      message: directiveMessage,
      setMessage: setDirectiveMessage,
      send: sendDirective,
      restart: restartAgent,
      replay: replayAgent,
    },
    prompt: {
      state: promptData,
      edit: promptEdit,
      setEdit: setPromptEdit,
      notes: promptNotes,
      setNotes: setPromptNotes,
      editing: editingPrompt,
      setEditing: setEditingPrompt,
      diffOpen: showDiff,
      setDiffOpen: setShowDiff,
      diffData: promptDiffData,
      openDiff: openPromptDiff,
      toggleEdit: togglePromptEdit,
      saveOverride: savePromptOverride,
      revertOverride: revertPromptOverride,
    },
    quickDirective: {
      enabled: (agent.id || "").trim() === "empire-coordinator",
      geography: quickGeography,
      setGeography: setQuickGeography,
      useCorpus: quickUseCorpus,
      setUseCorpus: setQuickUseCorpus,
      mode: quickMode,
      setMode: setQuickMode,
      corpusPath: quickCorpusPath,
      setCorpusPath: setQuickCorpusPath,
      value: quickDirective,
      options: ["US", "Argentina", "Brazil", "Mexico", "Chile", "Peru", "Paraguay", "Uruguay", "Colombia"],
      datalistID: `geo-options-${(agent.id || "agent").replace(/[^a-zA-Z0-9_-]/g, "-")}`,
    },
    conversation: (conversationQuery.data || { messages: [], turns: [] }) as ConversationDetail,
    busy,
  };
}
