import { useCallback, useEffect, useMemo, useState } from "react";
import { deleteJSON, fetchJSON, postJSON, putJSON } from "../../api/client.js";

export function useAgentConsole({ agent, addToast, onAction }) {
  const [chatMode, setChatMode] = useState("live");
  const [chatMessage, setChatMessage] = useState("");
  const [directiveMessage, setDirectiveMessage] = useState("");
  const [quickGeography, setQuickGeography] = useState("US");
  const [quickUseCorpus, setQuickUseCorpus] = useState(true);
  const [quickMode, setQuickMode] = useState("saas_gap");
  const [quickCorpusPath, setQuickCorpusPath] = useState("/data/test-signals-25.jsonl");
  const [turns, setTurns] = useState([]);
  const [busy, setBusy] = useState("");
  const [promptState, setPromptState] = useState(null);
  const [promptEdit, setPromptEdit] = useState("");
  const [promptNotes, setPromptNotes] = useState("");
  const [showDiff, setShowDiff] = useState(false);
  const [diffData, setDiffData] = useState(null);
  const [editingPrompt, setEditingPrompt] = useState(false);

  const loadTurns = useCallback(async () => {
    const data = await fetchJSON(`/dashboard/api/conversations/${encodeURIComponent(agent.id)}`);
    setTurns(data.turns || []);
  }, [agent.id]);

  const loadPrompt = useCallback(async () => {
    const data = await fetchJSON(`/api/agents/${encodeURIComponent(agent.id)}/prompt`);
    setPromptState(data);
    setPromptEdit(data.effective_prompt || "");
  }, [agent.id]);

  useEffect(() => {
    loadTurns().catch(() => {});
    loadPrompt().catch(() => {});
  }, [loadPrompt, loadTurns]);

  const run = useCallback(async (key, fn) => {
    setBusy(key);
    try {
      const out = await fn();
      addToast(out.message || "Done", "success");
      if (onAction) onAction();
      return out;
    } catch (err) {
      addToast(err.message, "error");
      throw err;
    } finally {
      setBusy("");
    }
  }, [addToast, onAction]);

  const openPromptDiff = useCallback(() => {
    fetchJSON(`/api/agents/${encodeURIComponent(agent.id)}/prompt/diff`)
      .then((data) => {
        setDiffData(data);
        setShowDiff(true);
      })
      .catch((err) => addToast(err.message, "error"));
  }, [addToast, agent.id]);

  const togglePromptEdit = useCallback(() => {
    setPromptEdit(promptState?.effective_prompt || "");
    setPromptNotes("");
    setEditingPrompt((value) => !value);
  }, [promptState]);

  const savePromptOverride = useCallback(() => run("save-prompt", async () => {
    const out = await putJSON(`/api/agents/${encodeURIComponent(agent.id)}/prompt`, {
      prompt: promptEdit,
      source: "dashboard",
      notes: promptNotes || undefined,
    });
    await loadPrompt();
    setEditingPrompt(false);
    return out;
  }), [agent.id, loadPrompt, promptEdit, promptNotes, run]);

  const revertPromptOverride = useCallback(() => run("revert-prompt", async () => {
    const out = await deleteJSON(`/api/agents/${encodeURIComponent(agent.id)}/prompt`);
    await loadPrompt();
    setEditingPrompt(false);
    return out;
  }), [agent.id, loadPrompt, run]);

  const sendChat = useCallback(() => {
    const message = chatMessage.trim();
    if (!message) return Promise.resolve(null);
    return run("chat", async () => {
      const out = await postJSON(`/api/chat/${encodeURIComponent(agent.id)}`, { mode: chatMode, message });
      setChatMessage("");
      await loadTurns();
      return out;
    });
  }, [agent.id, chatMessage, chatMode, loadTurns, run]);

  const sendDirective = useCallback(() => {
    const message = directiveMessage.trim();
    if (!message) return Promise.resolve(null);
    return run("directive", async () => {
      const out = await postJSON("/dashboard/api/control/directive", { agent_id: agent.id, message });
      setDirectiveMessage("");
      return out;
    });
  }, [agent.id, directiveMessage, run]);

  const restartAgent = useCallback(() => run("restart", () => postJSON("/dashboard/api/control/agents/restart", { agent_id: agent.id })), [agent.id, run]);
  const replayAgent = useCallback(() => run("replay", () => postJSON("/dashboard/api/control/agents/replay", { agent_id: agent.id })), [agent.id, run]);

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
      state: promptState,
      edit: promptEdit,
      setEdit: setPromptEdit,
      notes: promptNotes,
      setNotes: setPromptNotes,
      editing: editingPrompt,
      setEditing: setEditingPrompt,
      diffOpen: showDiff,
      setDiffOpen: setShowDiff,
      diffData,
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
    turns,
    busy,
  };
}
