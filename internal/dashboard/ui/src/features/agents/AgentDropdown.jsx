import React, { useEffect, useState } from "react";
import { deleteJSON, fetchJSON, postJSON, putJSON } from "../../api/client.js";
import Modal from "../../components/Modal.jsx";
import { fmtTime, formatDurationMs, relTime } from "../../lib/format.js";

export default function AgentDropdown({ agent, addToast, onNavigate, onAction }) {
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

  useEffect(() => {
    fetchJSON(`/dashboard/api/conversations/${encodeURIComponent(agent.id)}`)
      .then((d) => setTurns(d.turns || []))
      .catch(() => {});
    fetchJSON(`/api/agents/${encodeURIComponent(agent.id)}/prompt`)
      .then((d) => {
        setPromptState(d);
        setPromptEdit(d.effective_prompt || "");
      })
      .catch(() => {});
  }, [agent.id]);

  function reloadTurns() {
    fetchJSON(`/dashboard/api/conversations/${encodeURIComponent(agent.id)}`)
      .then((d) => setTurns(d.turns || []))
      .catch(() => {});
  }

  function reloadPrompt() {
    fetchJSON(`/api/agents/${encodeURIComponent(agent.id)}/prompt`)
      .then((d) => {
        setPromptState(d);
        setPromptEdit(d.effective_prompt || "");
      })
      .catch(() => {});
  }

  async function exec(key, fn) {
    setBusy(key);
    try {
      const out = await fn();
      addToast(out.message || "Done", "success");
      if (onAction) onAction();
    } catch (err) {
      addToast(err.message, "error");
    } finally {
      setBusy("");
    }
  }

  const creationID = agent.creation_event && agent.creation_event.id ? agent.creation_event.id : "";
  const isEmpireCoordinator = (agent.id || "").trim() === "empire-coordinator";
  const geoDatalistID = `geo-options-${(agent.id || "agent").replace(/[^a-zA-Z0-9_-]/g, "-")}`;
  const quickGeographyOptions = ["US", "Argentina", "Brazil", "Mexico", "Chile", "Peru", "Paraguay", "Uruguay", "Colombia"];

  function buildQuickDirective() {
    const geo = (quickGeography || "").trim() || "US";
    if (quickUseCorpus) {
      const corpusPath = (quickCorpusPath || "").trim() || "/data/test-signals-25.jsonl";
      return `run corpus in ${geo}, corpus_path=${corpusPath}`;
    }
    const mode = (quickMode || "").trim() || "saas_gap";
    return `run ${mode} in ${geo}`;
  }

  return (
    <div className="agent-drop">
      <div className="agent-drop-grid">
        <div>
          <div className="agent-kv tiny"><strong>Agent</strong><span className="mono">{agent.id}</span></div>
          <div className="agent-kv tiny"><strong>Role</strong>{agent.role || "-"}</div>
          <div className="agent-kv tiny"><strong>Vertical</strong>{agent.vertical_slug || agent.vertical_id || "holding"}</div>
          <div className="agent-kv tiny"><strong>Created</strong><span title={fmtTime(agent.started_at)}>{relTime(agent.started_at)}</span></div>
          <div className="agent-kv tiny"><strong>Pending</strong>{agent.pending_events || 0}{(agent.oldest_pending_age_sec || 0) > 0 ? ` (oldest ${formatDurationMs((agent.oldest_pending_age_sec || 0) * 1000)})` : ""}</div>
          <div className="agent-kv tiny"><strong>In-Flight Turn</strong>{agent.in_flight_turn ? `yes (${formatDurationMs((agent.in_flight_seconds || 0) * 1000)})` : "no"}</div>
          <div className="agent-kv tiny"><strong>Session Lease</strong>{agent.lock_owner ? `locked until ${relTime(agent.lock_expires_at)}` : "unlocked"}</div>
          <div className="agent-kv tiny"><strong>Creation Event</strong>{agent.creation_event && agent.creation_event.type ? `${agent.creation_event.type} ${relTime(agent.creation_event.created_at)}` : "No source event"}</div>
          <div className="stack" style={{ marginBottom: 8 }}>
            <button className="btn-secondary" disabled={!creationID} onClick={() => onNavigate("events", { eventID: creationID })}>Open Creation Event</button>
            <button className="btn-secondary" onClick={() => onNavigate("convos", { convID: agent.id })}>Open Conversation</button>
            <button className="btn-secondary" onClick={() => onNavigate("events", { eventsSubscriber: agent.id })}>View Events</button>
            <button className="btn-secondary" onClick={() => onNavigate("logs", { logsAgent: agent.id })}>View Logs</button>
          </div>
          {promptState ? (
            <details className="agent-system-prompt" style={{ marginBottom: 8 }}>
              <summary className="tiny" style={{ cursor: "pointer", userSelect: "none" }}>
                Prompt{" "}
                <span className={`prompt-badge ${promptState.has_override ? "prompt-badge-override" : ""}`}>
                  {promptState.has_override ? "OVERRIDE" : "TEMPLATE"}
                </span>
              </summary>
              <pre className="system-prompt-body mono">{promptState.effective_prompt}</pre>
              <div className="stack" style={{ marginTop: 6 }}>
                <button className="btn-secondary" onClick={() => {
                  fetchJSON(`/api/agents/${encodeURIComponent(agent.id)}/prompt/diff`)
                    .then((d) => {
                      setDiffData(d);
                      setShowDiff(true);
                    })
                    .catch((err) => addToast(err.message, "error"));
                }}>View Diff</button>
                <button className="btn-secondary" onClick={() => {
                  setPromptEdit(promptState.effective_prompt || "");
                  setPromptNotes("");
                  setEditingPrompt(!editingPrompt);
                }}>{editingPrompt ? "Cancel Edit" : "Edit Override"}</button>
                {promptState.has_override ? (
                  <button className="btn-secondary" disabled={!!busy} onClick={() => {
                    if (!window.confirm("Revert to template prompt? This will remove the current override.")) return;
                    exec("revert-prompt", async () => {
                      const out = await deleteJSON(`/api/agents/${encodeURIComponent(agent.id)}/prompt`);
                      reloadPrompt();
                      setEditingPrompt(false);
                      return out;
                    });
                  }}>{busy === "revert-prompt" ? "\u2026" : "Revert to Template"}</button>
                ) : null}
              </div>
              {editingPrompt ? (
                <div style={{ marginTop: 8 }}>
                  <textarea className="prompt-editor" value={promptEdit} onChange={(e) => setPromptEdit(e.target.value)} />
                  <input style={{ width: "100%", marginTop: 4 }} placeholder="Notes (optional)" value={promptNotes} onChange={(e) => setPromptNotes(e.target.value)} />
                  <div className="stack" style={{ marginTop: 6 }}>
                    <button disabled={!!busy || !promptEdit.trim()} onClick={() => {
                      exec("save-prompt", async () => {
                        const out = await putJSON(`/api/agents/${encodeURIComponent(agent.id)}/prompt`, {
                          prompt: promptEdit,
                          source: "dashboard",
                          notes: promptNotes || undefined,
                        });
                        reloadPrompt();
                        setEditingPrompt(false);
                        return out;
                      });
                    }}>{busy === "save-prompt" ? "Saving\u2026" : "Save Override"}</button>
                  </div>
                </div>
              ) : null}
            </details>
          ) : agent.system_prompt ? (
            <details className="agent-system-prompt" style={{ marginBottom: 8 }}>
              <summary className="tiny" style={{ cursor: "pointer", userSelect: "none" }}>System Prompt</summary>
              <pre className="system-prompt-body mono">{agent.system_prompt}</pre>
            </details>
          ) : null}
          {showDiff && diffData ? (
            <Modal title="Prompt Diff" onClose={() => setShowDiff(false)} copyText={diffData.diff || ""}>
              <pre className="system-prompt-body mono">{(diffData.diff || "No differences").split("\n").map((line, i) => {
                let cls = "diff-line-ctx";
                if (line.startsWith("+")) cls = "diff-line-add";
                else if (line.startsWith("-")) cls = "diff-line-del";
                else if (line.startsWith("@@")) cls = "diff-line-hunk";
                return <div key={i} className={cls}>{line}</div>;
              })}</pre>
            </Modal>
          ) : null}
          <div className="tiny">Recent turns</div>
          <div className="body scroll" style={{ maxHeight: 180, padding: 0 }}>
            <table>
              <thead><tr><th>#</th><th>OK</th><th>Latency</th><th>Result</th></tr></thead>
              <tbody>
                {turns.length === 0 ? (
                  <tr><td colSpan={4} className="empty-state">No turns recorded</td></tr>
                ) : turns.slice(0, 8).map((t, i) => (
                  <tr key={`${t.turn_index || i}-${i}`}>
                    <td>{t.turn_index}</td>
                    <td>{t.parse_ok ? "yes" : "no"}</td>
                    <td>{t.latency_ms}</td>
                    <td className="tiny">{t.tool_result || t.assistant_text || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="agent-actions">
          <div className="tiny">Direct discussion</div>
          <textarea value={chatMessage} onChange={(e) => setChatMessage(e.target.value)} placeholder="Ask this agent directly..." />
          <div className="stack" style={{ marginTop: 6 }}>
            <select value={chatMode} onChange={(e) => setChatMode(e.target.value)}>
              <option value="live">live</option>
              <option value="async">async</option>
            </select>
            <button disabled={!!busy || !chatMessage.trim()} onClick={() => {
              const msg = chatMessage.trim();
              if (!msg) return;
              exec("chat", async () => {
                const out = await postJSON(`/api/chat/${encodeURIComponent(agent.id)}`, { mode: chatMode, message: msg });
                setChatMessage("");
                reloadTurns();
                return out;
              });
            }}>{busy === "chat" ? "Sending\u2026" : "Send Chat"}</button>
          </div>

          <div className="tiny" style={{ marginTop: 10 }}>Directive</div>
          {isEmpireCoordinator ? (
            <div className="control-card" style={{ marginTop: 6, marginBottom: 8, padding: 10 }}>
              <div className="tiny" style={{ marginBottom: 6 }}>Quick Campaign Builder</div>
              <div className="tiny" style={{ marginBottom: 4 }}>Geography</div>
              <input
                list={geoDatalistID}
                value={quickGeography}
                onChange={(e) => setQuickGeography(e.target.value)}
                placeholder="US"
              />
              <datalist id={geoDatalistID}>
                {quickGeographyOptions.map((geo) => <option key={geo} value={geo} />)}
              </datalist>
              <label className="tiny" style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 8 }}>
                <input
                  type="checkbox"
                  checked={quickUseCorpus}
                  onChange={(e) => setQuickUseCorpus(!!e.target.checked)}
                />
                Use corpus file
              </label>
              {quickUseCorpus ? (
                <>
                  <div className="tiny" style={{ marginTop: 8, marginBottom: 4 }}>Corpus Path</div>
                  <input value={quickCorpusPath} onChange={(e) => setQuickCorpusPath(e.target.value)} placeholder="/data/test-signals-25.jsonl" />
                </>
              ) : (
                <>
                  <div className="tiny" style={{ marginTop: 8, marginBottom: 4 }}>Mode</div>
                  <select value={quickMode} onChange={(e) => setQuickMode(e.target.value)}>
                    <option value="saas_gap">saas_gap</option>
                    <option value="ops_tooling">ops_tooling</option>
                    <option value="ai_workflow">ai_workflow</option>
                    <option value="b2b_services">b2b_services</option>
                  </select>
                </>
              )}
              <div className="stack" style={{ marginTop: 8 }}>
                <button className="btn-secondary" type="button" onClick={() => setDirectiveMessage(buildQuickDirective())}>Fill Directive</button>
              </div>
            </div>
          ) : null}
          <textarea value={directiveMessage} onChange={(e) => setDirectiveMessage(e.target.value)} placeholder="Tell the agent what to do..." />
          <div className="stack" style={{ marginTop: 6 }}>
            <button disabled={!!busy || !directiveMessage.trim()} onClick={() => {
              const msg = directiveMessage.trim();
              if (!msg) return;
              exec("directive", async () => {
                const out = await postJSON("/dashboard/api/control/directive", { agent_id: agent.id, message: msg });
                setDirectiveMessage("");
                return out;
              });
            }}>{busy === "directive" ? "Sending\u2026" : "Send Directive"}</button>
            <button className="btn-secondary" disabled={!!busy} onClick={() => exec("restart", async () => postJSON("/dashboard/api/control/agents/restart", { agent_id: agent.id }))}>
              {busy === "restart" ? "Restarting\u2026" : "Restart"}
            </button>
            <button className="btn-secondary" disabled={!!busy} onClick={() => exec("replay", async () => postJSON("/dashboard/api/control/agents/replay", { agent_id: agent.id }))}>
              {busy === "replay" ? "Replaying\u2026" : "Replay"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
