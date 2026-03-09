import React from "react";
import Modal from "../../components/Modal.tsx";

function DiffContent({ diff }) {
  return (
    <pre className="system-prompt-body mono">
      {(diff || "No differences").split("\n").map((line, index) => {
        let cls = "diff-line-ctx";
        if (line.startsWith("+")) cls = "diff-line-add";
        else if (line.startsWith("-")) cls = "diff-line-del";
        else if (line.startsWith("@@")) cls = "diff-line-hunk";
        return <div key={index} className={cls}>{line}</div>;
      })}
    </pre>
  );
}

export default function AgentPromptPanel({ agent, prompt, busy, addToast }) {
  if (!prompt.state && !agent.system_prompt) return null;

  return (
    <>
      {prompt.state ? (
        <details className="agent-system-prompt" style={{ marginBottom: 8 }}>
          <summary className="tiny" style={{ cursor: "pointer", userSelect: "none" }}>
            Prompt{" "}
            <span className={`prompt-badge ${prompt.state.has_override ? "prompt-badge-override" : ""}`}>
              {prompt.state.has_override ? "OVERRIDE" : "TEMPLATE"}
            </span>
          </summary>
          <pre className="system-prompt-body mono">{prompt.state.effective_prompt}</pre>
          <div className="stack" style={{ marginTop: 6 }}>
            <button className="btn-secondary" onClick={prompt.openDiff}>View Diff</button>
            <button className="btn-secondary" onClick={prompt.toggleEdit}>{prompt.editing ? "Cancel Edit" : "Edit Override"}</button>
            {prompt.state.has_override ? (
              <button
                className="btn-secondary"
                disabled={!!busy}
                onClick={() => {
                  if (!window.confirm("Revert to template prompt? This will remove the current override.")) return;
                  prompt.revertOverride().catch((err) => addToast(err.message, "error"));
                }}
              >
                {busy === "revert-prompt" ? "…" : "Revert to Template"}
              </button>
            ) : null}
          </div>
          {prompt.editing ? (
            <div style={{ marginTop: 8 }}>
              <textarea className="prompt-editor" value={prompt.edit} onChange={(e) => prompt.setEdit(e.target.value)} />
              <input style={{ width: "100%", marginTop: 4 }} placeholder="Notes (optional)" value={prompt.notes} onChange={(e) => prompt.setNotes(e.target.value)} />
              <div className="stack" style={{ marginTop: 6 }}>
                <button
                  disabled={!!busy || !prompt.edit.trim()}
                  onClick={() => prompt.saveOverride().catch((err) => addToast(err.message, "error"))}
                >
                  {busy === "save-prompt" ? "Saving…" : "Save Override"}
                </button>
              </div>
            </div>
          ) : null}
        </details>
      ) : (
        <details className="agent-system-prompt" style={{ marginBottom: 8 }}>
          <summary className="tiny" style={{ cursor: "pointer", userSelect: "none" }}>System Prompt</summary>
          <pre className="system-prompt-body mono">{agent.system_prompt}</pre>
        </details>
      )}
      {prompt.diffOpen && prompt.diffData ? (
        <Modal title="Prompt Diff" onClose={() => prompt.setDiffOpen(false)} copyText={prompt.diffData.diff || ""}>
          <DiffContent diff={prompt.diffData.diff} />
        </Modal>
      ) : null}
    </>
  );
}
