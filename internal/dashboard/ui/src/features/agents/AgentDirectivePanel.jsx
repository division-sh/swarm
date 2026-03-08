import React from "react";

function QuickDirectiveBuilder({ quickDirective, setDirectiveMessage }) {
  return (
    <div className="control-card" style={{ marginTop: 6, marginBottom: 8, padding: 10 }}>
      <div className="tiny" style={{ marginBottom: 6 }}>Quick Campaign Builder</div>
      <div className="tiny" style={{ marginBottom: 4 }}>Geography</div>
      <input
        list={quickDirective.datalistID}
        value={quickDirective.geography}
        onChange={(e) => quickDirective.setGeography(e.target.value)}
        placeholder="US"
      />
      <datalist id={quickDirective.datalistID}>
        {quickDirective.options.map((geo) => <option key={geo} value={geo} />)}
      </datalist>
      <label className="tiny" style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 8 }}>
        <input
          type="checkbox"
          checked={quickDirective.useCorpus}
          onChange={(e) => quickDirective.setUseCorpus(!!e.target.checked)}
        />
        Use corpus file
      </label>
      {quickDirective.useCorpus ? (
        <>
          <div className="tiny" style={{ marginTop: 8, marginBottom: 4 }}>Corpus Path</div>
          <input value={quickDirective.corpusPath} onChange={(e) => quickDirective.setCorpusPath(e.target.value)} placeholder="/data/test-signals-25.jsonl" />
        </>
      ) : (
        <>
          <div className="tiny" style={{ marginTop: 8, marginBottom: 4 }}>Mode</div>
          <select value={quickDirective.mode} onChange={(e) => quickDirective.setMode(e.target.value)}>
            <option value="saas_gap">saas_gap</option>
            <option value="ops_tooling">ops_tooling</option>
            <option value="ai_workflow">ai_workflow</option>
            <option value="b2b_services">b2b_services</option>
          </select>
        </>
      )}
      <div className="stack" style={{ marginTop: 8 }}>
        <button className="btn-secondary" type="button" onClick={() => setDirectiveMessage(quickDirective.value)}>Fill Directive</button>
      </div>
    </div>
  );
}

export default function AgentDirectivePanel({ directive, quickDirective, busy, addToast }) {
  return (
    <>
      <div className="tiny" style={{ marginTop: 10 }}>Directive</div>
      {quickDirective.enabled ? (
        <QuickDirectiveBuilder quickDirective={quickDirective} setDirectiveMessage={directive.setMessage} />
      ) : null}
      <textarea value={directive.message} onChange={(e) => directive.setMessage(e.target.value)} placeholder="Tell the agent what to do..." />
      <div className="stack" style={{ marginTop: 6 }}>
        <button
          disabled={!!busy || !directive.message.trim()}
          onClick={() => directive.send().catch((err) => addToast(err.message, "error"))}
        >
          {busy === "directive" ? "Sending…" : "Send Directive"}
        </button>
        <button className="btn-secondary" disabled={!!busy} onClick={() => directive.restart().catch((err) => addToast(err.message, "error"))}>
          {busy === "restart" ? "Restarting…" : "Restart"}
        </button>
        <button className="btn-secondary" disabled={!!busy} onClick={() => directive.replay().catch((err) => addToast(err.message, "error"))}>
          {busy === "replay" ? "Replaying…" : "Replay"}
        </button>
      </div>
    </>
  );
}
