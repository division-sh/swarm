import React from "react";
import { fmtTime } from "../../lib/format.js";

export default function DigestView({ state, actions }) {
  const { digestResp } = state;
  const { refresh } = actions;

  return (
    <section>
      <div className="head">
        <h2>Portfolio Digest</h2>
        <div className="stack">
          <button onClick={refresh}>Refresh</button>
        </div>
      </div>
      <div className="body scroll">
        <div className="tiny">Last compiled</div>
        <div className="mono">{fmtTime(digestResp && digestResp.last_compiled && digestResp.last_compiled.at)}</div>
        <div className="tiny" style={{ marginTop: 10 }}>Current digest</div>
        <pre className="json" style={{ whiteSpace: "pre-wrap", maxHeight: "58vh" }}>
          {(digestResp && digestResp.current && digestResp.current.text) || "No digest available."}
        </pre>
        <div className="tiny" style={{ marginTop: 10 }}>Last compiled payload</div>
        <pre className="json" style={{ maxHeight: 260 }}>
          {JSON.stringify((digestResp && digestResp.last_compiled && digestResp.last_compiled.payload) || {}, null, 2)}
        </pre>
      </div>
    </section>
  );
}
