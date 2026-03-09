import React from "react";

function JsonView({ data, defaultOpen = 1, depth = 0 }) {
  if (data === null) return <span className="json-null">null</span>;
  if (data === undefined) return <span className="json-null">undefined</span>;
  if (typeof data === "boolean") return <span className="json-bool">{data ? "true" : "false"}</span>;
  if (typeof data === "number") return <span className="json-num">{String(data)}</span>;
  if (typeof data === "string") {
    const display = data.length > 300 ? `${data.slice(0, 300)}\u2026` : data;
    return <span className="json-str">{'"'}{display}{'"'}</span>;
  }

  const isArray = Array.isArray(data);
  const entries = isArray ? data.map((v, i) => [i, v]) : Object.entries(data);

  if (entries.length === 0) {
    return <span className="json-bracket">{isArray ? "[]" : "{}"}</span>;
  }

  const isSmall = entries.length <= 2 && entries.every(([, v]) => v === null || typeof v !== "object");
  if (isSmall && depth > 0) {
    return (
      <span>
        <span className="json-bracket">{isArray ? "[" : "{"}</span>
        {entries.map(([k, v], i) => (
          <span key={k}>
            {i > 0 ? <span className="json-bracket">, </span> : null}
            {!isArray ? <><span className="json-key">{'"'}{k}{'"'}</span><span className="json-bracket">: </span></> : null}
            <JsonView data={v} defaultOpen={defaultOpen} depth={depth + 1} />
          </span>
        ))}
        <span className="json-bracket">{isArray ? "]" : "}"}</span>
      </span>
    );
  }

  const open = depth < defaultOpen;
  const label = isArray ? `Array(${entries.length})` : `{${entries.length} keys}`;

  return (
    <details className="json-toggle" open={open || undefined}>
      <summary>
        <span className="json-bracket">{isArray ? "[" : "{"}</span>
        <span className="json-collapse-hint">{label}</span>
      </summary>
      <div className="json-indent">
        {entries.map(([k, v], i) => (
          <div key={k} className="json-entry">
            {!isArray ? <><span className="json-key">{'"'}{k}{'"'}</span><span className="json-bracket">: </span></> : null}
            <JsonView data={v} defaultOpen={defaultOpen} depth={depth + 1} />
            {i < entries.length - 1 ? <span className="json-bracket">,</span> : null}
          </div>
        ))}
      </div>
      <span className="json-bracket">{isArray ? "]" : "}"}</span>
    </details>
  );
}

export default function JsonBlock({ data, defaultOpen }) {
  return (
    <div className="json-view">
      <JsonView data={data} defaultOpen={defaultOpen != null ? defaultOpen : 1} />
    </div>
  );
}
