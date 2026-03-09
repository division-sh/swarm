import React, { useState } from "react";

export default function CopyID({ id, len = 8 }) {
  const [copied, setCopied] = useState(false);
  if (!id) return <span className="mono">-</span>;
  return (
    <span
      className={`copy-id mono ${copied ? "copied" : ""}`}
      title={`Click to copy: ${id}`}
      onClick={(e) => {
        e.stopPropagation();
        navigator.clipboard.writeText(id).catch(() => {});
        setCopied(true);
        setTimeout(() => setCopied(false), 1200);
      }}
    >
      {id.slice(0, len)}{copied ? " \u2713" : ""}
    </span>
  );
}
