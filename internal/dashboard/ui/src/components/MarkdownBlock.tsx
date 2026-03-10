import React from "react";
import JsonBlock from "./JsonBlock.tsx";

function formatInline(text) {
  const parts = [];
  const rest = text;
  let k = 0;
  const rx = /(`[^`]+`|\*\*[^*]+\*\*|\*[^*]+\*|_[^_]+_|\[([^\]]+)\]\(([^)]+)\))/g;
  let lastIdx = 0;
  let m;
  while ((m = rx.exec(rest)) !== null) {
    if (m.index > lastIdx) parts.push(<span key={k++}>{rest.slice(lastIdx, m.index)}</span>);
    const tok = m[0];
    if (tok.startsWith("`")) parts.push(<code key={k++} className="md-inline-code">{tok.slice(1, -1)}</code>);
    else if (tok.startsWith("**")) parts.push(<strong key={k++}>{tok.slice(2, -2)}</strong>);
    else if (tok.startsWith("*") || tok.startsWith("_")) parts.push(<em key={k++}>{tok.slice(1, -1)}</em>);
    else if (m[2] && m[3]) parts.push(<span key={k++} className="md-link" title={m[3]}>{m[2]}</span>);
    lastIdx = m.index + tok.length;
  }
  if (lastIdx < rest.length) parts.push(<span key={k++}>{rest.slice(lastIdx)}</span>);
  return parts.length > 0 ? parts : text;
}

export default function MarkdownBlock({ text, className = "md-body" }) {
  if (!text) return null;
  const lines = text.split("\n");
  const out = [];
  let inCode = false;
  let codeLang = "";
  let codeBuf = [];
  let codeKey = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (line.startsWith("```")) {
      if (inCode) {
        const raw = codeBuf.join("\n");
        if (codeLang === "json") {
          try {
            const parsed = JSON.parse(raw);
            out.push(<JsonBlock key={`code-${codeKey++}`} data={parsed} defaultOpen={2} />);
          } catch {
            out.push(<pre key={`code-${codeKey++}`} className="md-code-block">{raw}</pre>);
          }
        } else {
          out.push(<pre key={`code-${codeKey++}`} className="md-code-block">{raw}</pre>);
        }
        codeBuf = [];
        inCode = false;
        codeLang = "";
      } else {
        inCode = true;
        codeLang = line.slice(3).trim().toLowerCase();
      }
      continue;
    }
    if (inCode) {
      codeBuf.push(line);
      continue;
    }
    if (/^#{1,3}\s/.test(line)) {
      const level = line.match(/^(#{1,3})/)[1].length;
      const value = line.replace(/^#{1,3}\s+/, "");
      out.push(<div key={i} className={`md-h${level}`}>{value}</div>);
    } else if (/^[-*]\s/.test(line)) {
      out.push(<div key={i} className="md-li">{formatInline(line.replace(/^[-*]\s+/, ""))}</div>);
    } else if (/^\d+\.\s/.test(line)) {
      out.push(<div key={i} className="md-li md-li-num">{formatInline(line)}</div>);
    } else if (line.trim() === "") {
      out.push(<div key={i} className="md-blank" />);
    } else {
      out.push(<div key={i} className="md-p">{formatInline(line)}</div>);
    }
  }

  if (inCode && codeBuf.length > 0) {
    out.push(<pre key={`code-${codeKey}`} className="md-code-block">{codeBuf.join("\n")}</pre>);
  }

  return <div className={className}>{out}</div>;
}
