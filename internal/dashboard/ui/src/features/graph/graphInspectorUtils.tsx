import React from "react";
import CodeViewer from "../../components/CodeViewer.tsx";
import { edgeSelectionID, findEdgeBySelectionID } from "./graphSelection.ts";

export function compactValue(value) {
  if (value == null || value === "") return "-";
  if (Array.isArray(value)) {
    if (value.length === 0) return "-";
    if (value.every((item) => item == null || ["string", "number", "boolean"].includes(typeof item))) {
      return value.join(", ");
    }
    return `${value.length} items`;
  }
  if (typeof value === "object") {
    const keys = Object.keys(value);
    return keys.length ? keys.join(", ") : "-";
  }
  return String(value);
}

export function renderTagList(items, emptyLabel = "None") {
  if (!items || items.length === 0) return <div className="empty-state">{emptyLabel}</div>;
  return (
    <div className="node-tools">
      {items.map((item, index) => (
        <span key={`${item}:${index}`} className="node-tool-badge">{item}</span>
      ))}
    </div>
  );
}

export function renderRawDetails(title, data) {
  if (data == null) return null;
  return (
    <details className="node-detail-card">
      <summary className="tiny" style={{ cursor: "pointer" }}>{title}</summary>
      <CodeViewer
        language="json"
        value={JSON.stringify(data, null, 2)}
        height={260}
        compact
      />
    </details>
  );
}

export function toYamlBlock(key, value) {
  const body = String(value || "")
    .split("\n")
    .map((line) => `  ${line}`)
    .join("\n");
  return `${key}: |-\n${body}`;
}

export { edgeSelectionID, findEdgeBySelectionID };
