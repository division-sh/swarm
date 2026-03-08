import Editor, { DiffEditor } from "@monaco-editor/react";
import React from "react";

const DEFAULT_OPTIONS = {
  readOnly: true,
  minimap: { enabled: false },
  scrollBeyondLastLine: false,
  smoothScrolling: true,
  automaticLayout: true,
  wordWrap: "on",
  wrappingIndent: "same",
  lineNumbersMinChars: 3,
  glyphMargin: false,
  folding: true,
  renderWhitespace: "selection",
  scrollbar: {
    verticalScrollbarSize: 8,
    horizontalScrollbarSize: 8,
  },
};

export default function CodeViewer({
  value,
  language = "json",
  height = 280,
  className = "",
  compact = false,
}) {
  return (
    <div className={`code-viewer ${compact ? "code-viewer-compact" : ""} ${className}`.trim()}>
      <Editor
        theme="vs-dark"
        language={language}
        value={String(value ?? "")}
        height={height}
        loading={<div className="code-viewer-loading tiny">Loading editor…</div>}
        options={DEFAULT_OPTIONS}
      />
    </div>
  );
}

export function DiffCodeViewer({
  original,
  modified,
  language = "json",
  height = 320,
  className = "",
  compact = false,
}) {
  return (
    <div className={`code-viewer ${compact ? "code-viewer-compact" : ""} ${className}`.trim()}>
      <DiffEditor
        theme="vs-dark"
        original={String(original ?? "")}
        modified={String(modified ?? "")}
        language={language}
        height={height}
        loading={<div className="code-viewer-loading tiny">Loading editor…</div>}
        options={{
          ...DEFAULT_OPTIONS,
          renderSideBySide: true,
          readOnly: true,
          originalEditable: false,
          diffWordWrap: "on",
        }}
      />
    </div>
  );
}
