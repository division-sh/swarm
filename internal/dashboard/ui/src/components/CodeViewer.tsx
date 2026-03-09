import Editor, { DiffEditor } from "@monaco-editor/react";
import React from "react";

type ViewerHeight = number | `${number}px` | `${number}vh` | `${number}vw` | `${number}%`;

type CodeViewerProps = {
  value: unknown;
  language?: string;
  height?: ViewerHeight;
  className?: string;
  compact?: boolean;
};

type DiffCodeViewerProps = {
  original: unknown;
  modified: unknown;
  language?: string;
  height?: ViewerHeight;
  className?: string;
  compact?: boolean;
};

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
}: CodeViewerProps) {
  return (
    <div className={`code-viewer ${compact ? "code-viewer-compact" : ""} ${className}`.trim()}>
      <Editor
        theme="vs-dark"
        language={language}
        value={String(value ?? "")}
        height={height}
        loading={<div className="code-viewer-loading tiny">Loading editor…</div>}
        options={DEFAULT_OPTIONS as any}
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
}: DiffCodeViewerProps) {
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
        } as any}
      />
    </div>
  );
}
