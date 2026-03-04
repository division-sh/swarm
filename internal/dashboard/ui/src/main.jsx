import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  ReactFlow,
  Background,
  BaseEdge,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  Panel,
  Position,
  useEdgesState,
  useInternalNode,
  useNodesState,
  useReactFlow,
} from "@xyflow/react";

function getEmpireKey() {
  try {
    return (localStorage.getItem("empire_api_key") || "").trim();
  } catch {
    return "";
  }
}

function fmtTime(v) {
  if (!v) return "-";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleString();
}

function relTime(v) {
  if (!v) return "-";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "-";
  const sec = Math.floor((Date.now() - d.getTime()) / 1000);
  if (sec < 0) return "just now";
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

function formatDollars(cents) {
  if (!cents && cents !== 0) return "$0.00";
  return "$" + (cents / 100).toFixed(2);
}

function formatDurationMs(ms) {
  const n = Number(ms || 0);
  if (!Number.isFinite(n) || n <= 0) return "-";
  if (n < 1000) return `${Math.round(n)}ms`;
  const sec = n / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const min = Math.floor(sec / 60);
  const rem = Math.round(sec % 60);
  return `${min}m ${rem}s`;
}

function readPath(obj, path) {
  let cur = obj;
  for (const key of path) {
    if (!cur || typeof cur !== "object" || !(key in cur)) return "";
    cur = cur[key];
  }
  if (typeof cur === "string") return cur.trim();
  if (typeof cur === "number" || typeof cur === "boolean") return String(cur);
  return "";
}

function firstNonEmptyText(values) {
  for (const v of values || []) {
    const s = typeof v === "string" ? v.trim() : "";
    if (s) return s;
  }
  return "";
}

function hasRuntimeError(item) {
  if (!item || typeof item !== "object") return false;
  if ((item.error_code || "").trim() !== "") return true;
  const level = (item.level || "").toLowerCase();
  if (level === "error") return true;
  return (item.error || "").trim() !== "";
}

function hasEventError(item) {
  if (!item || typeof item !== "object") return false;
  const errors = Number(item.error_count || 0);
  const dead = Number(item.dead_count || 0);
  return errors > 0 || dead > 0;
}

function shardScopeSummary(scope) {
  if (!scope || typeof scope !== "object") return "-";
  const tax = Array.isArray(scope.taxonomy_categories) ? scope.taxonomy_categories : [];
  if (tax.length > 0) return tax.join(", ");
  const trends = Array.isArray(scope.trend_categories) ? scope.trend_categories : [];
  if (trends.length > 0) return trends.join(", ");
  return "-";
}

const VALID_TABS = ["agents", "digest", "events", "logs", "incidents", "flow", "convos", "graph", "control", "tasks", "pipeline", "holding", "health"];

function readHashTab() {
  const h = (location.hash || "").replace("#", "").toLowerCase();
  return VALID_TABS.includes(h) ? h : "agents";
}

async function fetchJSON(url) {
  const headers = {};
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, { headers });
  if (!r.ok) {
    throw new Error(`HTTP ${r.status}`);
  }
  return r.json();
}

async function postJSON(url, body) {
  const headers = { "content-type": "application/json" };
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body || {}),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error(data && data.error ? data.error : `HTTP ${r.status}`);
  }
  return data;
}

async function putJSON(url, body) {
  const headers = { "content-type": "application/json" };
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, {
    method: "PUT",
    headers,
    body: JSON.stringify(body || {}),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error(data && data.error ? data.error : `HTTP ${r.status}`);
  }
  return data;
}

async function deleteJSON(url) {
  const headers = {};
  const key = getEmpireKey();
  if (key) headers["X-Empire-Key"] = key;
  const r = await fetch(url, { method: "DELETE", headers });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error(data && data.error ? data.error : `HTTP ${r.status}`);
  }
  return data;
}

/* ---- Small reusable components ---- */

function LiveClock() {
  const [now, setNow] = useState(new Date());
  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);
  const h = String(now.getHours()).padStart(2, "0");
  const m = String(now.getMinutes()).padStart(2, "0");
  const s = String(now.getSeconds()).padStart(2, "0");
  return <span className="live-clock">{h}:{m}:{s}</span>;
}

function StatusDot({ state }) {
  const cls = `status-dot status-dot-${state || "idle"}`;
  return <span className={cls} />;
}

const VALIDATION_GATES = ["researching", "mvp_speccing", "spec_review", "cto_spec_review", "branding"];
const GATE_LABELS = ["G1 Research", "G2 Spec", "G3 CTO", "G4 Brand"];
const FLOW_STAGE_OPTIONS = ["all", "discovery", "scoring", "validation", "mailbox", "opco", "system"];
const FLOW_RUBRIC_OPTIONS = ["all", "universal"];

function flowStageForEvent(eventType) {
  const t = String(eventType || "").toLowerCase().trim();
  if (!t) return "system";
  if (
    t.startsWith("scan.") ||
    t.startsWith("market_research.") ||
    t.startsWith("trend_research.") ||
    t.startsWith("scanner.") ||
    t.startsWith("category.") ||
    t.startsWith("trend.") ||
    t.startsWith("source.") ||
    t === "campaign.completed"
  ) return "discovery";
  if (
    t.startsWith("score.") ||
    t.startsWith("scoring.") ||
    t === "vertical.discovered" ||
    t === "vertical.scored" ||
    t === "vertical.shortlisted" ||
    t === "vertical.marginal" ||
    t === "vertical.rejected" ||
    t === "timer.portfolio_digest"
  ) return "scoring";
  if (
    t.startsWith("validation.") ||
    t.startsWith("research.") ||
    t.startsWith("spec.") ||
    t.startsWith("cto.") ||
    t.startsWith("brand.") ||
    t === "vertical.ready_for_review" ||
    t === "vertical.resumed"
  ) return "validation";
  if (
    t === "vertical.approved" ||
    t === "vertical.killed" ||
    t === "vertical.needs_more_data" ||
    t.startsWith("human_task.") ||
    t === "mailbox.item_decided"
  ) return "mailbox";
  if (
    t.startsWith("opco.") ||
    t.startsWith("build.") ||
    t.startsWith("deploy.") ||
    t.startsWith("devops.") ||
    t.startsWith("qa.") ||
    t.startsWith("product.") ||
    t.startsWith("growth.") ||
    t.startsWith("support.") ||
    t.startsWith("launch.") ||
    t === "mandate_updated"
  ) return "opco";
  return "system";
}

function flowEventMatchesFilters(eventType, stageFilter, rubricFilter) {
  const stage = flowStageForEvent(eventType);
  if (stageFilter && stageFilter !== "all" && stage !== stageFilter) return false;
  if (rubricFilter && rubricFilter !== "all") {
    const t = String(eventType || "").toLowerCase().trim();
    const rubricAware =
      t.startsWith("score.") ||
      t.startsWith("scoring.") ||
      t === "vertical.discovered" ||
      t === "vertical.scored" ||
      t === "vertical.shortlisted" ||
      t === "vertical.marginal" ||
      t === "vertical.rejected";
    if (!rubricAware) return false;
  }
  return true;
}

function GateIndicator({ stage }) {
  const idx = VALIDATION_GATES.indexOf(stage);
  return (
    <div className="gate-row">
      {GATE_LABELS.map((label, i) => {
        const cls = i < idx ? "gate gate-done" : i === idx ? "gate gate-active" : "gate gate-pending";
        return <span key={i} className={cls}><span className="gate-dot" /><span className="gate-label">{label}</span></span>;
      })}
    </div>
  );
}

function CopyID({ id, len = 8 }) {
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

function Toasts({ items }) {
  if (!items || items.length === 0) return null;
  return (
    <div className="toast-container">
      {items.map((t) => (
        <div key={t.id} className={`toast toast-${t.type}`}>{t.msg}</div>
      ))}
    </div>
  );
}

/* ---- Simple markdown renderer (headings, bold, italic, code, lists, links) ---- */
function renderMarkdown(text) {
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
        // If language hint is json, try to parse and render with JsonBlock
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
    if (inCode) { codeBuf.push(line); continue; }
    if (/^#{1,3}\s/.test(line)) {
      const level = line.match(/^(#{1,3})/)[1].length;
      const text = line.replace(/^#{1,3}\s+/, "");
      out.push(<div key={i} className={`md-h${level}`}>{text}</div>);
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
  return out;
}

function formatInline(text) {
  const parts = [];
  let rest = text;
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

/* ---- Pretty JSON renderer with syntax coloring + collapsible sections ---- */
function JsonView({ data, defaultOpen = 1, _depth = 0 }) {
  if (data === null) return <span className="json-null">null</span>;
  if (data === undefined) return <span className="json-null">undefined</span>;
  if (typeof data === "boolean") return <span className="json-bool">{data ? "true" : "false"}</span>;
  if (typeof data === "number") return <span className="json-num">{String(data)}</span>;
  if (typeof data === "string") {
    // Truncate very long strings inline
    const display = data.length > 300 ? data.slice(0, 300) + "\u2026" : data;
    return <span className="json-str">{'"'}{display}{'"'}</span>;
  }

  const isArray = Array.isArray(data);
  const entries = isArray ? data.map((v, i) => [i, v]) : Object.entries(data);

  if (entries.length === 0) {
    return <span className="json-bracket">{isArray ? "[]" : "{}"}</span>;
  }

  // Small leaf objects render inline
  const isSmall = entries.length <= 2 && entries.every(([, v]) => v === null || typeof v !== "object");
  if (isSmall && _depth > 0) {
    return (
      <span>
        <span className="json-bracket">{isArray ? "[" : "{"}</span>
        {entries.map(([k, v], i) => (
          <span key={k}>
            {i > 0 ? <span className="json-bracket">, </span> : null}
            {!isArray ? <><span className="json-key">{'"'}{k}{'"'}</span><span className="json-bracket">: </span></> : null}
            <JsonView data={v} defaultOpen={defaultOpen} _depth={_depth + 1} />
          </span>
        ))}
        <span className="json-bracket">{isArray ? "]" : "}"}</span>
      </span>
    );
  }

  const open = _depth < defaultOpen;
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
            <JsonView data={v} defaultOpen={defaultOpen} _depth={_depth + 1} />
            {i < entries.length - 1 ? <span className="json-bracket">,</span> : null}
          </div>
        ))}
      </div>
      <span className="json-bracket">{isArray ? "]" : "}"}</span>
    </details>
  );
}

function JsonBlock({ data, defaultOpen }) {
  return (
    <div className="json-view">
      <JsonView data={data} defaultOpen={defaultOpen != null ? defaultOpen : 1} />
    </div>
  );
}

/* ---- Modal overlay ---- */
function Modal({ title, onClose, copyText, children, className = "" }) {
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    function onKey(e) { if (e.key === "Escape") onClose(); }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className={`modal-container ${className}`.trim()} onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div className="modal-title">{title}</div>
          <div className="stack">
            {copyText ? (
              <button className="btn-secondary" onClick={() => {
                navigator.clipboard.writeText(copyText).catch(() => {});
                setCopied(true);
                setTimeout(() => setCopied(false), 1500);
              }}>{copied ? "Copied!" : "Copy"}</button>
            ) : null}
            <button className="btn-secondary modal-close" onClick={onClose}>&times;</button>
          </div>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  );
}

function artifactIsScalar(v) {
  return v == null || typeof v === "string" || typeof v === "number" || typeof v === "boolean";
}

function prettyArtifactKey(key) {
  return String(key || "")
    .replace(/[_\.]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatArtifactScalar(v) {
  if (v == null) return "-";
  if (typeof v === "boolean") return v ? "Yes" : "No";
  if (typeof v === "number") {
    if (Number.isInteger(v)) return String(v);
    return Number(v).toFixed(2);
  }
  const s = String(v).trim();
  return s || "-";
}

function truncateText(text, max = 280) {
  const s = String(text || "").trim();
  if (!s) return "";
  return s.length > max ? s.slice(0, max) + "\u2026" : s;
}

function renderArtifactGeneric(payload) {
  if (!payload) return <div className="empty-state">No data</div>;
  if (typeof payload === "string") {
    return <div className="artifact-text-block">{renderMarkdown(payload)}</div>;
  }
  if (Array.isArray(payload)) {
    if (payload.length === 0) return <div className="empty-state">No data</div>;
    if (payload.every((v) => artifactIsScalar(v))) {
      return (
        <div className="artifact-chip-row">
          {payload.map((v, i) => <span key={i} className="tag">{formatArtifactScalar(v)}</span>)}
        </div>
      );
    }
    return <JsonBlock data={payload} defaultOpen={2} />;
  }
  if (typeof payload !== "object") {
    return <div className="artifact-text-block">{formatArtifactScalar(payload)}</div>;
  }

  const entries = Object.entries(payload);
  if (entries.length === 0) return <div className="empty-state">No data</div>;
  const scalarEntries = entries.filter(([, v]) => artifactIsScalar(v));
  const nestedEntries = entries.filter(([, v]) => !artifactIsScalar(v));

  return (
    <div className="artifact-generic">
      {scalarEntries.length > 0 ? (
        <div className="artifact-kv-list">
          {scalarEntries.map(([k, v]) => (
            <div key={k} className="artifact-kv-row">
              <span>{prettyArtifactKey(k)}</span>
              <span>{formatArtifactScalar(v)}</span>
            </div>
          ))}
        </div>
      ) : null}
      {nestedEntries.map(([k, v]) => (
        <details key={k} className="artifact-subsection">
          <summary>{prettyArtifactKey(k)}</summary>
          {typeof v === "string" ? (
            <div className="artifact-text-block">{renderMarkdown(v)}</div>
          ) : (
            <JsonBlock data={v} defaultOpen={2} />
          )}
        </details>
      ))}
    </div>
  );
}

function normalizeScoreRows(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) {
    return raw
      .map((item, idx) => {
        if (!item || typeof item !== "object") return null;
        const dimension = item.dimension || item.name || item.key || `Dimension ${idx + 1}`;
        const score = item.score ?? item.resolved_score ?? item.value ?? item.points;
        const notes = item.evidence || item.reason || item.rationale || item.comment || "";
        return {
          dimension: String(dimension),
          score: score,
          notes: truncateText(notes, 220),
        };
      })
      .filter(Boolean);
  }
  if (typeof raw === "object") {
    return Object.entries(raw).map(([dimension, value]) => {
      if (artifactIsScalar(value)) {
        return { dimension: prettyArtifactKey(dimension), score: value, notes: "" };
      }
      if (value && typeof value === "object") {
        return {
          dimension: prettyArtifactKey(dimension),
          score: value.score ?? value.resolved_score ?? value.value ?? "",
          notes: truncateText(value.evidence || value.reason || value.rationale || value.comment || "", 220),
        };
      }
      return { dimension: prettyArtifactKey(dimension), score: "", notes: "" };
    });
  }
  return [];
}

function renderScoresArtifact(payload) {
  if (!payload || typeof payload !== "object") return renderArtifactGeneric(payload);
  const summaryKeys = [
    "result",
    "rubric",
    "mode",
    "composite_score",
    "viability_score",
    "market_score",
    "confidence",
    "confidence_score",
    "signal_strength",
  ];
  const summary = summaryKeys
    .filter((k) => payload[k] != null && String(payload[k]).trim() !== "")
    .map((k) => [k, payload[k]]);

  const dimensionRows = normalizeScoreRows(
    payload.dimensions || payload.dimension_scores || payload.breakdown || payload.scores
  );

  return (
    <div className="artifact-generic">
      {summary.length > 0 ? (
        <div className="artifact-kv-list">
          {summary.map(([k, v]) => (
            <div key={k} className="artifact-kv-row">
              <span>{prettyArtifactKey(k)}</span>
              <span>{formatArtifactScalar(v)}</span>
            </div>
          ))}
        </div>
      ) : null}

      {dimensionRows.length > 0 ? (
        <div className="artifact-table-wrap">
          <table className="artifact-table">
            <thead>
              <tr><th>Dimension</th><th>Score</th><th>Notes</th></tr>
            </thead>
            <tbody>
              {dimensionRows.map((row, idx) => (
                <tr key={`${row.dimension}-${idx}`}>
                  <td>{row.dimension}</td>
                  <td>{formatArtifactScalar(row.score)}</td>
                  <td>{row.notes || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}

      <details className="artifact-subsection">
        <summary>Raw Score Payload</summary>
        <JsonBlock data={payload} defaultOpen={2} />
      </details>
    </div>
  );
}

function renderRawSignalsArtifact(payload) {
  if (!payload || typeof payload !== "object") return renderArtifactGeneric(payload);
  const summaryKeys = ["category", "subcategory", "geography", "mode", "signal_strength", "priority"];
  const summary = summaryKeys
    .filter((k) => payload[k] != null && String(payload[k]).trim() !== "")
    .map((k) => [k, payload[k]]);
  const opportunity = payload.opportunity_hypothesis || payload.opportunity || payload.hypothesis || "";
  const evidence = payload.evidence || payload.market_evidence || payload.problem_evidence || "";
  const automationMicro = payload.automation_micro;

  return (
    <div className="artifact-generic">
      {summary.length > 0 ? (
        <div className="artifact-kv-list">
          {summary.map(([k, v]) => (
            <div key={k} className="artifact-kv-row">
              <span>{prettyArtifactKey(k)}</span>
              <span>{formatArtifactScalar(v)}</span>
            </div>
          ))}
        </div>
      ) : null}

      {opportunity ? (
        <div className="artifact-text-card">
          <div className="tiny">Opportunity Hypothesis</div>
          <div className="holding-detail-text">{opportunity}</div>
        </div>
      ) : null}

      {evidence ? (
        <div className="artifact-text-card">
          <div className="tiny">Evidence</div>
          <div className="holding-detail-text">{evidence}</div>
        </div>
      ) : null}

      {automationMicro && typeof automationMicro === "object" ? (
        <details className="artifact-subsection">
          <summary>Automation-Micro Signal</summary>
          {renderArtifactGeneric(automationMicro)}
        </details>
      ) : null}

      <details className="artifact-subsection">
        <summary>Raw Signal Payload</summary>
        <JsonBlock data={payload} defaultOpen={2} />
      </details>
    </div>
  );
}

function renderArtifactPayload(label, payload) {
  if (label === "Scores") return renderScoresArtifact(payload);
  if (label === "Raw Signals") return renderRawSignalsArtifact(payload);
  return renderArtifactGeneric(payload);
}

function HoldingVerticalDetail({ detail }) {
  if (!detail || typeof detail !== "object") return <div className="empty-state">No detail available</div>;
  const v = detail.vertical || {};
  const businessModel = firstNonEmptyText([
    readPath(v, ["business_brief", "business_model"]),
    readPath(v, ["business_brief", "revenue_model"]),
    readPath(v, ["mvp_spec", "business_model"]),
    readPath(v, ["validation_kit", "business_model"]),
    readPath(v, ["full_spec", "business_model"]),
    readPath(v, ["raw_signals", "business_model"]),
  ]);
  const opportunity = firstNonEmptyText([
    readPath(v, ["raw_signals", "opportunity_hypothesis"]),
    readPath(v, ["business_brief", "opportunity_hypothesis"]),
    readPath(v, ["mvp_spec", "opportunity"]),
    readPath(v, ["validation_kit", "opportunity_hypothesis"]),
  ]);

  const artifacts = [
    ["Raw Signals", v.raw_signals],
    ["Scores", v.scores],
    ["Business Brief", v.business_brief],
    ["MVP Spec", v.mvp_spec],
    ["Spec Review", v.spec_review],
    ["CTO Feasibility", v.cto_feasibility],
    ["Brand", v.brand],
    ["Validation Kit", v.validation_kit],
    ["Full Spec", v.full_spec],
    ["Deploy Config", v.deploy_config],
    ["Launch Targets", v.launch_targets],
  ];

  return (
    <div className="holding-detail">
      <div className="holding-detail-summary">
        <div className="holding-detail-title">
          <strong>{v.name || v.slug || v.id || "Vertical"}</strong>
          <div className="stack">
            <span className="tag">{v.stage || "-"}</span>
            {v.mode ? <span className="tag">{v.mode}</span> : null}
            {v.composite_score ? <span className="tag tag-info">score {v.composite_score}</span> : null}
            {v.geography ? <span className="tag">{v.geography}</span> : null}
          </div>
        </div>
        <div className="row">
          <div className="health-card">
            <div className="tiny">Project Snapshot</div>
            <div className="health-kv"><span>ID</span><span className="mono">{v.id || "-"}</span></div>
            <div className="health-kv"><span>Slug</span><span className="mono">{v.slug || "-"}</span></div>
            <div className="health-kv"><span>Template</span><span>{v.template_version || "-"}</span></div>
            <div className="health-kv"><span>Created</span><span>{fmtTime(v.created_at)}</span></div>
            <div className="health-kv"><span>Updated</span><span>{fmtTime(v.updated_at)}</span></div>
            {v.approved_at ? <div className="health-kv"><span>Approved</span><span>{fmtTime(v.approved_at)}</span></div> : null}
            {v.launched_at ? <div className="health-kv"><span>Launched</span><span>{fmtTime(v.launched_at)}</span></div> : null}
          </div>
          <div className="health-card">
            <div className="tiny">Business Model + Opportunity</div>
            <div className="holding-detail-text"><strong>Business Model:</strong> {businessModel || "-"}</div>
            <div className="holding-detail-text"><strong>Opportunity:</strong> {opportunity || "-"}</div>
            {v.live_url ? <div className="holding-detail-text"><strong>Live URL:</strong> <a className="md-link" href={v.live_url} target="_blank" rel="noreferrer">{v.live_url}</a></div> : null}
            {v.kill_reason ? <div className="holding-detail-text health-bad"><strong>Kill Reason:</strong> {v.kill_reason}</div> : null}
            {v.human_notes ? <div className="holding-detail-text"><strong>Notes:</strong> {v.human_notes}</div> : null}
          </div>
        </div>
      </div>

      <div className="holding-detail-section">
        <div className="tiny" style={{ marginBottom: 6 }}>Team</div>
        {(detail.agents || []).length === 0 ? (
          <div className="empty-state">No agents linked yet</div>
        ) : (
          <table>
            <thead><tr><th>Agent</th><th>Role</th><th>Status</th><th>Mode</th><th>Task</th><th>Last Active</th></tr></thead>
            <tbody>
              {(detail.agents || []).map((a) => (
                <tr key={a.id}>
                  <td className="mono">{a.id}</td>
                  <td>{a.role || "-"}</td>
                  <td>{a.status || "-"}</td>
                  <td>{a.mode || "-"}</td>
                  <td className="mono">{a.current_task_id || "-"}</td>
                  <td>{relTime(a.last_active_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="holding-detail-section">
        <div className="tiny" style={{ marginBottom: 6 }}>Artifacts</div>
        {artifacts.map(([label, payload]) => (
          <details key={label} className="holding-artifact-card" open={label === "Business Brief" || label === "Scores"}>
            <summary>{label}</summary>
            {renderArtifactPayload(label, payload)}
          </details>
        ))}
      </div>

      <div className="row">
        <div className="holding-detail-section">
          <div className="tiny" style={{ marginBottom: 6 }}>Recent Events</div>
          {(detail.events || []).length === 0 ? (
            <div className="empty-state">No events</div>
          ) : (
            <table>
              <thead><tr><th>When</th><th>Type</th><th>Source</th></tr></thead>
              <tbody>
                {(detail.events || []).slice(0, 20).map((e) => (
                  <tr key={e.id}>
                    <td>{relTime(e.created_at)}</td>
                    <td>{e.type}</td>
                    <td>{e.source_agent}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        <div className="holding-detail-section">
          <div className="tiny" style={{ marginBottom: 6 }}>Mailbox + Spend</div>
          <div className="stack" style={{ marginBottom: 6 }}>
            <span className="badge">30d spend: {formatDollars((detail.spend && detail.spend.last_30d_cents) || 0)}</span>
            <span className="badge">all-time: {formatDollars((detail.spend && detail.spend.all_time_cents) || 0)}</span>
          </div>
          {(detail.mailbox || []).length === 0 ? (
            <div className="empty-state">No mailbox items</div>
          ) : (
            <table>
              <thead><tr><th>When</th><th>Type</th><th>Status</th><th>Summary</th></tr></thead>
              <tbody>
                {(detail.mailbox || []).slice(0, 20).map((m) => (
                  <tr key={m.id}>
                    <td>{relTime(m.created_at)}</td>
                    <td>{m.type}</td>
                    <td>{m.status}</td>
                    <td>{m.summary || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}

function extractToolCallNames(message, turn) {
  const names = [];
  if (turn && Array.isArray(turn.tool_calls)) {
    for (const tc of turn.tool_calls) {
      const name = (tc && tc.name ? String(tc.name) : "").trim();
      if (name) names.push(name);
    }
  }
  const content = message && message.content;
  if (Array.isArray(content)) {
    for (const item of content) {
      if (!item || typeof item !== "object") continue;
      if ((item.type || "").trim() !== "tool_use") continue;
      const name = (item.name ? String(item.name) : "").trim();
      if (name) names.push(name);
    }
  }
  return [...new Set(names)];
}

function chatMsgSummary(message, text, turn) {
  const role = (message && message.role ? String(message.role) : "").trim();
  const toolNames = extractToolCallNames(message, turn);
  if (role === "assistant" && toolNames.length > 0) {
    return `TOOLS: ${toolNames.join(", ")}`;
  }
  const first = text.split("\n").find((l) => l.trim().length > 0) || "";
  if (role === "assistant" && !first) return "NO ASSISTANT TEXT";
  return first.length > 120 ? first.slice(0, 120) + "\u2026" : first;
}

function ChatMessages({ messages, onOpenMessage, agentID, turns }) {
  if (!messages || messages.length === 0) {
    return <div className="empty-state">No messages recorded</div>;
  }
  // Build turn_index → created_at map from turns (sorted newest-first from API)
  const turnTimeMap = new Map();
  const turnMap = new Map();
  if (turns) {
    for (const t of turns) {
      if (t.turn_index != null && t.created_at) {
        turnTimeMap.set(t.turn_index, t.created_at);
        turnMap.set(t.turn_index, t);
      }
    }
  }
  // Track turn index: each user message starts a new turn
  let turnIdx = -1;
  return (
    <div className="chat-messages">
      {messages.map((m, i) => {
        const role = m.role || "unknown";
        if (role === "user") turnIdx++;
        const label = agentID
          ? (role === "assistant" ? agentID : role === "user" ? "orchestrator" : role)
          : role;
        const text =
          typeof m.content === "string"
            ? m.content
            : Array.isArray(m.content)
              ? m.content.map((c) => c.text || c.type || "").join("\n")
              : JSON.stringify(m.content, null, 2);
        const turn = turnMap.get(turnIdx) || null;
        const summary = chatMsgSummary(m, text, turn);
        const isNoOp = summary === "NO ASSISTANT TEXT";
        const ts = turnTimeMap.get(turnIdx);
        return (
          <details key={i} className={`chat-msg chat-${role}${isNoOp ? " chat-noop" : ""}`}>
            <summary className="chat-summary">
              <span className="chat-role">{label}</span>
              <span className="chat-summary-text">{summary}</span>
              {ts ? <span className="chat-time" title={fmtTime(ts)}>{relTime(ts)}</span> : null}
            </summary>
            <pre className="chat-content" onClick={() => onOpenMessage && onOpenMessage({ role, text })}>{text}</pre>
          </details>
        );
      })}
    </div>
  );
}

/* ---- Agent table ---- */

function AgentTable({ agents, selectedAgentID, onSelectAgent, renderDropdown, onNavigateTask }) {
  if (!agents || agents.length === 0) {
    return <div className="empty-state">No agents in this group.</div>;
  }
  // Sort: stuck first, then running, then idle, then terminated
  const stateOrder = { stuck: 0, running: 1, idle: 2, terminated: 3 };
  const sorted = [...agents].sort((a, b) => (stateOrder[a.state] ?? 9) - (stateOrder[b.state] ?? 9) || (a.id || "").localeCompare(b.id || ""));
  return (
    <table>
      <thead>
        <tr>
          <th>Agent</th>
          <th>State</th>
          <th>Task</th>
          <th>Turns</th>
          <th>Tokens 24h</th>
          <th>Pending</th>
          <th>Last Tool</th>
        </tr>
      </thead>
      <tbody>
        {sorted.map((a) => {
          const pct = a.turn_limit > 0 ? Math.min(100, Math.round((a.turn_count / a.turn_limit) * 100)) : 0;
          const fillClass = pct >= 95 ? "turnfill bad" : pct >= 85 ? "turnfill warn" : "turnfill";
          const tool = a.last_tool && a.last_tool.name ? `${a.last_tool.name}${a.last_tool.ok === false ? " (fail)" : ""}` : "-";
          const active = selectedAgentID === a.id;
          return (
            <React.Fragment key={a.id}>
              <tr className={`agent-row ${active ? "active" : ""}`} onClick={() => onSelectAgent(active ? "" : a.id)}>
                <td>
                  <div><strong>{a.id}</strong></div>
                  <div className="tiny">{a.role || "-"}</div>
                </td>
                <td>
                  <span className={`badge b-${a.state}`}><StatusDot state={a.state} />{a.state}</span>
                  {a.stuck_reason ? <div className="tiny" style={{ color: "#f87171" }}>{a.stuck_reason}</div> : null}
                </td>
                <td>{a.current_task_id ? <span className="copy-id mono" style={{ cursor: "pointer", color: "var(--info)" }} onClick={(e) => { e.stopPropagation(); if (onNavigateTask) onNavigateTask(a.current_task_id); }} title="Open in Tasks tab">{a.current_task_id.slice(0, 10)}</span> : <span className="mono">-</span>}</td>
                <td>
                  <div className="turnbar"><div className={fillClass} style={{ width: `${pct}%` }} /></div>
                  <div className="tiny">{a.turn_count}/{a.turn_limit} <span className="mono">({a.turns_24h || 0} in 24h)</span></div>
                </td>
                <td className="mono">{(a.total_tokens_24h || 0).toLocaleString()}</td>
                <td>{a.pending_events || 0}</td>
                <td>
                  <div>{tool}</div>
                  <div className="tiny">{(a.last_tool && a.last_tool.result) || "-"}</div>
                </td>
              </tr>
              {active ? (
                <tr>
                  <td className="agent-drop-cell" colSpan={7}>
                    {renderDropdown ? renderDropdown(a) : null}
                  </td>
                </tr>
              ) : null}
            </React.Fragment>
          );
        })}
      </tbody>
    </table>
  );
}

/* ---- Agent dropdown (self-contained per-agent state) ---- */

function AgentDropdown({ agent, addToast, onNavigate, onAction }) {
  const [chatMode, setChatMode] = useState("live");
  const [chatMessage, setChatMessage] = useState("");
  const [directiveMessage, setDirectiveMessage] = useState("");
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
      .then((d) => { setPromptState(d); setPromptEdit(d.effective_prompt || ""); })
      .catch(() => {});
  }, [agent.id]);

  function reloadTurns() {
    fetchJSON(`/dashboard/api/conversations/${encodeURIComponent(agent.id)}`)
      .then((d) => setTurns(d.turns || []))
      .catch(() => {});
  }

  function reloadPrompt() {
    fetchJSON(`/api/agents/${encodeURIComponent(agent.id)}/prompt`)
      .then((d) => { setPromptState(d); setPromptEdit(d.effective_prompt || ""); })
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

  return (
    <div className="agent-drop">
      <div className="agent-drop-grid">
        <div>
          <div className="agent-kv tiny"><strong>Agent</strong><span className="mono">{agent.id}</span></div>
          <div className="agent-kv tiny"><strong>Role</strong>{agent.role || "-"}</div>
          <div className="agent-kv tiny"><strong>Vertical</strong>{agent.vertical_slug || agent.vertical_id || "holding"}</div>
          <div className="agent-kv tiny"><strong>Created</strong><span title={fmtTime(agent.started_at)}>{relTime(agent.started_at)}</span></div>
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
                    .then((d) => { setDiffData(d); setShowDiff(true); })
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
          <textarea value={directiveMessage} onChange={(e) => setDirectiveMessage(e.target.value)} placeholder="Give this agent a direct directive..." />
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
            <button className="btn-secondary" disabled={!!busy} onClick={() => {
              if (!window.confirm(`Restart agent "${agent.id}"? This will interrupt any in-progress work.`)) return;
              exec("restart", () => postJSON("/dashboard/api/control/agents/restart", { agent_id: agent.id }));
            }}>{busy === "restart" ? "\u2026" : "Restart"}</button>
            <button className="btn-secondary" disabled={!!busy} onClick={() => {
              if (!window.confirm(`Replay backlog for "${agent.id}"?`)) return;
              exec("replay", () => postJSON("/dashboard/api/control/agents/replay", { agent_id: agent.id }));
            }}>{busy === "replay" ? "\u2026" : "Replay"}</button>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ---- Graph helpers (unchanged) ---- */

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

function isEventLinkedEdgeKind(kind) {
  return kind === "routing" || kind === "subscription" || kind === "producer";
}

function roleKeyFromAgentID(id, role) {
  if (role) return role;
  const s = (id || "").trim();
  const idx = s.lastIndexOf("-");
  if (idx > 0) return s.slice(0, idx);
  return s;
}

function forceDirectedLayout(nodes, edges, cx, cy) {
  /* Simple force-directed layout (Fruchterman-Reingold style)
     Works well for small dense graphs (< 30 nodes) */
  const N = nodes.length;
  if (N === 0) return new Map();

  const AREA = 800;
  const K = Math.sqrt((AREA * AREA) / N) * 1.6; /* ideal spring length */
  const ITERS = 120;
  const COOL = 0.97;

  /* Init positions in a circle so nothing starts overlapping */
  const p = new Map();
  nodes.forEach((n, i) => {
    const angle = (2 * Math.PI * i) / N;
    const r = K * 1.2;
    p.set(n.id, { x: cx + r * Math.cos(angle), y: cy + r * Math.sin(angle) });
  });

  /* Build adjacency for attraction */
  const adj = new Map();
  const nodeSet = new Set(nodes.map((n) => n.id));
  for (const e of edges) {
    if (!nodeSet.has(e.from) || !nodeSet.has(e.to)) continue;
    if (!adj.has(e.from)) adj.set(e.from, new Set());
    if (!adj.has(e.to)) adj.set(e.to, new Set());
    adj.get(e.from).add(e.to);
    adj.get(e.to).add(e.from);
  }

  let temp = K * 0.6;
  for (let iter = 0; iter < ITERS; iter++) {
    const disp = new Map();
    for (const n of nodes) disp.set(n.id, { dx: 0, dy: 0 });

    /* Repulsive forces between all pairs */
    for (let i = 0; i < N; i++) {
      for (let j = i + 1; j < N; j++) {
        const a = nodes[i].id, b = nodes[j].id;
        const pa = p.get(a), pb = p.get(b);
        let dx = pa.x - pb.x, dy = pa.y - pb.y;
        const dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
        const force = (K * K) / dist;
        const fx = (dx / dist) * force;
        const fy = (dy / dist) * force;
        const da = disp.get(a), db = disp.get(b);
        da.dx += fx; da.dy += fy;
        db.dx -= fx; db.dy -= fy;
      }
    }

    /* Attractive forces along edges */
    for (const e of edges) {
      if (!nodeSet.has(e.from) || !nodeSet.has(e.to)) continue;
      const pa = p.get(e.from), pb = p.get(e.to);
      let dx = pa.x - pb.x, dy = pa.y - pb.y;
      const dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
      const force = (dist * dist) / K;
      const fx = (dx / dist) * force;
      const fy = (dy / dist) * force;
      const da = disp.get(e.from), db = disp.get(e.to);
      da.dx -= fx; da.dy -= fy;
      db.dx += fx; db.dy += fy;
    }

    /* Apply displacements clamped by temperature */
    for (const n of nodes) {
      const d = disp.get(n.id);
      const mag = Math.sqrt(d.dx * d.dx + d.dy * d.dy) || 0.01;
      const clampedMag = Math.min(mag, temp);
      const pp = p.get(n.id);
      pp.x += (d.dx / mag) * clampedMag;
      pp.y += (d.dy / mag) * clampedMag;
    }

    temp *= COOL;
  }

  return p;
}

function buildGraphLayout(graph, direction) {
  const nodes = (graph && graph.nodes) || [];
  const edges = (graph && graph.edges) || [];
  const dir = direction || "LR";

  const byID = new Map();
  for (const n of nodes) byID.set(n.id, n);

  if (nodes.length === 0) {
    return { nodes: [], edges: [], pos: new Map(), bounds: { minX: 0, minY: 0, maxX: 1200, maxY: 700 }, byID };
  }

  const agents = nodes.filter((n) => n.kind === "agent");
  const events = nodes.filter((n) => n.kind === "event");
  const nonEvents = nodes.filter((n) => n.kind !== "event");

  const pos = new Map();

  /* ============================================================
     Choose layout strategy based on graph shape
     ============================================================ */

  if (events.length === 0 || (nonEvents.length > 0 && events.length <= nonEvents.length * 0.3)) {
    /* FORCE-DIRECTED — for agent-only / direct-link graphs */
    const fp = forceDirectedLayout(nodes, edges, 400, 300);
    for (const [id, p] of fp) pos.set(id, p);
  } else if (events.length > 0) {
    /* BIPARTITE — events on left, agents on right */
    const agentSubCount = new Map();
    const eventToAgents = new Map();
    for (const e of edges) {
      if (!isEventLinkedEdgeKind(e.kind)) continue;
      const fromNode = byID.get(e.from);
      const toNode = byID.get(e.to);
      if (fromNode && fromNode.kind === "event" && toNode && toNode.kind === "agent" && e.kind !== "producer") {
        agentSubCount.set(e.to, (agentSubCount.get(e.to) || 0) + 1);
        if (!eventToAgents.has(e.from)) eventToAgents.set(e.from, []);
        eventToAgents.get(e.from).push(e.to);
      }
    }

    const AGENT_GAP = 140;
    const EVENT_ROW_GAP = 28;
    const sortedAgents = [...agents].sort((a, b) => (agentSubCount.get(b.id) || 0) - (agentSubCount.get(a.id) || 0));

    const agentEventGroups = new Map();
    const unassignedEvents = [];
    for (const ev of events) {
      const targets = eventToAgents.get(ev.id) || [];
      if (targets.length > 0) {
        const primary = [...targets].sort((a, b) => (agentSubCount.get(b) || 0) - (agentSubCount.get(a) || 0))[0];
        if (!agentEventGroups.has(primary)) agentEventGroups.set(primary, []);
        agentEventGroups.get(primary).push(ev);
      } else {
        unassignedEvents.push(ev);
      }
    }

    if (dir === "LR") {
      let agentY = 60, eventY = 60;
      for (const agent of sortedAgents) {
        const groupEvents = (agentEventGroups.get(agent.id) || []).sort((a, b) => (a.label || "").localeCompare(b.label || ""));
        const groupStartY = eventY;
        for (const ev of groupEvents) { pos.set(ev.id, { x: 40, y: eventY }); eventY += EVENT_ROW_GAP; }
        if (groupEvents.length > 0) eventY += 12;
        const groupCenterY = groupEvents.length > 0 ? groupStartY + ((groupEvents.length - 1) * EVENT_ROW_GAP) / 2 : agentY;
        const finalAgentY = Math.max(agentY, groupCenterY);
        pos.set(agent.id, { x: 700, y: finalAgentY });
        agentY = finalAgentY + AGENT_GAP;
      }
      for (const ev of unassignedEvents) { pos.set(ev.id, { x: 40, y: eventY }); eventY += EVENT_ROW_GAP; }
    } else {
      let agentX = 60, eventX = 60;
      for (const agent of sortedAgents) {
        const groupEvents = (agentEventGroups.get(agent.id) || []).sort((a, b) => (a.label || "").localeCompare(b.label || ""));
        const groupStartX = eventX;
        for (const ev of groupEvents) { pos.set(ev.id, { x: eventX, y: 40 }); eventX += 200; }
        if (groupEvents.length > 0) eventX += 20;
        const groupCenterX = groupEvents.length > 0 ? groupStartX + ((groupEvents.length - 1) * 200) / 2 : agentX;
        const finalAgentX = Math.max(agentX, groupCenterX);
        pos.set(agent.id, { x: finalAgentX, y: 500 });
        agentX = finalAgentX + 280;
      }
      for (const ev of unassignedEvents) { pos.set(ev.id, { x: eventX, y: 40 }); eventX += 200; }
    }

    /* Position non-agent/non-event nodes (system, mailbox, human) */
    const others = nodes.filter((n) => n.kind !== "agent" && n.kind !== "event");
    others.forEach((n, i) => {
      if (!pos.has(n.id)) pos.set(n.id, { x: 40, y: 40 + i * 80 });
    });
  }

  /* --- Bounds --- */
  let minX = 1e9, minY = 1e9, maxX = -1e9, maxY = -1e9;
  for (const n of nodes) {
    const p = pos.get(n.id) || { x: 0, y: 0 };
    minX = Math.min(minX, p.x); minY = Math.min(minY, p.y);
    maxX = Math.max(maxX, p.x); maxY = Math.max(maxY, p.y);
  }
  if (!Number.isFinite(minX)) { minX = 0; minY = 0; maxX = 1200; maxY = 700; }

  const usedForce = events.length === 0 || (nonEvents.length > 0 && events.length <= nonEvents.length * 0.3);

  return {
    nodes,
    edges,
    pos,
    bounds: { minX: minX - 180, minY: minY - 120, maxX: maxX + 260, maxY: maxY + 160 },
    byID,
    forceLayout: usedForce,
  };
}

function deriveGraphForView(graph, opts) {
  let g = graph || { nodes: [], edges: [] };
  const collapseEvents = !!(opts && opts.collapseEvents);
  const hideOrphans = !!(opts && opts.hideOrphans);
  const stageFilter = (opts && opts.stageFilter) ? String(opts.stageFilter) : "all";
  const rubricFilter = (opts && opts.rubricFilter) ? String(opts.rubricFilter) : "all";

  if (stageFilter !== "all" || rubricFilter !== "all") {
    const edgePass = (e) => {
      const stages = Array.isArray(e && e.stages) ? e.stages : [];
      const rubrics = Array.isArray(e && e.rubrics) ? e.rubrics : [];
      if (stageFilter !== "all" && stages.length > 0 && !stages.includes(stageFilter)) return false;
      if (rubricFilter !== "all" && rubrics.length > 0 && !rubrics.includes(rubricFilter)) return false;
      return true;
    };
    const nextEdges = (g.edges || []).filter(edgePass);
    const keepNodes = new Set();
    for (const e of nextEdges) {
      keepNodes.add(e.from);
      keepNodes.add(e.to);
    }
    const nextNodes = (g.nodes || []).filter((n) => {
      if (keepNodes.has(n.id)) return true;
      return n.kind === "human" || n.kind === "mailbox" || n.kind === "system";
    });
    g = { ...g, nodes: nextNodes, edges: nextEdges };
  }

  /* --- Hide idle events: NOT(has producer AND has subscriber) --- */
  if (hideOrphans && !collapseEvents) {
    const hasProducer = new Set();   /* events with a producer edge TO them (agent → event) */
    const hasSubscriber = new Set(); /* events with a subscription edge FROM them (event → agent) */
    const nodeKind = new Map();
    for (const n of g.nodes || []) nodeKind.set(n.id, n.kind);
    for (const e of g.edges || []) {
      if (e.kind === "producer" && nodeKind.get(e.to) === "event") hasProducer.add(e.to);
      if ((e.kind === "subscription" || e.kind === "routing") && nodeKind.get(e.from) === "event") hasSubscriber.add(e.from);
    }
    const filtered = (g.nodes || []).filter((n) =>
      n.kind !== "event" || (hasProducer.has(n.id) && hasSubscriber.has(n.id)),
    );
    if (filtered.length !== (g.nodes || []).length) {
      g = { ...g, nodes: filtered };
    }
  }

  if (!collapseEvents) return g;

  /* Build direct agent→agent edges through events:
     For each event, find producers (agent → event) and subscribers (event → agent),
     then create direct edges from each producer to each subscriber. */
  const byID = new Map();
  for (const n of g.nodes || []) byID.set(n.id, n);

  /* Map event ID → producer agent IDs */
  const eventProducers = new Map();
  /* Map event ID → subscriber agent IDs */
  const eventSubscribers = new Map();

  const passthrough = []; /* edges that don't involve events (management, message, mailbox) */

  for (const e of g.edges || []) {
    if (e.kind === "producer") {
      const toNode = byID.get(e.to);
      if (toNode && toNode.kind === "event") {
        if (!eventProducers.has(e.to)) eventProducers.set(e.to, []);
        eventProducers.get(e.to).push({ agent: e.from, edge: e });
      } else {
        passthrough.push(e);
      }
    } else if (e.kind === "routing" || e.kind === "subscription") {
      const fromNode = byID.get(e.from);
      if (fromNode && fromNode.kind === "event") {
        if (!eventSubscribers.has(e.from)) eventSubscribers.set(e.from, []);
        eventSubscribers.get(e.from).push({ agent: e.to, edge: e });
      } else {
        passthrough.push(e);
      }
    } else {
      passthrough.push(e);
    }
  }

  /* Create direct edges: producer agent → subscriber agent */
  const directEdges = [...passthrough];
  const seen = new Set();
  for (const [evtID, producers] of eventProducers) {
    const subscribers = eventSubscribers.get(evtID) || [];
    const evtNode = byID.get(evtID);
    const evtLabel = (evtNode && evtNode.label) || evtID.replace(/^evt:/, "");
    for (const prod of producers) {
      for (const sub of subscribers) {
        if (prod.agent === sub.agent) continue; /* skip self-loops */
        const key = `${prod.agent}->${sub.agent}:${evtLabel}`;
        if (seen.has(key)) continue;
        seen.add(key);
        directEdges.push({
          from: prod.agent,
          to: sub.agent,
          kind: "routing",
          label: evtLabel,
          event_type: (prod.edge && prod.edge.event_type) || (sub.edge && sub.edge.event_type) || evtLabel,
          schema_required: (prod.edge && prod.edge.schema_required) || (sub.edge && sub.edge.schema_required) || [],
          schema_properties: (prod.edge && prod.edge.schema_properties) || (sub.edge && sub.edge.schema_properties) || [],
          interceptor_handler: (prod.edge && prod.edge.interceptor_handler) || (sub.edge && sub.edge.interceptor_handler) || "",
          intercepted: !!((prod.edge && prod.edge.intercepted) || (sub.edge && sub.edge.intercepted)),
          passthrough: !!((prod.edge && prod.edge.passthrough) || (sub.edge && sub.edge.passthrough)),
          producers: [prod.agent],
          consumers: [sub.agent],
          stages: (prod.edge && prod.edge.stages) || (sub.edge && sub.edge.stages) || [],
          rubrics: (prod.edge && prod.edge.rubrics) || (sub.edge && sub.edge.rubrics) || [],
          status: "active",
          source: prod.edge.source || sub.edge.source || "",
        });
      }
    }
  }

  /* Keep only non-event nodes */
  const nodes = (g.nodes || []).filter((n) => n.kind !== "event");

  return { ...g, nodes, edges: directEdges };
}

function readSavedPositions(storageKey) {
  try {
    const raw = localStorage.getItem(storageKey) || "";
    if (!raw) return new Map();
    const obj = JSON.parse(raw);
    const m = new Map();
    for (const [id, p] of Object.entries(obj || {})) {
      if (!p || typeof p.x !== "number" || typeof p.y !== "number") continue;
      m.set(id, { x: p.x, y: p.y });
    }
    return m;
  } catch {
    return new Map();
  }
}

function writeSavedPositions(storageKey, nodes) {
  try {
    const obj = {};
    for (const n of nodes || []) {
      if (!n || !n.id || !n.position) continue;
      obj[n.id] = { x: n.position.x, y: n.position.y };
    }
    localStorage.setItem(storageKey, JSON.stringify(obj));
  } catch {}
}

function AgentNode({ data, selected }) {
  const role = data.role || roleKeyFromAgentID(data.id, "");
  const rt = data.runtime;
  const state = rt ? rt.state : null;
  const isStuck = state === "stuck";
  const isLR = data.layoutDir !== "TB";
  const centered = data.forceLayout;
  const fill =
    data.group === "holding"
      ? "var(--graph-holding)"
      : data.group === "template"
        ? "var(--graph-template)"
        : "var(--graph-opco)";

  return (
    <div className={`rf-node rf-agent ${selected ? "selected" : ""} ${isStuck ? "rf-stuck" : ""}`} style={{ background: fill }}>
      <Handle type="target" position={centered ? Position.Top : (isLR ? Position.Left : Position.Top)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <Handle type="source" position={centered ? Position.Bottom : (isLR ? Position.Right : Position.Bottom)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <div className="rf-top">
        <div className="rf-role">{state ? <StatusDot state={state} /> : null}{role}</div>
        <div className="rf-status">{data.status || "-"}</div>
      </div>
      <div className="rf-id mono">{data.id}</div>
      {rt ? (
        <div className="rf-stats">
          {rt.turn_limit > 0 ? (
            <div className="rf-mini-turn">
              <div className={`rf-mini-fill${Math.round((rt.turn_count / rt.turn_limit) * 100) >= 90 ? " warn" : ""}`} style={{ width: `${Math.min(100, Math.round((rt.turn_count / rt.turn_limit) * 100))}%` }} />
            </div>
          ) : null}
          <span className="rf-stat">{rt.turn_count || 0}/{rt.turn_limit || 0}t</span>
          <span className="rf-stat">{((rt.total_tokens_24h || 0) / 1000).toFixed(0)}k</span>
          {(rt.pending_events || 0) > 0 ? <span className="rf-pending">{rt.pending_events}</span> : null}
        </div>
      ) : null}
      {isStuck && rt.stuck_reason ? <div className="rf-stuck-reason" title={rt.stuck_reason}>{rt.stuck_reason}</div> : null}
    </div>
  );
}

function EventNode({ data, selected }) {
  const isLR = data.layoutDir !== "TB";
  const centered = data.forceLayout;
  return (
    <div className={`rf-node rf-event ${selected ? "selected" : ""}`} style={{ background: "var(--graph-event)" }}>
      <Handle type="target" position={centered ? Position.Top : (isLR ? Position.Left : Position.Top)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <Handle type="source" position={centered ? Position.Bottom : (isLR ? Position.Right : Position.Bottom)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <div className="rf-event-label mono">{data.label || data.id}</div>
      {data.subscriberCount > 0 ? <div className="rf-event-count">{data.subscriberCount} sub{data.subscriberCount !== 1 ? "s" : ""}</div> : null}
    </div>
  );
}

function ControlNode({ data, selected }) {
  const isLR = data.layoutDir !== "TB";
  const centered = data.forceLayout;
  return (
    <div className={`rf-node rf-control ${selected ? "selected" : ""}`}>
      <Handle type="target" position={centered ? Position.Top : (isLR ? Position.Left : Position.Top)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <Handle type="source" position={centered ? Position.Bottom : (isLR ? Position.Right : Position.Bottom)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <div className="rf-control-kind">{(data.kind || "system").toUpperCase()}</div>
      <div className="rf-control-label">{data.label || data.id}</div>
      <div className="rf-id mono">{data.id}</div>
    </div>
  );
}

/* ---------- Custom edge: straight line clipped to node boundaries ---------- */
function clipLineToRect(x1, y1, x2, y2, rx, ry, rw, rh) {
  /* Find the intersection of the line (x1,y1)->(x2,y2) with the rectangle
     centered at (rx + rw/2, ry + rh/2) with half-sizes (rw/2, rh/2).
     Returns the point on the rect boundary closest to (x1,y1). */
  const cx = rx + rw / 2, cy = ry + rh / 2;
  const dx = x2 - x1, dy = y2 - y1;
  if (dx === 0 && dy === 0) return { x: cx, y: cy };
  const hw = rw / 2 + 2, hh = rh / 2 + 2; /* +2px padding so arrow doesn't touch border */
  let tMin = Infinity;
  /* check 4 sides */
  const sides = [
    { nx: -1, ny: 0, d: -hw }, /* left */
    { nx: 1, ny: 0, d: -hw },  /* right */
    { nx: 0, ny: -1, d: -hh }, /* top */
    { nx: 0, ny: 1, d: -hh },  /* bottom */
  ];
  for (const s of sides) {
    const edgeX = cx + s.nx * hw, edgeY = cy + s.ny * hh;
    const denom = s.nx !== 0 ? dx : dy;
    if (denom === 0) continue;
    const t = s.nx !== 0 ? (edgeX - x1) / dx : (edgeY - y1) / dy;
    if (t < 0 || t > 1) continue;
    const ix = x1 + dx * t, iy = y1 + dy * t;
    if (Math.abs(ix - cx) <= hw + 0.5 && Math.abs(iy - cy) <= hh + 0.5) {
      if (t < tMin) tMin = t;
    }
  }
  if (!Number.isFinite(tMin)) return { x: cx, y: cy };
  return { x: x1 + dx * tMin, y: y1 + dy * tMin };
}

function StraightClippedEdge({ id, source, target, style, markerEnd }) {
  const sourceNode = useInternalNode(source);
  const targetNode = useInternalNode(target);
  if (!sourceNode || !targetNode) return null;

  const sp = sourceNode.internals.positionAbsolute || sourceNode.position;
  const tp = targetNode.internals.positionAbsolute || targetNode.position;
  const sw = sourceNode.measured?.width || sourceNode.width || 210;
  const sh = sourceNode.measured?.height || sourceNode.height || 60;
  const tw = targetNode.measured?.width || targetNode.width || 210;
  const th = targetNode.measured?.height || targetNode.height || 60;

  const sx = sp.x + sw / 2, sy = sp.y + sh / 2;
  const tx = tp.x + tw / 2, ty = tp.y + th / 2;

  /* clip from source center toward target center — gives us the exit point on source border */
  const s = clipLineToRect(sx, sy, tx, ty, sp.x, sp.y, sw, sh);
  /* clip from target center toward source center — gives us the entry point on target border */
  const t = clipLineToRect(tx, ty, sx, sy, tp.x, tp.y, tw, th);

  const path = `M ${s.x} ${s.y} L ${t.x} ${t.y}`;
  return <BaseEdge id={id} path={path} style={style} markerEnd={markerEnd} />;
}

function GraphFlowToolbar({ collapseEvents, setCollapseEvents, hideOrphans, setHideOrphans, q, setQ, graphKey, onResetLayout, nodeCount, edgeCount, stuckCount, layoutDir, setLayoutDir, isFullscreen, onToggleFullscreen }) {
  const rf = useReactFlow();
  return (
    <Panel position="top-left" className="rf-panel tiny">
      <div className="graph-toolbar">
        <input className="mono" style={{ width: 180 }} placeholder="search node..." value={q} onChange={(e) => setQ(e.target.value)} />
        <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
          <input type="checkbox" checked={collapseEvents} onChange={(e) => setCollapseEvents(e.target.checked)} />
          direct links
        </label>
        <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
          <input type="checkbox" checked={hideOrphans} onChange={(e) => setHideOrphans(e.target.checked)} />
          hide idle
        </label>
        <div className="graph-dir-toggle">
          <button className={`graph-dir-btn ${layoutDir === "LR" ? "active" : ""}`} onClick={() => setLayoutDir("LR")} title="Left to Right">&rarr;</button>
          <button className={`graph-dir-btn ${layoutDir === "TB" ? "active" : ""}`} onClick={() => setLayoutDir("TB")} title="Top to Bottom">&darr;</button>
        </div>
        <button className="btn-secondary" onClick={() => rf.zoomIn({ duration: 180 })}>+</button>
        <button className="btn-secondary" onClick={() => rf.zoomOut({ duration: 180 })}>-</button>
        <button className="btn-secondary" onClick={() => rf.fitView({ padding: 0.18, duration: 220 })}>Fit</button>
        <button className="btn-secondary" onClick={onResetLayout}>Reset</button>
        <button className={`btn-secondary graph-fullscreen-btn ${isFullscreen ? "active" : ""}`} onClick={onToggleFullscreen} title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}>{isFullscreen ? "\u2716" : "\u26F6"}</button>
        <span className="graph-stats mono">
          <span>{nodeCount} nodes</span>
          <span className="graph-stats-sep">/</span>
          <span>{edgeCount} edges</span>
          {stuckCount > 0 ? <span className="graph-stats-stuck">{stuckCount} stuck</span> : null}
        </span>
      </div>
    </Panel>
  );
}

function GraphLegendPanel() {
  return (
    <Panel position="bottom-left" className="rf-panel tiny">
      <div className="graph-legend tiny" style={{ position: "static" }}>
        <span className="legend-chip holding">Holding</span>
        <span className="legend-chip template">Template</span>
        <span className="legend-chip opco">OpCo</span>
        <span className="legend-chip event">Event</span>
        <span className="legend-line mgmt">management</span>
        <span className="legend-line bootstrap">bootstrap</span>
        <span className="legend-line seeded">seeded</span>
        <span className="legend-line discovered">discovered</span>
        <span className="legend-line producer">producer</span>
        <span className="legend-line message">message</span>
        <span className="legend-line mailbox">mailbox</span>
      </div>
    </Panel>
  );
}

function GraphView({ graph, graphKey, selectedNodeID, selectedEdgeID, onSelectNode, onSelectEdge, onDerivedGraph, runtimeAgents, isFullscreen, onToggleFullscreen, activeEdgeKeys, stageFilter = "all", rubricFilter = "all" }) {
  const [collapseEvents, setCollapseEvents] = useState(true);
  const [hideOrphans, setHideOrphans] = useState(false);
  const [q, setQ] = useState("");
  const [layoutDir, setLayoutDir] = useState("LR");
  const [hoverNodeID, setHoverNodeID] = useState("");

  const derived = useMemo(() => deriveGraphForView(graph, { collapseEvents, hideOrphans, stageFilter, rubricFilter }), [graph, collapseEvents, hideOrphans, stageFilter, rubricFilter]);
  const layout = useMemo(() => buildGraphLayout(derived, layoutDir), [derived, layoutDir]);

  const agentRuntime = useMemo(() => {
    const m = new Map();
    for (const a of (runtimeAgents || [])) m.set(a.id, a);
    return m;
  }, [runtimeAgents]);

  const subscriberCounts = useMemo(() => {
    const m = new Map();
    for (const e of (derived.edges || [])) {
      if (isEventLinkedEdgeKind(e.kind) && e.kind !== "producer") {
        m.set(e.from, (m.get(e.from) || 0) + 1);
      }
    }
    return m;
  }, [derived]);

  useEffect(() => {
    if (onDerivedGraph) onDerivedGraph(derived);
  }, [derived, onDerivedGraph]);

  const storageKey = useMemo(
    () => `empire_graph_pos:${(graphKey || "graph")}:${layoutDir}${collapseEvents ? ":collapse" : ""}`,
    [graphKey, collapseEvents, layoutDir],
  );

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const nodesRef = useRef([]);

  useEffect(() => {
    nodesRef.current = nodes;
  }, [nodes]);

  function edgeStroke(e) {
    if (e.kind === "management") return "var(--edge-mgmt)";
    if (e.kind === "message") return "var(--edge-message)";
    if (e.kind === "mailbox") return "var(--edge-mailbox)";
    if (e.kind === "producer") return "var(--edge-producer)";
    if (e.source === "bootstrap") return "var(--edge-bootstrap)";
    if (e.source === "seeded") return "var(--edge-seeded)";
    if (e.source === "discovered") return "var(--edge-discovered)";
    return "var(--edge-routing)";
  }

  function edgeDash(e) {
    if (e.kind === "management") return "2 6";
    if (e.kind === "mailbox") return "7 4";
    if (e.kind === "producer") return "4 5";
    if (e.kind === "routing" || e.kind === "subscription") {
      if (e.source === "seeded") return "8 5";
      if (e.source === "discovered") return "2 6";
      return "";
    }
    if (e.status === "proposed") return "4 4";
    if (e.status === "deactivated") return "2 6";
    return "";
  }

  function nodeMatches(n, term) {
    const t = (term || "").trim().toLowerCase();
    if (!t) return true;
    const d = n && n.data ? n.data : n;
    const hay = `${d.id || ""} ${(d.label || "")} ${(d.role || "")}`.toLowerCase();
    return hay.includes(t);
  }

  useEffect(() => {
    const saved = readSavedPositions(storageKey);
    setNodes((prev) => {
      const prevPos = new Map();
      for (const n of prev || []) prevPos.set(n.id, n.position);

      return (layout.nodes || []).map((n) => {
        const base = layout.pos.get(n.id) || { x: 0, y: 0 };
        const p = saved.get(n.id) || prevPos.get(n.id) || base;
        const term = (q || "").trim();
        const fl = layout.forceLayout || false;
        const nodeData = n.kind === "agent"
          ? { ...n, runtime: agentRuntime.get(n.id) || null, layoutDir, forceLayout: fl }
          : { ...n, subscriberCount: subscriberCounts.get(n.id) || 0, layoutDir, forceLayout: fl };
        const nodeType = n.kind === "event" ? "event" : (n.kind === "mailbox" || n.kind === "human" || n.kind === "system" ? "control" : "agent");
        return {
          id: n.id,
          type: nodeType,
          position: { x: p.x, y: p.y },
          data: nodeData,
          draggable: true,
          selectable: true,
          hidden: term ? !nodeMatches(n, term) : false,
          selected: selectedNodeID === n.id,
        };
      });
    });

    setEdges(
      (layout.edges || []).map((e, i) => {
        const stroke = edgeStroke(e);
        const dash = edgeDash(e);
        const color = stroke;
        const fl = layout.forceLayout || false;
        const edgeID = `${e.kind}:${e.from}->${e.to}:${i}`;
        const edgeKey = `${e.from}->${e.to}|${e.label || ""}`;
        const hoverActive = !!hoverNodeID && (e.from === hoverNodeID || e.to === hoverNodeID);
        const isSelected = selectedEdgeID === edgeID;
        const isActive = !!(activeEdgeKeys && activeEdgeKeys.has(edgeKey)) || hoverActive || isSelected;
        return {
          id: edgeID,
          source: e.from,
          target: e.to,
          type: fl ? "straightClipped" : (e.kind === "management" ? "smoothstep" : "default"),
          animated: isActive,
          data: e,
          selected: isSelected,
          style: { stroke, strokeWidth: isActive ? 2.8 : 1.8, strokeDasharray: dash || undefined },
          markerEnd: { type: MarkerType.ArrowClosed, color },
        };
      }),
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [derived, storageKey, agentRuntime, subscriberCounts, layoutDir, activeEdgeKeys, selectedEdgeID, hoverNodeID]);

  useEffect(() => {
    setNodes((nds) => (nds || []).map((n) => ({ ...n, selected: n.id === selectedNodeID })));
  }, [selectedNodeID, setNodes]);

  useEffect(() => {
    const term = (q || "").trim();
    setNodes((nds) => (nds || []).map((n) => ({ ...n, hidden: term ? !nodeMatches(n, term) : false })));
  }, [q, setNodes]);

  useEffect(() => {
    const hidden = new Map();
    for (const n of nodes || []) hidden.set(n.id, !!n.hidden);
    setEdges((eds) => (eds || []).map((e) => ({ ...e, hidden: hidden.get(e.source) || hidden.get(e.target) })));
  }, [nodes, setEdges]);

  const persistPositions = useCallback(() => {
    writeSavedPositions(storageKey, nodesRef.current);
  }, [storageKey]);

  const onNodeDragStop = useCallback(() => {
    persistPositions();
  }, [persistPositions]);

  const resetLayout = useCallback(() => {
    try {
      localStorage.removeItem(storageKey);
    } catch {}
    setNodes((nds) =>
      (nds || []).map((n) => {
        const p = layout.pos.get(n.id) || { x: 0, y: 0 };
        return { ...n, position: { x: p.x, y: p.y } };
      }),
    );
  }, [layout.pos, setNodes, storageKey]);

  const nodeTypes = useMemo(() => ({ agent: AgentNode, event: EventNode, control: ControlNode }), []);
  const edgeTypes = useMemo(() => ({ straightClipped: StraightClippedEdge }), []);

  return (
    <div className={`graph-wrap ${isFullscreen ? "graph-fullscreen" : ""}`}>
      <div className="graph-flow">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeDragStop={onNodeDragStop}
          onNodeClick={(_, n) => {
            onSelectNode(n && n.id ? n.id : "");
            if (onSelectEdge) onSelectEdge("");
          }}
          onNodeMouseEnter={(_, n) => setHoverNodeID(n && n.id ? n.id : "")}
          onNodeMouseLeave={() => setHoverNodeID("")}
          onEdgeClick={(_, e) => {
            if (onSelectEdge) onSelectEdge(e && e.id ? e.id : "");
          }}
          onPaneClick={() => {
            onSelectNode("");
            if (onSelectEdge) onSelectEdge("");
          }}
          fitView
          fitViewOptions={{ padding: 0.18 }}
          proOptions={{ hideAttribution: true }}
        >
          <Background gap={22} size={1} color="rgba(255, 255, 255, 0.05)" />
          <MiniMap
            pannable
            zoomable
            className="rf-minimap"
            nodeColor={(n) => {
              const rt = n.data && n.data.runtime;
              if (!rt) return n.data && n.data.kind === "event" ? "rgba(255,255,255,0.12)" : "rgba(255,255,255,0.22)";
              if (rt.state === "stuck") return "#f87171";
              if (rt.state === "running") return "#34d399";
              return "rgba(255,255,255,0.18)";
            }}
          />
          <Controls position="bottom-right" />
          <GraphFlowToolbar
            collapseEvents={collapseEvents}
            setCollapseEvents={setCollapseEvents}
            hideOrphans={hideOrphans}
            setHideOrphans={setHideOrphans}
            q={q}
            setQ={setQ}
            graphKey={graphKey}
            onResetLayout={resetLayout}
            nodeCount={(nodes || []).filter((n) => !n.hidden).length}
            edgeCount={(edges || []).filter((e) => !e.hidden).length}
            stuckCount={(nodes || []).filter((n) => n.data && n.data.runtime && n.data.runtime.state === "stuck").length}
            layoutDir={layoutDir}
            setLayoutDir={(d) => { try { localStorage.removeItem(storageKey); } catch {} setLayoutDir(d); }}
            isFullscreen={isFullscreen}
            onToggleFullscreen={onToggleFullscreen}
          />
          <GraphLegendPanel />
        </ReactFlow>
      </div>
    </div>
  );
}

/* ============================================================
   App
   ============================================================ */

function App() {
  const [activeView, setActiveView] = useState(readHashTab);
  const [statusText, setStatusText] = useState("Loading...");
  const [apiKey, setApiKey] = useState(getEmpireKey());
  const [initialLoading, setInitialLoading] = useState(true);
  const [agentSearch, setAgentSearch] = useState("");
  const [selectedMailboxItem, setSelectedMailboxItem] = useState("");
  const [modalContent, setModalContent] = useState(null);

  const [overview, setOverview] = useState({});
  const [agentsResp, setAgentsResp] = useState({ agents: [], states: {} });
  const [digestResp, setDigestResp] = useState(null);
  const [taskStatus, setTaskStatus] = useState("open");
  const [tasksResp, setTasksResp] = useState({ tasks: [] });
  const [selectedTaskID, setSelectedTaskID] = useState("");
  const [taskResultText, setTaskResultText] = useState("");
  const [taskOutcome, setTaskOutcome] = useState("success");
  const [taskFollowUpNeeded, setTaskFollowUpNeeded] = useState(false);
  const [taskRejectReason, setTaskRejectReason] = useState("");
  const [tasksStats, setTasksStats] = useState(null);
  const [eventsFilter, setEventsFilter] = useState({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
  const [eventsIncludeRuntime, setEventsIncludeRuntime] = useState(true);
  const [eventsRuntimeErrorsOnly, setEventsRuntimeErrorsOnly] = useState(false);
  const [events, setEvents] = useState([]);
  const [runtimeLogs, setRuntimeLogs] = useState([]);
  const [selectedEventID, setSelectedEventID] = useState("");
  const [eventDetail, setEventDetail] = useState(null);
  const [conversations, setConversations] = useState([]);
  const [selectedConv, setSelectedConv] = useState("");
  const [conversationDetail, setConversationDetail] = useState({ messages: [], turns: [] });
  const [funnel, setFunnel] = useState({ throughput: {}, stuck: [] });
  const [shardScans, setShardScans] = useState([]);
  const [selectedShardScanID, setSelectedShardScanID] = useState("");
  const [shardScanDetails, setShardScanDetails] = useState({});
  const [traceVertical, setTraceVertical] = useState("");
  const [traceRows, setTraceRows] = useState([]);
  const [mailStatus, setMailStatus] = useState("all");
  const [mailbox, setMailbox] = useState({ summary: {}, items: [] });
  const [health, setHealth] = useState({});
  const [targets, setTargets] = useState([]);
  const [selectedAgentID, setSelectedAgentID] = useState("");

  const [logsFilter, setLogsFilter] = useState({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
  const [logsRuntimeErrorsOnly, setLogsRuntimeErrorsOnly] = useState(false);
  const [logsData, setLogsData] = useState([]);
  const [selectedLogID, setSelectedLogID] = useState(null);
  const [logsOrder, setLogsOrder] = useState("desc");
  const [incidentsFilter, setIncidentsFilter] = useState({ sinceHours: 24, mcpOnly: true, level: "warn", component: "" });
  const [incidentsData, setIncidentsData] = useState([]);
  const [selectedIncidentCode, setSelectedIncidentCode] = useState("");
  const [selectedIncidentAgent, setSelectedIncidentAgent] = useState("");
  const [incidentLogs, setIncidentLogs] = useState([]);
  const [incidentArtifacts, setIncidentArtifacts] = useState({ loading: false, error: "", data: null });

  const [controlTarget, setControlTarget] = useState("");
  const [directiveMessage, setDirectiveMessage] = useState("");
  const [chatMode, setChatMode] = useState("live");
  const [chatMessage, setChatMessage] = useState("");
  const [verticalName, setVerticalName] = useState("");
  const [verticalGeo, setVerticalGeo] = useState("");
  const [verticalSlug, setVerticalSlug] = useState("");
  const [requeueEventID, setRequeueEventID] = useState("");
  const [requeueAgentID, setRequeueAgentID] = useState("");
  const [mailboxID, setMailboxID] = useState("");
  const [mailboxAction, setMailboxAction] = useState("approve");
  const [mailboxNotes, setMailboxNotes] = useState("");
  const [resetConfirm, setResetConfirm] = useState("");
  const [controlOutput, setControlOutput] = useState({ ok: true, message: "Ready." });

  const [verticals, setVerticals] = useState([]);
  const [graphMode, setGraphMode] = useState("holding");
  const [graphVertical, setGraphVertical] = useState("");
  const [graph, setGraph] = useState({ nodes: [], edges: [] });
  const [graphViewGraph, setGraphViewGraph] = useState(null);
  const [selectedGraphNodeID, setSelectedGraphNodeID] = useState("");
  const [selectedGraphEdgeID, setSelectedGraphEdgeID] = useState("");
  const [graphFullscreen, setGraphFullscreen] = useState(false);
  const [flowView, setFlowView] = useState("design");
  const [flowVertical, setFlowVertical] = useState("");
  const [flowGraph, setFlowGraph] = useState({ nodes: [], edges: [] });
  const [flowGraphMeta, setFlowGraphMeta] = useState({});
  const [flowEvents, setFlowEvents] = useState([]);
  const [selectedFlowNodeID, setSelectedFlowNodeID] = useState("");
  const [selectedFlowEdgeID, setSelectedFlowEdgeID] = useState("");
  const [flowViewGraph, setFlowViewGraph] = useState(null);
  const [flowStart, setFlowStart] = useState("");
  const [flowEnd, setFlowEnd] = useState("");
  const [flowStage, setFlowStage] = useState("all");
  const [flowRubric, setFlowRubric] = useState("all");
  const [flowReplaySpeed, setFlowReplaySpeed] = useState(10);
  const [flowReplayOn, setFlowReplayOn] = useState(false);
  const [flowReplayIndex, setFlowReplayIndex] = useState(0);

  const [holdingData, setHoldingData] = useState({ campaigns: [], verticals: [], agent_counts: {}, summary: {} });
  const [holdingDetailModal, setHoldingDetailModal] = useState({
    open: false,
    loading: false,
    id: "",
    error: "",
    data: null,
  });

  /* ---- Toast notification system ---- */
  const [toasts, setToasts] = useState([]);
  const toastSeq = useRef(0);
  function addToast(msg, type) {
    const id = ++toastSeq.current;
    setToasts((prev) => [...prev, { id, msg, type: type || "info" }]);
    setTimeout(() => setToasts((prev) => prev.filter((t) => t.id !== id)), 4000);
  }

  /* ---- Navigation helper for AgentDropdown ---- */
  const handleAgentNavigate = useCallback((view, opts) => {
    if (opts && opts.eventID) setSelectedEventID(opts.eventID);
    if (opts && opts.convID) setSelectedConv(opts.convID);
    if (opts && opts.eventsSubscriber) {
      setEventsFilter({ type: "", source: "", vertical: "", component: "", level: "", subscriber: opts.eventsSubscriber });
      setEventsRuntimeErrorsOnly(false);
    }
    if (opts && opts.logsAgent) {
      setLogsFilter({ type: "", source: opts.logsAgent, vertical: "", component: "", level: "", subscriber: "" });
      setLogsRuntimeErrorsOnly(false);
    }
    setActiveView(view);
  }, []);

  /* ---- Data loaders ---- */

  const loadOverview = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/overview");
    setOverview(d || {});
    setStatusText(`Updated ${relTime(d.generated_at)}`);
  }, []);

  const loadAgents = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/agents");
    setAgentsResp({ agents: d.agents || [], states: d.states || {} });
  }, []);

  const loadTasks = useCallback(async () => {
    const p = new URLSearchParams();
    p.set("status", taskStatus || "open");
    p.set("limit", "250");
    const d = await fetchJSON(`/api/tasks?${p.toString()}`);
    setTasksResp({ tasks: d.tasks || [], weekly_budget: d.weekly_budget || {} });
  }, [taskStatus]);

  const loadEvents = useCallback(async () => {
    const p = new URLSearchParams();
    if (eventsFilter.type) p.set("type", eventsFilter.type);
    if (eventsFilter.source) p.set("source", eventsFilter.source);
    if (eventsFilter.vertical) p.set("vertical", eventsFilter.vertical);
    if (eventsFilter.subscriber) p.set("subscriber", eventsFilter.subscriber);
    p.set("limit", "200");
    const d = await fetchJSON(`/api/events?${p.toString()}`);
    setEvents(d.events || []);
  }, [eventsFilter]);

  const loadRuntimeLogs = useCallback(async () => {
    const p = new URLSearchParams();
    if (eventsFilter.type) p.set("type", eventsFilter.type);
    if (eventsFilter.subscriber) p.set("source", eventsFilter.subscriber);
    else if (eventsFilter.source) p.set("source", eventsFilter.source);
    if (eventsFilter.vertical) p.set("vertical", eventsFilter.vertical);
    if (eventsFilter.component) p.set("component", eventsFilter.component);
    if (eventsFilter.level) p.set("level", eventsFilter.level);
    else if (eventsRuntimeErrorsOnly) p.set("level", "error");
    p.set("limit", "200");
    const d = await fetchJSON(`/api/runtime/logs?${p.toString()}`);
    setRuntimeLogs(d.runtime_logs || []);
  }, [eventsFilter, eventsRuntimeErrorsOnly]);

  const loadLogs = useCallback(async () => {
    const p = new URLSearchParams();
    if (logsFilter.type) p.set("type", logsFilter.type);
    if (logsFilter.subscriber) p.set("source", logsFilter.subscriber);
    else if (logsFilter.source) p.set("source", logsFilter.source);
    if (logsFilter.vertical) p.set("vertical", logsFilter.vertical);
    if (logsFilter.component) p.set("component", logsFilter.component);
    if (logsFilter.level) p.set("level", logsFilter.level);
    else if (logsRuntimeErrorsOnly) p.set("level", "error");
    p.set("order", logsOrder);
    p.set("limit", "200");
    const d = await fetchJSON(`/api/runtime/logs?${p.toString()}`);
    setLogsData(d.runtime_logs || []);
  }, [logsFilter, logsOrder, logsRuntimeErrorsOnly]);

  const loadIncidents = useCallback(async () => {
    const p = new URLSearchParams();
    p.set("since_hours", String(Math.max(1, Number(incidentsFilter.sinceHours || 24))));
    p.set("mcp_only", incidentsFilter.mcpOnly ? "true" : "false");
    if (incidentsFilter.level) p.set("level", incidentsFilter.level);
    if (incidentsFilter.component) p.set("component", incidentsFilter.component);
    p.set("limit", "2000");
    const d = await fetchJSON(`/api/runtime/incidents?${p.toString()}`);
    const items = d.incidents || [];
    setIncidentsData(items);
    setSelectedIncidentCode((cur) => {
      if (!cur) return items.length > 0 ? items[0].code : "";
      const exists = items.some((it) => it.code === cur);
      return exists ? cur : (items.length > 0 ? items[0].code : "");
    });
  }, [incidentsFilter]);

  const loadIncidentLogs = useCallback(async (code) => {
    const c = (code || "").trim();
    if (!c) {
      setIncidentLogs([]);
      return;
    }
    const p = new URLSearchParams();
    p.set("error_code", c);
    p.set("order", "desc");
    p.set("limit", "250");
    const d = await fetchJSON(`/api/runtime/logs?${p.toString()}`);
    const rows = d.runtime_logs || [];
    setIncidentLogs(rows);
    setSelectedIncidentAgent((cur) => {
      if (cur && rows.some((r) => r.agent_id === cur)) return cur;
      const first = rows.find((r) => (r.agent_id || "").trim() !== "");
      return first ? first.agent_id : "";
    });
  }, []);

  const loadIncidentArtifacts = useCallback(async (agentID) => {
    const id = (agentID || "").trim();
    if (!id) {
      setIncidentArtifacts({ loading: false, error: "", data: null });
      return;
    }
    setIncidentArtifacts({ loading: true, error: "", data: null });
    try {
      const d = await fetchJSON(`/dashboard/api/conversations/${encodeURIComponent(id)}/artifacts?lines=120`);
      setIncidentArtifacts({ loading: false, error: "", data: d || null });
    } catch (err) {
      setIncidentArtifacts({
        loading: false,
        error: (err && err.message) ? err.message : "failed to load artifacts",
        data: null,
      });
    }
  }, []);

  const loadEventDetail = useCallback(async (id) => {
    if (!id) {
      setEventDetail(null);
      return;
    }
    const d = await fetchJSON(`/api/events/${encodeURIComponent(id)}`);
    setEventDetail(d);
  }, []);

  const loadConversations = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/conversations?limit=100");
    const items = d.conversations || [];
    setConversations(items);
    if (items.length > 0) {
      setSelectedConv((cur) => cur || items[0].agent_id);
    }
  }, []);

  const loadConversationDetail = useCallback(async (agentID) => {
    if (!agentID) {
      setConversationDetail({ messages: [], turns: [] });
      return;
    }
    const d = await fetchJSON(`/dashboard/api/conversations/${encodeURIComponent(agentID)}`);
    setConversationDetail({ messages: d.messages || [], turns: d.turns || [] });
  }, []);

  const loadFunnel = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/funnel");
    setFunnel({ throughput: d.throughput || {}, stuck: d.stuck || [] });
  }, []);

  const loadShardScans = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/pipeline/shards?limit=30");
    setShardScans(d.scans || []);
  }, []);

  const loadShardScanDetail = useCallback(async (scanID) => {
    const id = (scanID || "").trim();
    if (!id) return;
    const d = await fetchJSON(`/dashboard/api/pipeline/shards/${encodeURIComponent(id)}`);
    setShardScanDetails((prev) => ({ ...prev, [id]: d.shards || [] }));
  }, []);

  const shardAction = useCallback(async (scanID, shardID, action) => {
    const sid = (scanID || "").trim();
    const hid = (shardID || "").trim();
    if (!sid || !hid) return;
    await postJSON(`/api/pipeline/shards/${encodeURIComponent(hid)}/${encodeURIComponent(action)}`, {});
    addToast(`Shard ${action} queued`, "info");
    await Promise.all([loadShardScans(), loadShardScanDetail(sid)]);
  }, [loadShardScans, loadShardScanDetail]);

  const loadTrace = useCallback(async (vertical) => {
    if (!vertical) return;
    const d = await fetchJSON(`/dashboard/api/verticals/${encodeURIComponent(vertical)}/trace`);
    setTraceRows(d.trace || []);
  }, []);

  const loadMailbox = useCallback(async () => {
    const d = await fetchJSON(`/api/mailbox?status=${encodeURIComponent(mailStatus)}&limit=150`);
    setMailbox({ summary: d.summary || {}, items: d.items || [] });
  }, [mailStatus]);

  const loadDigest = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/digest?top=10");
    setDigestResp(d || null);
  }, []);

  const loadHealth = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/health");
    setHealth(d || {});
  }, []);

  const loadHolding = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/holding");
    setHoldingData({ campaigns: d.campaigns || [], verticals: d.verticals || [], agent_counts: d.agent_counts || {}, summary: d.summary || {} });
  }, []);

  const openHoldingVerticalDetail = useCallback(async (verticalID) => {
    const id = (verticalID || "").trim();
    if (!id) return;
    setHoldingDetailModal({ open: true, loading: true, id, error: "", data: null });
    try {
      const d = await fetchJSON(`/dashboard/api/holding/vertical?id=${encodeURIComponent(id)}`);
      setHoldingDetailModal({ open: true, loading: false, id, error: "", data: d || null });
    } catch (err) {
      setHoldingDetailModal({
        open: true,
        loading: false,
        id,
        error: (err && err.message) ? err.message : "failed to load vertical detail",
        data: null,
      });
    }
  }, []);

  const loadVerticals = useCallback(async () => {
    const d = await fetchJSON("/api/verticals");
    const items = d.verticals || [];
    setVerticals(items);
    if (!graphVertical && items.length > 0) {
      setGraphVertical(items[0].slug || items[0].id);
    }
    if (!flowVertical && items.length > 0) {
      setFlowVertical(items[0].slug || items[0].id);
    }
  }, [graphVertical, flowVertical]);

  const loadGraph = useCallback(async () => {
    const p = new URLSearchParams();
    p.set("mode", graphMode);
    if (graphMode === "opco") {
      if (!graphVertical) return;
      p.set("vertical", graphVertical);
    }
    const d = await fetchJSON(`/api/graph?${p.toString()}`);
    setGraph(d || { nodes: [], edges: [] });
    if (selectedGraphNodeID) {
      const exists = (d.nodes || []).some((n) => n.id === selectedGraphNodeID);
      if (!exists) setSelectedGraphNodeID("");
    }
    if (selectedGraphEdgeID) {
      const exists = (d.edges || []).some((e, i) => `${e.kind}:${e.from}->${e.to}:${i}` === selectedGraphEdgeID);
      if (!exists) setSelectedGraphEdgeID("");
    }
  }, [graphMode, graphVertical, selectedGraphNodeID, selectedGraphEdgeID]);

  const loadPipelineFlow = useCallback(async () => {
    const p = new URLSearchParams();
    p.set("view", flowView || "design");
    p.set("limit", "500");
    if (flowVertical && (flowView === "runtime" || flowView === "replay")) {
      p.set("vertical", flowVertical);
    }
    if (flowView === "replay") {
      if (flowStart) p.set("start", flowStart);
      if (flowEnd) p.set("end", flowEnd);
    }
    const d = await fetchJSON(`/api/pipeline/graph?${p.toString()}`);
    setFlowGraph({ nodes: d.nodes || [], edges: d.edges || [] });
    setFlowGraphMeta(d.meta || {});
    setFlowEvents(d.flow_events || []);
    setFlowReplayIndex(0);
    setFlowReplayOn(false);
    if (selectedFlowNodeID) {
      const exists = (d.nodes || []).some((n) => n.id === selectedFlowNodeID);
      if (!exists) setSelectedFlowNodeID("");
    }
    if (selectedFlowEdgeID) {
      const exists = (d.edges || []).some((e, i) => `${e.kind}:${e.from}->${e.to}:${i}` === selectedFlowEdgeID);
      if (!exists) setSelectedFlowEdgeID("");
    }
  }, [flowView, flowVertical, flowStart, flowEnd, selectedFlowNodeID, selectedFlowEdgeID]);

  const loadTargets = useCallback(async () => {
    const d = await fetchJSON("/dashboard/api/control/targets");
    const items = d.targets || [];
    setTargets(items);
    if (items.length > 0) {
      setControlTarget((cur) => cur || items[0].agent_id);
    }
  }, []);

  const refreshAll = useCallback(async () => {
    await Promise.all([
      loadOverview(),
      loadAgents(),
      loadDigest(),
      loadTasks(),
      loadEvents(),
      loadRuntimeLogs(),
      loadConversations(),
      loadTargets(),
      loadFunnel(),
      loadShardScans(),
      loadMailbox(),
      loadHealth(),
      loadVerticals(),
      loadHolding(),
      loadIncidents(),
    ]);
  }, [loadOverview, loadAgents, loadDigest, loadTasks, loadEvents, loadRuntimeLogs, loadConversations, loadTargets, loadFunnel, loadShardScans, loadMailbox, loadHealth, loadVerticals, loadHolding, loadIncidents]);

  /* ---- Escape exits graph fullscreen ---- */
  useEffect(() => {
    if (!graphFullscreen) return;
    function onKey(e) { if (e.key === "Escape") setGraphFullscreen(false); }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [graphFullscreen]);

  /* ---- URL hash routing ---- */
  useEffect(() => { location.hash = activeView; }, [activeView]);
  useEffect(() => {
    const handler = () => { const t = readHashTab(); setActiveView(t); };
    window.addEventListener("hashchange", handler);
    return () => window.removeEventListener("hashchange", handler);
  }, []);

  /* ---- Effects ---- */

  useEffect(() => {
    refreshAll()
      .catch((err) => { setStatusText(`Dashboard error: ${err.message}`); })
      .finally(() => setInitialLoading(false));
  }, [refreshAll]);

  useEffect(() => {
    if (activeView !== "graph") return;
    Promise.all([loadVerticals(), loadGraph()]).catch(() => {});
  }, [activeView, loadVerticals, loadGraph]);

  useEffect(() => {
    if (activeView !== "flow") return;
    Promise.all([loadVerticals(), loadPipelineFlow()]).catch(() => {});
  }, [activeView, loadVerticals, loadPipelineFlow]);

  useEffect(() => {
    if (activeView !== "logs") return;
    loadLogs().catch(() => {});
  }, [activeView, loadLogs]);

  useEffect(() => {
    if (activeView !== "incidents") return;
    loadIncidents().catch(() => {});
  }, [activeView, loadIncidents]);

  useEffect(() => {
    if (activeView !== "incidents") return;
    loadIncidentLogs(selectedIncidentCode).catch(() => {});
  }, [activeView, selectedIncidentCode, loadIncidentLogs]);

  useEffect(() => {
    if (activeView !== "incidents") return;
    loadIncidentArtifacts(selectedIncidentAgent).catch(() => {});
  }, [activeView, selectedIncidentAgent, loadIncidentArtifacts]);

  useEffect(() => {
    if (selectedConv) {
      loadConversationDetail(selectedConv).catch((err) => addToast(err.message, "error"));
    }
  }, [selectedConv, loadConversationDetail]);

  useEffect(() => {
    if (selectedEventID) {
      loadEventDetail(selectedEventID).catch((err) => addToast(err.message, "error"));
    }
  }, [selectedEventID, loadEventDetail]);

  useEffect(() => {
    if (!selectedShardScanID) return;
    const exists = (shardScans || []).some((s) => s.scan_id === selectedShardScanID);
    if (!exists) setSelectedShardScanID("");
  }, [selectedShardScanID, shardScans]);

  useEffect(() => {
    let stream = null;
    let retryTimer = null;
    let retryCount = 0;

    function connect() {
      const p = new URLSearchParams();
      if (eventsFilter.type) p.set("type", eventsFilter.type);
      if (eventsFilter.source) p.set("source", eventsFilter.source);
      if (eventsFilter.vertical) p.set("vertical", eventsFilter.vertical);
      if (eventsFilter.component) p.set("component", eventsFilter.component);
      if (eventsFilter.level) p.set("level", eventsFilter.level);
      else if (eventsRuntimeErrorsOnly) p.set("level", "error");
      if (eventsFilter.subscriber) p.set("subscriber", eventsFilter.subscriber);
      p.set("include_runtime", eventsIncludeRuntime ? "true" : "false");
      const key = getEmpireKey();
      if (key) p.set("key", key);
      stream = new EventSource(`/api/events?stream=true&${p.toString()}`);
      stream.addEventListener("event", () => {
        retryCount = 0;
        loadEvents().catch(() => {});
      });
      stream.addEventListener("runtime_log", () => {
        retryCount = 0;
        loadRuntimeLogs().catch(() => {});
      });
      stream.addEventListener("open", () => { retryCount = 0; });
      stream.onerror = () => {
        stream.close();
        retryCount++;
        const delay = Math.min(5000 * retryCount, 30000);
        addToast(`Event stream disconnected, reconnecting in ${Math.round(delay / 1000)}s\u2026`, "error");
        retryTimer = setTimeout(connect, delay);
      };
    }

    connect();
    return () => {
      if (stream) stream.close();
      if (retryTimer) clearTimeout(retryTimer);
    };
  }, [eventsFilter, eventsIncludeRuntime, eventsRuntimeErrorsOnly, loadEvents, loadRuntimeLogs]);

  useEffect(() => {
    if (activeView !== "flow" || flowView !== "runtime") return undefined;
    let stream = null;
    let retryTimer = null;
    let retryCount = 0;
    function connect() {
      const p = new URLSearchParams();
      p.set("stream", "true");
      p.set("limit", "200");
      if (flowVertical) p.set("vertical", flowVertical);
      const key = getEmpireKey();
      if (key) p.set("key", key);
      stream = new EventSource(`/api/events/flow?${p.toString()}`);
      stream.addEventListener("flow", (ev) => {
        retryCount = 0;
        try {
          const item = JSON.parse(ev.data || "{}");
          if (!item || !item.event_id) return;
          setFlowEvents((prev) => {
            const rows = [item, ...(prev || []).filter((x) => x.event_id !== item.event_id)];
            return rows.slice(0, 500);
          });
        } catch {}
      });
      stream.addEventListener("open", () => { retryCount = 0; });
      stream.onerror = () => {
        if (stream) stream.close();
        retryCount++;
        const delay = Math.min(5000 * retryCount, 30000);
        retryTimer = setTimeout(connect, delay);
      };
    }
    connect();
    return () => {
      if (stream) stream.close();
      if (retryTimer) clearTimeout(retryTimer);
    };
  }, [activeView, flowView, flowVertical]);

  useEffect(() => {
    if (flowView !== "replay" || !flowReplayOn) return undefined;
    const step = flowReplaySpeed >= 100 ? 10 : flowReplaySpeed >= 50 ? 5 : 1;
    const t = setInterval(() => {
      setFlowReplayIndex((idx) => {
        const next = Math.min((flowEvents || []).length, idx + step);
        if (next >= (flowEvents || []).length) {
          setFlowReplayOn(false);
        }
        return next;
      });
    }, 280);
    return () => clearInterval(t);
  }, [flowView, flowReplayOn, flowReplaySpeed, flowEvents]);

  // Polling — skip when tab is hidden
  useEffect(() => {
    const i1 = setInterval(() => {
      if (document.hidden) return;
      loadOverview().catch(() => {});
      loadAgents().catch(() => {});
      loadDigest().catch(() => {});
      loadTasks().catch(() => {});
      loadMailbox().catch(() => {});
      loadHealth().catch(() => {});
      loadRuntimeLogs().catch(() => {});
      loadIncidents().catch(() => {});
    }, 15000);
    const i2 = setInterval(() => {
      if (document.hidden) return;
      loadTargets().catch(() => {});
      loadFunnel().catch(() => {});
      loadVerticals().catch(() => {});
      loadHolding().catch(() => {});
      if (activeView === "flow" && flowView !== "runtime") {
        loadPipelineFlow().catch(() => {});
      }
    }, 22000);
    return () => {
      clearInterval(i1);
      clearInterval(i2);
    };
  }, [loadOverview, loadAgents, loadTasks, loadMailbox, loadHealth, loadRuntimeLogs, loadTargets, loadFunnel, loadVerticals, loadHolding, loadIncidents, activeView, flowView, loadPipelineFlow]);

  /* ---- Derived data ---- */

  const groupedAgents = useMemo(() => {
    const q = (agentSearch || "").trim().toLowerCase();
    const all = (agentsResp.agents || []).filter((a) => {
      if (!q) return true;
      return `${a.id} ${a.role || ""} ${a.state || ""} ${a.vertical_slug || ""}`.toLowerCase().includes(q);
    });
    const holding = [];
    const opco = new Map();
    for (const a of all) {
      const isHolding = !(a.vertical_slug || a.vertical_id) || a.mode !== "operating";
      if (isHolding) {
        holding.push(a);
      } else {
        const key = a.vertical_slug || a.vertical_id || "unknown";
        if (!opco.has(key)) opco.set(key, []);
        opco.get(key).push(a);
      }
    }
    const opcos = Array.from(opco.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([slug, agents]) => ({ slug, agents }));
    return { holding, opcos };
  }, [agentsResp.agents, agentSearch]);

  const filteredEvents = useMemo(
    () => (eventsRuntimeErrorsOnly ? (events || []).filter(hasEventError) : (events || [])),
    [events, eventsRuntimeErrorsOnly],
  );

  const filteredRuntimeLogs = useMemo(
    () => (eventsRuntimeErrorsOnly ? (runtimeLogs || []).filter(hasRuntimeError) : (runtimeLogs || [])),
    [runtimeLogs, eventsRuntimeErrorsOnly],
  );

  const filteredLogsData = useMemo(
    () => (logsRuntimeErrorsOnly ? (logsData || []).filter(hasRuntimeError) : (logsData || [])),
    [logsData, logsRuntimeErrorsOnly],
  );

  const selectedAgent = useMemo(
    () => (agentsResp.agents || []).find((a) => a.id === selectedAgentID) || null,
    [agentsResp.agents, selectedAgentID],
  );

  useEffect(() => {
    if (!selectedAgentID) return;
    if (!selectedAgent) setSelectedAgentID("");
  }, [selectedAgentID, selectedAgent]);

  useEffect(() => {
    if (!selectedLogID) return;
    const exists = (filteredLogsData || []).some((l) => l.id === selectedLogID);
    if (!exists) setSelectedLogID(null);
  }, [selectedLogID, filteredLogsData]);

  const selectedIncident = useMemo(
    () => (incidentsData || []).find((it) => it.code === selectedIncidentCode) || null,
    [incidentsData, selectedIncidentCode],
  );

  const flowStageOptions = useMemo(() => {
    const fromMeta = Array.isArray(flowGraphMeta && flowGraphMeta.stages) ? flowGraphMeta.stages : [];
    const merged = new Set(["all", ...FLOW_STAGE_OPTIONS, ...fromMeta]);
    return Array.from(merged);
  }, [flowGraphMeta]);

  const flowRubricOptions = useMemo(() => {
    const fromMeta = Array.isArray(flowGraphMeta && flowGraphMeta.rubrics) ? flowGraphMeta.rubrics : [];
    const merged = new Set(["all", ...FLOW_RUBRIC_OPTIONS, ...fromMeta]);
    return Array.from(merged);
  }, [flowGraphMeta]);

  const visibleFlowEvents = useMemo(() => {
    const rows = (flowEvents || []).filter((ev) => flowEventMatchesFilters(ev && ev.event_type, flowStage, flowRubric));
    if (flowView === "replay") {
      const n = Math.max(0, Math.min(rows.length, flowReplayIndex));
      return rows.slice(0, n);
    }
    return rows;
  }, [flowEvents, flowView, flowReplayIndex, flowStage, flowRubric]);

  const flowActiveEdgeKeys = useMemo(() => {
    const rows = visibleFlowEvents.slice(0, 150);
    const out = new Set();
    for (const ev of rows) {
      const source = (ev && ev.source_node ? String(ev.source_node) : "").trim();
      const eventType = (ev && ev.event_type ? String(ev.event_type) : "").trim();
      const targets = Array.isArray(ev && ev.target_nodes) ? ev.target_nodes : [];
      if (!source || !eventType) continue;
      for (const t of targets) {
        const target = String(t || "").trim();
        if (!target) continue;
        out.add(`${source}->${target}|${eventType}`);
      }
    }
    return out;
  }, [visibleFlowEvents]);

  useEffect(() => {
    if (!selectedGraphNodeID) return;
    const g = graphViewGraph || graph;
    const exists = ((g && g.nodes) || []).some((n) => n.id === selectedGraphNodeID);
    if (!exists) setSelectedGraphNodeID("");
  }, [selectedGraphNodeID, graph, graphViewGraph]);

  useEffect(() => {
    if (!selectedGraphEdgeID) return;
    const g = graphViewGraph || graph;
    const exists = ((g && g.edges) || []).some((e, i) => `${e.kind}:${e.from}->${e.to}:${i}` === selectedGraphEdgeID);
    if (!exists) setSelectedGraphEdgeID("");
  }, [selectedGraphEdgeID, graph, graphViewGraph]);

  useEffect(() => {
    if (!selectedFlowNodeID) return;
    const g = flowViewGraph || flowGraph;
    const exists = ((g && g.nodes) || []).some((n) => n.id === selectedFlowNodeID);
    if (!exists) setSelectedFlowNodeID("");
  }, [selectedFlowNodeID, flowGraph, flowViewGraph]);

  useEffect(() => {
    if (!selectedFlowEdgeID) return;
    const g = flowViewGraph || flowGraph;
    const exists = ((g && g.edges) || []).some((e, i) => `${e.kind}:${e.from}->${e.to}:${i}` === selectedFlowEdgeID);
    if (!exists) setSelectedFlowEdgeID("");
  }, [selectedFlowEdgeID, flowGraph, flowViewGraph]);

  function counts(agents) {
    return (agents || []).reduce((acc, a) => {
      const key = a.state || "idle";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
  }

  /* ---- Control actions ---- */

  async function runControl(fn) {
    try {
      const out = await fn();
      setControlOutput(out);
      addToast(out.message || "Action completed", "success");
      await Promise.all([loadAgents(), loadTasks(), loadEvents(), loadMailbox(), loadTargets(), loadOverview(), loadFunnel()]);
    } catch (err) {
      setControlOutput({ error: err.message });
      addToast(err.message, "error");
    }
  }

  async function quickMailboxDecide(id, action) {
    try {
      await postJSON(`/api/mailbox/${encodeURIComponent(id)}/decide`, { action, notes: "" });
      addToast(`${action}: ${(id || "").slice(0, 8)}`, "success");
      await loadMailbox();
    } catch (err) {
      addToast(err.message, "error");
    }
  }

  const selectedTask = useMemo(
    () => (tasksResp.tasks || []).find((t) => t.id === selectedTaskID) || null,
    [tasksResp.tasks, selectedTaskID],
  );

  async function claimSelectedTask() {
    if (!selectedTaskID) return;
    await runControl(() => postJSON(`/api/tasks/${encodeURIComponent(selectedTaskID)}/claim`, {}));
    await loadTasks();
  }

  async function completeSelectedTask() {
    if (!selectedTaskID) return;
    const body = {
      result_text: taskResultText.trim(),
      outcome: taskOutcome,
      follow_up_needed: !!taskFollowUpNeeded,
    };
    await runControl(() => postJSON(`/api/tasks/${encodeURIComponent(selectedTaskID)}/complete`, body));
    setTaskResultText("");
    setTaskOutcome("success");
    setTaskFollowUpNeeded(false);
    await loadTasks();
  }

  async function rejectSelectedTask() {
    if (!selectedTaskID) return;
    const body = { reason: (taskRejectReason || "").trim() };
    await runControl(() => postJSON(`/api/tasks/${encodeURIComponent(selectedTaskID)}/reject`, body));
    setTaskRejectReason("");
    await loadTasks();
  }

  async function loadTaskStats() {
    const d = await fetchJSON("/api/tasks/stats");
    setTasksStats(d || null);
  }

  /* ---- Navigation helpers ---- */

  function navigateToTask(taskID) {
    setSelectedTaskID(taskID);
    setActiveView("tasks");
  }

  /* ---- Agent dropdown renderer ---- */

  function renderAgentDropdown(agent) {
    return (
      <AgentDropdown
        agent={agent}
        addToast={addToast}
        onNavigate={handleAgentNavigate}
        onAction={() => { loadAgents().catch(() => {}); loadTargets().catch(() => {}); }}
      />
    );
  }

  const resetOK = (resetConfirm || "").trim() === "RESET";

  const holdingColumns = useMemo(() => {
    const cols = [
      { key: "discovery", label: "Discovery", stages: ["discovered"], items: [] },
      { key: "scoring", label: "Scoring", stages: ["scoring", "shortlisted", "marginal_review"], items: [] },
      { key: "validation", label: "Validation", stages: ["researching", "mvp_speccing", "spec_review", "cto_spec_review", "branding"], items: [] },
      { key: "mailbox", label: "Mailbox", stages: ["ready_for_review"], items: [] },
      { key: "approved", label: "Approved", stages: ["approved", "full_speccing", "building", "pre_launch", "launched", "operating", "expanding"], items: [] },
      { key: "killed", label: "Killed", stages: ["killed"], items: [] },
    ];
    const stageMap = {};
    for (const c of cols) for (const s of c.stages) stageMap[s] = c;
    for (const v of holdingData.verticals || []) {
      const col = stageMap[v.stage];
      if (col) col.items.push(v);
    }
    return cols;
  }, [holdingData.verticals]);

  /* ---- Tab definitions with badge counts ---- */

  const tabBadges = {
    agents: (agentsResp.states.stuck || 0) > 0 ? { n: agentsResp.states.stuck, type: "danger" } : null,
    control: (mailbox.summary.pending || 0) > 0 ? { n: mailbox.summary.pending, type: "warn" } : null,
    pipeline: (funnel.stuck || []).length > 0 ? { n: funnel.stuck.length, type: "warn" } : null,
    holding: (() => { const n = (holdingData.verticals || []).filter((v) => v.stage === "ready_for_review").length; return n > 0 ? { n, type: "warn" } : null; })(),
    incidents: (incidentsData || []).length > 0 ? { n: (incidentsData || []).length, type: "danger" } : null,
    flow: (flowEvents || []).length > 0 ? { n: Math.min((flowEvents || []).length, 999), type: "warn" } : null,
  };

  const tabs = [
    ["agents", "Agents"],
    ["digest", "Digest"],
    ["events", "Events"],
    ["logs", "Logs"],
    ["incidents", "Incidents"],
    ["flow", "Flow"],
    ["convos", "Convos"],
    ["graph", "Graph"],
    ["control", "Control + Mailbox"],
    ["tasks", "Tasks"],
    ["pipeline", "Pipeline"],
    ["holding", "Holding"],
    ["health", "Health"],
  ];

  return (
    <>
      {initialLoading ? (
        <div className="loading-overlay">
          <div className="loading-content">
            <div className="title">EMPIREAI</div>
            <div className="loading-spinner" />
            <div className="tiny">Connecting to command center\u2026</div>
          </div>
        </div>
      ) : null}
      <header>
        <div className="header-top">
          <div className="header-brand">
            <div className="title">EMPIREAI</div>
            <div className="sub">{statusText}</div>
            <LiveClock />
          </div>
          <div className="header-key">
            <span className="tiny">API Key</span>
            <input
              type="password"
              className="mono"
              placeholder="Enter key..."
              value={apiKey}
              onChange={(e) => {
                const v = e.target.value;
                setApiKey(v);
                try { localStorage.setItem("empire_api_key", v); } catch {}
              }}
            />
          </div>
        </div>
        <div className="overview">
          <div className="kpi"><div className="label">Agents Active</div><div className="value">{overview.agents_active || 0}</div></div>
          <div className="kpi"><div className="label">Events 24h</div><div className="value">{overview.events_24h || 0}</div></div>
          <div className="kpi"><div className="label">Mailbox Pending</div><div className="value">{overview.mailbox_pending || 0}</div></div>
          <div className="kpi"><div className="label">Verticals</div><div className="value">{overview.verticals_total || 0}</div></div>
          <div className={`kpi ${(agentsResp.states.stuck || 0) > 0 ? "kpi-alert" : ""}`}><div className="label">Stuck Agents</div><div className="value">{agentsResp.states.stuck || 0}</div></div>
        </div>
        <div className="view-nav">
          {tabs.map(([id, label]) => {
            const b = tabBadges[id];
            return (
              <button key={id} className={`view-btn ${activeView === id ? "active" : ""}`} onClick={() => setActiveView(id)}>
                {label}{b ? <span className={`tab-badge tab-badge-${b.type}`}>{b.n}</span> : null}
              </button>
            );
          })}
        </div>
      </header>

      <main>
        {activeView === "agents" ? (
          <section>
            <div className="head">
              <h2>Agent Activity</h2>
              <div className="stack tiny">
                <input className="agent-search" placeholder="Search agents\u2026" value={agentSearch} onChange={(e) => setAgentSearch(e.target.value)} />
                <span className="badge b-running"><StatusDot state="running" />running {agentsResp.states.running || 0}</span>
                <span className="badge b-idle"><StatusDot state="idle" />idle {agentsResp.states.idle || 0}</span>
                <span className="badge b-stuck"><StatusDot state="stuck" />stuck {agentsResp.states.stuck || 0}</span>
              </div>
            </div>
            <div className="body scroll">
              <details className="agent-group" open>
                <summary>
                  <div className="group-head">
                    <strong>Holding</strong>
                    <span className="badge">total {groupedAgents.holding.length}</span>
                    <span className="badge b-running"><StatusDot state="running" />run {counts(groupedAgents.holding).running || 0}</span>
                    <span className="badge b-idle"><StatusDot state="idle" />idle {counts(groupedAgents.holding).idle || 0}</span>
                    <span className="badge b-stuck"><StatusDot state="stuck" />stuck {counts(groupedAgents.holding).stuck || 0}</span>
                  </div>
                </summary>
                <div className="group-body">
                  <AgentTable
                    agents={groupedAgents.holding}
                    selectedAgentID={selectedAgentID}
                    onSelectAgent={setSelectedAgentID}
                    renderDropdown={renderAgentDropdown}
                    onNavigateTask={navigateToTask}
                  />
                </div>
              </details>

              {groupedAgents.opcos.map((g) => {
                const c = counts(g.agents);
                const hasStuck = (c.stuck || 0) > 0;
                return (
                  <details className="agent-group" key={g.slug} open={hasStuck}>
                    <summary>
                      <div className="group-head">
                        <strong>OpCO: {g.slug}</strong>
                        <span className="badge">total {g.agents.length}</span>
                        <span className="badge b-running"><StatusDot state="running" />run {c.running || 0}</span>
                        <span className="badge b-idle"><StatusDot state="idle" />idle {c.idle || 0}</span>
                        <span className="badge b-stuck"><StatusDot state="stuck" />stuck {c.stuck || 0}</span>
                      </div>
                    </summary>
                    <div className="group-body">
                      <AgentTable
                        agents={g.agents}
                        selectedAgentID={selectedAgentID}
                        onSelectAgent={setSelectedAgentID}
                        renderDropdown={renderAgentDropdown}
                        onNavigateTask={navigateToTask}
                      />
                    </div>
                  </details>
                );
              })}
            </div>
          </section>
        ) : null}

        {activeView === "digest" ? (
          <section>
            <div className="head">
              <h2>Portfolio Digest</h2>
              <div className="stack">
                <button onClick={() => loadDigest().catch((err) => addToast(err.message, "error"))}>Refresh</button>
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
        ) : null}

        {activeView === "events" ? (
          <section>
            <div className="head">
              <h2>Event Flow</h2>
              <div className="stack">
                <input placeholder="type (prefix*)" value={eventsFilter.type} onChange={(e) => setEventsFilter((p) => ({ ...p, type: e.target.value }))} />
                <input placeholder="source" value={eventsFilter.source} onChange={(e) => setEventsFilter((p) => ({ ...p, source: e.target.value }))} />
                <input placeholder="subscriber (agent)" value={eventsFilter.subscriber} onChange={(e) => setEventsFilter((p) => ({ ...p, subscriber: e.target.value }))} />
                <input placeholder="vertical slug/id" value={eventsFilter.vertical} onChange={(e) => setEventsFilter((p) => ({ ...p, vertical: e.target.value }))} />
                <input placeholder="component (runtime)" value={eventsFilter.component} onChange={(e) => setEventsFilter((p) => ({ ...p, component: e.target.value }))} />
                <input placeholder="level (debug|info|warn|error)" value={eventsFilter.level} onChange={(e) => setEventsFilter((p) => ({ ...p, level: e.target.value }))} />
                <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
                  <input type="checkbox" checked={eventsIncludeRuntime} onChange={(e) => setEventsIncludeRuntime(e.target.checked)} />
                  include runtime logs
                </label>
                <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
                  <input type="checkbox" checked={eventsRuntimeErrorsOnly} onChange={(e) => setEventsRuntimeErrorsOnly(e.target.checked)} />
                  runtime errors only
                </label>
                <button onClick={() => Promise.all([loadEvents(), loadRuntimeLogs()]).catch((err) => addToast(err.message, "error"))}>Filter</button>
                <button
                  className="btn-secondary"
                  onClick={() => {
                    setEventsFilter({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" });
                    setEventsIncludeRuntime(true);
                    setEventsRuntimeErrorsOnly(false);
                  }}
                >
                  Clear
                </button>
              </div>
            </div>
            <div className="row3 body">
              <div className="body scroll" style={{ maxHeight: "70vh", padding: 0 }}>
                {filteredEvents.length === 0 ? (
                  <div className="empty-state">No events match the current filters</div>
                ) : filteredEvents.map((e) => (
                  <div key={e.id} className={`timeline-item ${selectedEventID === e.id ? "selected" : ""}`} onClick={() => setSelectedEventID(e.id)}>
                    <div className="event-type">{e.type}</div>
                    <div className="tiny">{e.source_agent} | {e.vertical_slug || "-"} | <span title={fmtTime(e.created_at)}>{relTime(e.created_at)}</span></div>
                    <div className="tiny">delivered {e.delivery_count} | processed {e.processed_count} | errors {e.error_count} | pending {e.pending_count}</div>
                  </div>
                ))}
              </div>
              <div>
                <div className="tiny" style={{ marginBottom: 6 }}>Selected Event</div>
                <div className="tiny">{eventDetail && eventDetail.event ? `${eventDetail.event.type} by ${eventDetail.event.source_agent} at ${fmtTime(eventDetail.event.created_at)}` : "Select event"}</div>
                <JsonBlock data={(eventDetail && eventDetail.payload) || {}} defaultOpen={2} />
                <div className="tiny" style={{ margin: "8px 0 4px" }}>Deliveries</div>
                <div className="body scroll" style={{ maxHeight: 240, padding: 0 }}>
                  <table>
                    <thead><tr><th>Agent</th><th>Status</th><th>ms</th><th>Error</th></tr></thead>
                    <tbody>
                      {((eventDetail && eventDetail.deliveries) || []).length === 0 ? (
                        <tr><td colSpan={4} className="empty-state">No deliveries</td></tr>
                      ) : ((eventDetail && eventDetail.deliveries) || []).map((d) => (
                        <tr key={`${d.agent_id}-${d.status}-${d.retry_count || 0}`}>
                          <td>{d.agent_id}</td>
                          <td>{d.status}</td>
                          <td>{d.processing_ms || 0}</td>
                          <td>{d.error || "-"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
              <div>
                <div className="tiny" style={{ marginBottom: 6 }}>Runtime Logs</div>
                <div className="body scroll" style={{ maxHeight: "70vh", padding: 0 }}>
                  {filteredRuntimeLogs.length === 0 ? (
                    <div className="empty-state">No runtime logs match the current filters</div>
                  ) : filteredRuntimeLogs.map((rl) => (
                    <div key={`${rl.id}-${rl.ts || ""}`} className="timeline-item runtime-log-item">
                      <div className="event-type">{rl.component || "runtime"}.{rl.action || "-"}</div>
                      <div className="tiny">
                        <span className={`runtime-level rl-${(rl.level || "").toLowerCase()}`}>{rl.level || "info"}</span>
                        {" | "}
                        {rl.agent_id || "-"}
                        {" | "}
                        <span title={fmtTime(rl.ts)}>{relTime(rl.ts)}</span>
                      </div>
                      <div className="tiny mono">{rl.event_type || rl.error || "-"}</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </section>
        ) : null}

        {activeView === "logs" ? (() => {
          const selectedLog = filteredLogsData.find((l) => l.id === selectedLogID) || null;
          return (
            <div className="layout-two">
              <section>
                <div className="head">
                  <h2>Logs</h2>
                  <div className="stack">
                    <input placeholder="type" value={logsFilter.type} onChange={(e) => setLogsFilter((p) => ({ ...p, type: e.target.value }))} />
                    <input placeholder="agent" value={logsFilter.subscriber} onChange={(e) => setLogsFilter((p) => ({ ...p, subscriber: e.target.value }))} />
                    <input placeholder="source" value={logsFilter.source} onChange={(e) => setLogsFilter((p) => ({ ...p, source: e.target.value }))} />
                    <input placeholder="vertical" value={logsFilter.vertical} onChange={(e) => setLogsFilter((p) => ({ ...p, vertical: e.target.value }))} />
                    <input placeholder="component" value={logsFilter.component} onChange={(e) => setLogsFilter((p) => ({ ...p, component: e.target.value }))} />
                    <input placeholder="level" value={logsFilter.level} onChange={(e) => setLogsFilter((p) => ({ ...p, level: e.target.value }))} />
                    <label className="tiny" style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
                      <input type="checkbox" checked={logsRuntimeErrorsOnly} onChange={(e) => setLogsRuntimeErrorsOnly(e.target.checked)} />
                      runtime errors only
                    </label>
                    <button className="btn-secondary" onClick={() => setLogsOrder((o) => o === "desc" ? "asc" : "desc")}>{logsOrder === "desc" ? "Newest" : "Oldest"}</button>
                    <button onClick={() => loadLogs().catch((err) => addToast(err.message, "error"))}>Filter</button>
                    <button className="btn-secondary" onClick={() => { setLogsFilter({ type: "", source: "", vertical: "", component: "", level: "", subscriber: "" }); setLogsOrder("desc"); setLogsRuntimeErrorsOnly(false); }}>Clear</button>
                  </div>
                </div>
                <div className="body scroll">
                  {filteredLogsData.length === 0 ? (
                    <div className="empty-state">No runtime logs match the current filters</div>
                  ) : filteredLogsData.map((rl) => (
                    <div key={rl.id} className={`timeline-item runtime-log-item ${selectedLogID === rl.id ? "selected" : ""}`} onClick={() => setSelectedLogID(rl.id)}>
                      <div className="event-type">{rl.component || "runtime"}.{rl.action || "-"}</div>
                      <div className="tiny">
                        <span className={`runtime-level rl-${(rl.level || "").toLowerCase()}`}>{rl.level || "info"}</span>
                        {" | "}
                        {rl.agent_id || "-"}
                        {" | "}
                        <span title={fmtTime(rl.ts)}>{relTime(rl.ts)}</span>
                      </div>
                      <div className="tiny mono">{rl.event_type || rl.error || "-"}</div>
                    </div>
                  ))}
                </div>
              </section>
              <section>
                <div className="head"><h2>Log Detail</h2></div>
                <div className="body scroll">
                  {!selectedLog ? (
                    <div className="empty-state">Select a log entry</div>
                  ) : (
                    <>
                      <div className="log-detail-grid">
                        <span className="log-detail-label">ID</span><span className="log-detail-value mono">{selectedLog.id}</span>
                        <span className="log-detail-label">Timestamp</span><span className="log-detail-value">{fmtTime(selectedLog.ts)}</span>
                        <span className="log-detail-label">Level</span><span><span className={`runtime-level rl-${(selectedLog.level || "").toLowerCase()}`}>{selectedLog.level || "info"}</span></span>
                        <span className="log-detail-label">Component</span><span className="log-detail-value">{selectedLog.component || "-"}</span>
                        <span className="log-detail-label">Action</span><span className="log-detail-value">{selectedLog.action || "-"}</span>
                        <span className="log-detail-label">Agent</span><span className="log-detail-value mono">{selectedLog.agent_id || "-"}</span>
                        <span className="log-detail-label">Event ID</span><span className="log-detail-value mono">{selectedLog.event_id || "-"}</span>
                        <span className="log-detail-label">Event Type</span><span className="log-detail-value">{selectedLog.event_type || "-"}</span>
                        <span className="log-detail-label">Vertical</span><span className="log-detail-value mono">{selectedLog.vertical_id || "-"}</span>
                        <span className="log-detail-label">Campaign</span><span className="log-detail-value mono">{selectedLog.campaign_id || "-"}</span>
                        <span className="log-detail-label">Scan</span><span className="log-detail-value mono">{selectedLog.scan_id || "-"}</span>
                        <span className="log-detail-label">Session</span><span className="log-detail-value mono">{selectedLog.session_id || "-"}</span>
                        <span className="log-detail-label">Duration</span><span className="log-detail-value mono">{selectedLog.duration_us != null ? `${(selectedLog.duration_us / 1000).toFixed(1)} ms` : "-"}</span>
                      </div>
                      {selectedLog.error ? (
                        <>
                          <div className="log-detail-label" style={{ marginTop: 10 }}>Error</div>
                          <pre className="log-error-text">{selectedLog.error}</pre>
                        </>
                      ) : null}
                      {selectedLog.detail ? (
                        <>
                          <div className="log-detail-label" style={{ marginTop: 10 }}>Detail</div>
                          <JsonBlock data={selectedLog.detail} defaultOpen={2} />
                        </>
                      ) : null}
                    </>
                  )}
                </div>
              </section>
            </div>
          );
        })() : null}

        {activeView === "incidents" ? (
          <div className="layout-two">
            <section>
              <div className="head">
                <h2>Incident Response</h2>
                <div className="stack">
                  <select
                    value={String(incidentsFilter.sinceHours)}
                    onChange={(e) => setIncidentsFilter((p) => ({ ...p, sinceHours: Number(e.target.value || "24") }))}
                  >
                    <option value="1">1h</option>
                    <option value="6">6h</option>
                    <option value="24">24h</option>
                    <option value="72">72h</option>
                    <option value="168">7d</option>
                  </select>
                  <select
                    value={incidentsFilter.level}
                    onChange={(e) => setIncidentsFilter((p) => ({ ...p, level: e.target.value }))}
                  >
                    <option value="warn">warn+</option>
                    <option value="error">error only</option>
                    <option value="info">info+</option>
                  </select>
                  <input
                    placeholder="component"
                    value={incidentsFilter.component}
                    onChange={(e) => setIncidentsFilter((p) => ({ ...p, component: e.target.value }))}
                  />
                  <label className="tiny" style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                    <input
                      type="checkbox"
                      checked={incidentsFilter.mcpOnly}
                      onChange={(e) => setIncidentsFilter((p) => ({ ...p, mcpOnly: e.target.checked }))}
                    />
                    mcp only
                  </label>
                  <button onClick={() => loadIncidents().catch((err) => addToast(err.message, "error"))}>Refresh</button>
                  <button
                    className="btn-secondary"
                    onClick={() => {
                      setIncidentsFilter({ sinceHours: 24, mcpOnly: true, level: "warn", component: "" });
                    }}
                  >
                    Reset
                  </button>
                </div>
              </div>
              <div className="body scroll">
                {(incidentsData || []).length === 0 ? (
                  <div className="empty-state">No incidents for selected filters</div>
                ) : (
                  <table>
                    <thead><tr><th>Code</th><th>Count</th><th>Last Seen</th><th>Root Cause</th><th>Agents</th></tr></thead>
                    <tbody>
                      {(incidentsData || []).map((it) => (
                        <tr
                          key={it.code}
                          className={selectedIncidentCode === it.code ? "selected" : ""}
                          onClick={() => setSelectedIncidentCode(it.code)}
                          style={{ cursor: "pointer" }}
                        >
                          <td className="mono">{it.code}</td>
                          <td className="mono">{it.count || 0}</td>
                          <td title={fmtTime(it.last_seen)}>{relTime(it.last_seen)}</td>
                          <td>{it.root_cause || "-"}</td>
                          <td>{Array.isArray(it.agents) ? it.agents.length : 0}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </section>

            <section>
              <div className="head">
                <h2>Incident Detail</h2>
                <span className="tiny mono">{selectedIncidentCode || "none"}</span>
              </div>
              <div className="body scroll">
                {!selectedIncident ? (
                  <div className="empty-state">Select an incident code</div>
                ) : (
                  <>
                    <div className="health-card" style={{ marginBottom: 10 }}>
                      <div className="health-kv"><span>Code</span><span className="mono">{selectedIncident.code}</span></div>
                      <div className="health-kv"><span>Count</span><span className="mono">{selectedIncident.count || 0}</span></div>
                      <div className="health-kv"><span>First</span><span>{fmtTime(selectedIncident.first_seen)}</span></div>
                      <div className="health-kv"><span>Last</span><span>{fmtTime(selectedIncident.last_seen)}</span></div>
                      <div className="health-kv"><span>Root Cause</span><span>{selectedIncident.root_cause || "-"}</span></div>
                      <div className="health-kv"><span>Components</span><span>{(selectedIncident.components || []).join(", ") || "-"}</span></div>
                      <div className="health-kv"><span>Actions</span><span>{(selectedIncident.actions || []).join(", ") || "-"}</span></div>
                    </div>

                    <div className="tiny" style={{ marginBottom: 6 }}>Impacted Agents</div>
                    {(selectedIncident.agents || []).length === 0 ? (
                      <div className="empty-state" style={{ marginBottom: 10 }}>No agent IDs found in logs</div>
                    ) : (
                      <div className="stack" style={{ marginBottom: 10 }}>
                        {(selectedIncident.agents || []).map((agentID) => (
                          <button
                            key={agentID}
                            className={selectedIncidentAgent === agentID ? "" : "btn-secondary"}
                            onClick={() => setSelectedIncidentAgent(agentID)}
                          >
                            {agentID}
                          </button>
                        ))}
                        {selectedIncidentAgent ? (
                          <>
                            <button
                              className="btn-secondary"
                              onClick={() => {
                                setLogsFilter({ type: "", source: selectedIncidentAgent, vertical: "", component: "", level: "", subscriber: "" });
                                setLogsRuntimeErrorsOnly(false);
                                setActiveView("logs");
                              }}
                            >
                              Open Logs
                            </button>
                            <button
                              className="btn-secondary"
                              onClick={() => {
                                setSelectedConv(selectedIncidentAgent);
                                setActiveView("convos");
                              }}
                            >
                              Open Convo
                            </button>
                          </>
                        ) : null}
                      </div>
                    )}

                    <div className="tiny" style={{ marginBottom: 6 }}>Session Artifacts</div>
                    {incidentArtifacts.loading ? (
                      <div className="empty-state">Loading artifacts...</div>
                    ) : incidentArtifacts.error ? (
                      <div className="health-bad" style={{ marginBottom: 10 }}>{incidentArtifacts.error}</div>
                    ) : incidentArtifacts.data ? (
                      <details className="holding-artifact-card" open>
                        <summary>{incidentArtifacts.data.agent_id || selectedIncidentAgent || "agent"}</summary>
                        <JsonBlock data={incidentArtifacts.data} defaultOpen={2} />
                      </details>
                    ) : (
                      <div className="empty-state" style={{ marginBottom: 10 }}>Select an impacted agent</div>
                    )}

                    <div className="tiny" style={{ margin: "10px 0 6px" }}>Recent Runtime Logs</div>
                    {(incidentLogs || []).length === 0 ? (
                      <div className="empty-state">No runtime logs for selected code</div>
                    ) : (
                      <table>
                        <thead><tr><th>When</th><th>Agent</th><th>Component</th><th>Error</th></tr></thead>
                        <tbody>
                          {(incidentLogs || []).slice(0, 80).map((rl) => (
                            <tr key={rl.id} style={{ cursor: "pointer" }} onClick={() => setSelectedIncidentAgent(rl.agent_id || "")}>
                              <td title={fmtTime(rl.ts)}>{relTime(rl.ts)}</td>
                              <td className="mono">{rl.agent_id || "-"}</td>
                              <td>{rl.component || "runtime"}.{rl.action || "-"}</td>
                              <td className="tiny mono">{rl.error || rl.event_type || "-"}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </>
                )}
              </div>
            </section>
          </div>
        ) : null}

        {activeView === "flow" ? (
          <div className="layout-graph">
            <section>
              <div className="head">
                <h2>Pipeline Flow Visualizer</h2>
                <div className="stack">
                  <select value={flowView} onChange={(e) => { setFlowView(e.target.value); setFlowReplayOn(false); setFlowReplayIndex(0); setSelectedFlowEdgeID(""); }}>
                    <option value="design">design</option>
                    <option value="runtime">runtime</option>
                    <option value="replay">replay</option>
                  </select>
                  <select value={flowStage} onChange={(e) => { setFlowStage(e.target.value); setSelectedFlowEdgeID(""); }}>
                    {flowStageOptions.map((s) => <option key={s} value={s}>{s}</option>)}
                  </select>
                  <select value={flowRubric} onChange={(e) => { setFlowRubric(e.target.value); setSelectedFlowEdgeID(""); }}>
                    {flowRubricOptions.map((r) => <option key={r} value={r}>{r}</option>)}
                  </select>
                  {(flowView === "runtime" || flowView === "replay") ? (
                    <select value={flowVertical} onChange={(e) => { setFlowVertical(e.target.value); setSelectedFlowEdgeID(""); }}>
                      <option value="">all verticals</option>
                      {(verticals || []).map((v) => (
                        <option key={v.id || v.slug} value={v.slug || v.id}>
                          {(v.slug || (v.id || "").slice(0, 8))} | {v.stage || "-"} | {v.geography || "-"}
                        </option>
                      ))}
                    </select>
                  ) : null}
                  {flowView === "replay" ? (
                    <>
                      <input
                        type="datetime-local"
                        value={flowStart}
                        onChange={(e) => setFlowStart(e.target.value)}
                        title="start (local time)"
                      />
                      <input
                        type="datetime-local"
                        value={flowEnd}
                        onChange={(e) => setFlowEnd(e.target.value)}
                        title="end (local time)"
                      />
                      <select value={String(flowReplaySpeed)} onChange={(e) => setFlowReplaySpeed(Number(e.target.value || "10"))}>
                        <option value="10">10x</option>
                        <option value="50">50x</option>
                        <option value="100">100x</option>
                      </select>
                      <button
                        className="btn-secondary"
                        onClick={() => setFlowReplayOn((v) => !v)}
                        disabled={visibleFlowEvents.length >= (flowEvents || []).length && flowReplayOn}
                      >
                        {flowReplayOn ? "Pause" : "Play"}
                      </button>
                      <button className="btn-secondary" onClick={() => { setFlowReplayOn(false); setFlowReplayIndex(0); }}>Reset</button>
                    </>
                  ) : null}
                  <button onClick={() => loadPipelineFlow().catch((err) => addToast(err.message, "error"))}>Refresh</button>
                </div>
              </div>
              <div className="body">
                <GraphView
                  graph={flowGraph}
                  graphKey={`flow:${flowView}:${flowVertical || "all"}`}
                  selectedNodeID={selectedFlowNodeID}
                  selectedEdgeID={selectedFlowEdgeID}
                  onSelectNode={setSelectedFlowNodeID}
                  onSelectEdge={setSelectedFlowEdgeID}
                  onDerivedGraph={setFlowViewGraph}
                  runtimeAgents={agentsResp.agents}
                  isFullscreen={graphFullscreen}
                  onToggleFullscreen={() => setGraphFullscreen((p) => !p)}
                  activeEdgeKeys={flowActiveEdgeKeys}
                  stageFilter={flowStage}
                  rubricFilter={flowRubric}
                />
                <div className="tiny" style={{ marginTop: 6 }}>
                  Modes: design-time architecture, runtime live overlay, and replay from historical flow events.
                </div>
              </div>
            </section>

            <section>
              <div className="head">
                <h2>Flow Detail</h2>
                <span className="tiny mono">
                  {(flowGraphMeta && flowGraphMeta.node_count) || (flowGraph.nodes || []).length} nodes / {(flowGraphMeta && flowGraphMeta.edge_count) || (flowGraph.edges || []).length} edges
                </span>
              </div>
              <div className="body scroll">
                {(() => {
                  const g = flowViewGraph || flowGraph;
                  const node = (g.nodes || []).find((n) => n.id === selectedFlowNodeID) || null;
                  const edge = (g.edges || []).find((e, i) => `${e.kind}:${e.from}->${e.to}:${i}` === selectedFlowEdgeID) || null;
                  const nodeEdges = node ? (g.edges || []).filter((e) => e.from === node.id || e.to === node.id) : [];
                  return (
                    <>
                      {edge ? (
                        <>
                          <div className="tiny">Selected Edge</div>
                          <JsonBlock data={edge} defaultOpen={2} />
                        </>
                      ) : null}
                      {!node ? (
                        !edge ? <div className="empty-state">Click a node or edge to inspect flow details.</div> : null
                      ) : (
                        <>
                          <div className="tiny">Node</div>
                          <JsonBlock data={node} defaultOpen={2} />
                          <div className="tiny" style={{ margin: "8px 0 4px" }}>Connected Edges ({nodeEdges.length})</div>
                          {nodeEdges.length === 0 ? (
                            <div className="empty-state">No connected edges</div>
                          ) : (
                            <table>
                              <thead><tr><th>Kind</th><th>From</th><th>To</th><th>Label</th></tr></thead>
                              <tbody>
                                {nodeEdges.slice(0, 80).map((e, i) => (
                                  <tr key={`${e.from}-${e.to}-${i}`}>
                                    <td>{e.kind}</td>
                                    <td className="mono">{e.from}</td>
                                    <td className="mono">{e.to}</td>
                                    <td>{e.label || "-"}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          )}
                        </>
                      )}
                    </>
                  );
                })()}

                <div className="tiny" style={{ margin: "12px 0 4px" }}>
                  Flow Events ({visibleFlowEvents.length}{flowView === "replay" ? `/${(flowEvents || []).length}` : ""})
                </div>
                {visibleFlowEvents.length === 0 ? (
                  <div className="empty-state">No flow events loaded</div>
                ) : (
                  <table>
                    <thead><tr><th>When</th><th>Type</th><th>Source</th><th>Targets</th><th>Flags</th></tr></thead>
                    <tbody>
                      {visibleFlowEvents.slice(0, 120).map((ev) => (
                        <tr key={ev.event_id}>
                          <td title={fmtTime(ev.timestamp)}>{relTime(ev.timestamp)}</td>
                          <td className="mono">{ev.event_type}</td>
                          <td className="mono">{ev.source_node || "-"}</td>
                          <td className="tiny">{(ev.target_nodes || []).join(", ") || "-"}</td>
                          <td>
                            {ev.intercepted ? <span className="tag tag-warn">intercepted</span> : null}
                            {ev.passthrough ? <span className="tag tag-info" style={{ marginLeft: 4 }}>passthrough</span> : null}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </section>
          </div>
        ) : null}

        {activeView === "convos" ? (
          <section>
            <div className="head">
              <h2>Conversations</h2>
              <div className="stack">
                <select value={selectedConv} onChange={(e) => setSelectedConv(e.target.value)}>
                  {(conversations || []).map((c) => <option key={c.agent_id} value={c.agent_id}>{c.agent_id} ({c.role || "-"})</option>)}
                </select>
                <button onClick={() => loadConversationDetail(selectedConv).catch((err) => addToast(err.message, "error"))}>Open</button>
                <button className="btn-secondary" onClick={() => {
                  const msgs = conversationDetail.messages || [];
                  if (msgs.length === 0) { addToast("No messages to copy", "error"); return; }
                  const text = msgs.map((m) => {
                    const role = m.role || "unknown";
                    const label = selectedConv ? (role === "assistant" ? selectedConv : role === "user" ? "orchestrator" : role) : role;
                    const content = typeof m.content === "string" ? m.content : Array.isArray(m.content) ? m.content.map((c) => c.text || c.type || "").join("\n") : JSON.stringify(m.content, null, 2);
                    return `[${label}]\n${content}`;
                  }).join("\n\n---\n\n");
                  navigator.clipboard.writeText(text).then(() => addToast("Conversation copied", "success")).catch(() => addToast("Copy failed", "error"));
                }}>Copy All</button>
              </div>
            </div>
            <div className="row body">
              <div style={{ display: "flex", flexDirection: "column" }}>
                <div className="tiny">Messages ({(conversationDetail.messages || []).length})</div>
                <ChatMessages messages={conversationDetail.messages} agentID={selectedConv} turns={conversationDetail.turns} onOpenMessage={(m) => setModalContent({ title: `Message \u2014 ${m.role}`, text: m.text })} />
              </div>
              <div>
                <div className="tiny">Turns / Tool Calls</div>
                <div className="body scroll" style={{ maxHeight: "70vh", padding: 0 }}>
                  <table>
                    <thead><tr><th>#</th><th>OK</th><th>Latency</th><th>Tool Calls</th><th>Result</th></tr></thead>
                    <tbody>
                      {(conversationDetail.turns || []).length === 0 ? (
                        <tr><td colSpan={5} className="empty-state">No turns</td></tr>
                      ) : (conversationDetail.turns || []).map((t, i) => {
                        const text = t.assistant_text || t.tool_result || "-";
                        return (
                          <tr key={`${t.turn_index || i}-${i}`} style={{ cursor: "pointer" }} onClick={() => setModalContent({ title: `Turn ${t.turn_index != null ? t.turn_index : i}`, text })}>
                            <td>{t.turn_index}</td>
                            <td>{t.parse_ok ? "yes" : "no"}</td>
                            <td>{t.latency_ms}</td>
                            <td>{((t.tool_calls || []).map((x) => x.name || "-")).join(", ") || "-"}</td>
                            <td className="tiny" style={{ maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{text}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </section>
        ) : null}

        {activeView === "graph" ? (
          <div className="layout-graph">
            <section>
              <div className="head">
                <h2>Org Graph</h2>
                <div className="stack">
                  <select value={graphMode} onChange={(e) => { setSelectedGraphNodeID(""); setSelectedGraphEdgeID(""); setGraphMode(e.target.value); }}>
                    <option value="holding">holding</option>
                    <option value="template">default OpCo template</option>
                    <option value="opco">running OpCo</option>
                  </select>
                  {graphMode === "opco" ? (
                    <select value={graphVertical} onChange={(e) => { setSelectedGraphNodeID(""); setSelectedGraphEdgeID(""); setGraphVertical(e.target.value); }}>
                      {(verticals || []).map((v) => (
                        <option key={v.id || v.slug} value={v.slug || v.id}>
                          {(v.slug || (v.id || "").slice(0, 8))} | {v.stage || "-"} | {v.geography || "-"}
                        </option>
                      ))}
                    </select>
                  ) : null}
                  <button onClick={() => { Promise.all([loadVerticals(), loadGraph()]).catch(() => {}); }}>Refresh</button>
                </div>
              </div>
              <div className="body">
                <GraphView
                  graph={graph}
                  graphKey={`${graphMode}:${graphMode === "opco" ? graphVertical : ""}:${(graph && graph.template_version) || ""}`}
                  selectedNodeID={selectedGraphNodeID}
                  selectedEdgeID={selectedGraphEdgeID}
                  onSelectNode={setSelectedGraphNodeID}
                  onSelectEdge={setSelectedGraphEdgeID}
                  onDerivedGraph={setGraphViewGraph}
                  runtimeAgents={agentsResp.agents}
                  isFullscreen={graphFullscreen}
                  onToggleFullscreen={() => setGraphFullscreen((p) => !p)}
                />
                <div className="tiny" style={{ marginTop: 6 }}>
                  Bootstrap routes are solid, seeded routes dashed, discovered routes dotted. Message and mailbox edges are rendered separately from EventBus routing.
                </div>
              </div>
            </section>

            <section>
              <div className="head">
                <h2>Node Details</h2>
                <div className="tiny mono">{selectedGraphEdgeID ? "edge selected" : (selectedGraphNodeID || "none")}</div>
              </div>
              <div className="body scroll">
                {(() => {
                  const g = graphViewGraph || graph;
                  const edge = (g.edges || []).find((e, i) => `${e.kind}:${e.from}->${e.to}:${i}` === selectedGraphEdgeID) || null;
                  const node = (g.nodes || []).find((n) => n.id === selectedGraphNodeID) || null;
                  if (!node && !edge) return <div className="empty-state">Click a node or edge to inspect details.</div>;
                  if (!node && edge) {
                    return (
                      <div className="node-detail-card">
                        <div className="tiny">Selected Edge</div>
                        <JsonBlock data={edge} defaultOpen={2} />
                      </div>
                    );
                  }
                  const rt = node.kind === "agent" ? (agentsResp.agents || []).find((a) => a.id === node.id) : null;
                  const nodeEdges = (g.edges || []).filter((e) => e.from === node.id || e.to === node.id);
                  return (
                    <>
                      {edge ? (
                        <div className="node-detail-card">
                          <div className="tiny">Selected Edge</div>
                          <JsonBlock data={edge} defaultOpen={2} />
                        </div>
                      ) : null}
                      <div className="node-detail-card">
                        <div className="tiny">Identity</div>
                        <div className="node-detail-grid">
                          <span className="node-detail-label">ID</span><span className="mono" style={{ fontSize: 11 }}>{node.id}</span>
                          <span className="node-detail-label">Kind</span><span>{node.kind}</span>
                          <span className="node-detail-label">Group</span><span>{node.group || "-"}</span>
                          <span className="node-detail-label">Role</span><span>{node.role || "-"}</span>
                          {node.mode ? <><span className="node-detail-label">Mode</span><span>{node.mode}</span></> : null}
                          {node.status ? <><span className="node-detail-label">Status</span><span>{node.status}</span></> : null}
                          {node.vertical_slug ? <><span className="node-detail-label">Vertical</span><span>{node.vertical_slug}</span></> : null}
                          {node.parent_id ? <><span className="node-detail-label">Parent</span><span className="mono" style={{ fontSize: 10, cursor: "pointer", color: "var(--info)" }} onClick={() => setSelectedGraphNodeID(node.parent_id)}>{node.parent_id}</span></> : null}
                        </div>
                      </div>

                      {rt ? (
                        <div className="node-detail-card">
                          <div className="tiny">Runtime</div>
                          <div className="node-detail-grid">
                            <span className="node-detail-label">State</span>
                            <span><StatusDot state={rt.state} />{rt.state}</span>
                            <span className="node-detail-label">Turns</span>
                            <span className="mono">{rt.turn_count}/{rt.turn_limit}{rt.turn_limit > 0 ? ` (${Math.round((rt.turn_count / rt.turn_limit) * 100)}%)` : ""}</span>
                            <span className="node-detail-label">Turns 24h</span>
                            <span className="mono">{(rt.turns_24h || 0).toLocaleString()}</span>
                            <span className="node-detail-label">Tokens 24h</span>
                            <span className="mono">{(rt.total_tokens_24h || 0).toLocaleString()}</span>
                            <span className="node-detail-label">Pending</span>
                            <span className={`mono ${(rt.pending_events || 0) > 0 ? "health-warn" : ""}`}>{rt.pending_events || 0}</span>
                            {rt.current_task_id ? <><span className="node-detail-label">Task</span><span className="mono" style={{ cursor: "pointer", color: "var(--info)" }} onClick={() => navigateToTask(rt.current_task_id)}>{rt.current_task_id.slice(0, 12)}</span></> : null}
                            {rt.stuck_reason ? <><span className="node-detail-label">Stuck</span><span style={{ color: "var(--bad)" }}>{rt.stuck_reason}</span></> : null}
                            {rt.started_at ? <><span className="node-detail-label">Started</span><span title={fmtTime(rt.started_at)}>{relTime(rt.started_at)}</span></> : null}
                          </div>
                          {rt.last_tool && rt.last_tool.name ? (
                            <div style={{ marginTop: 6, display: "flex", gap: 8, alignItems: "baseline" }}>
                              <span className="node-detail-label">Last Tool</span>
                              <span className="mono" style={{ fontSize: 11 }}>{rt.last_tool.name}{rt.last_tool.ok === false ? " (fail)" : ""}</span>
                            </div>
                          ) : null}
                        </div>
                      ) : null}

                      {(node.tools || []).length > 0 ? (
                        <div className="node-detail-card">
                          <div className="tiny">Tools ({(node.tools || []).length})</div>
                          <div className="node-tools">
                            {(node.tools || []).map((t, i) => (
                              <span key={i} className="node-tool-badge">{typeof t === "string" ? t : (t.name || t.type || JSON.stringify(t))}</span>
                            ))}
                          </div>
                        </div>
                      ) : null}

                      {(node.subscriptions || []).length > 0 ? (
                        <div className="node-detail-card">
                          <div className="tiny">Subscriptions ({(node.subscriptions || []).length})</div>
                          <div className="node-subs">
                            {(node.subscriptions || []).map((s, i) => (
                              <div key={i} className="node-sub-item mono">{typeof s === "string" ? s : (s.type || s.event_type || JSON.stringify(s))}</div>
                            ))}
                          </div>
                        </div>
                      ) : null}

                      {nodeEdges.length > 0 ? (
                        <div className="node-detail-card">
                          <div className="tiny">Connections ({nodeEdges.length})</div>
                          <table style={{ fontSize: 11 }}>
                            <thead><tr><th>Kind</th><th>From</th><th>To</th><th>Source</th></tr></thead>
                            <tbody>
                              {nodeEdges.map((e, i) => (
                                <tr key={i}>
                                  <td><span className={`badge ${isEventLinkedEdgeKind(e.kind) || e.kind === "message" || e.kind === "mailbox" ? "b-running" : ""}`} style={{ fontSize: 9 }}>{e.kind}</span></td>
                                  <td className="mono" style={{ fontSize: 10, cursor: e.from !== node.id ? "pointer" : "default", color: e.from === node.id ? "var(--text-3)" : "var(--info)" }} onClick={() => { if (e.from !== node.id) setSelectedGraphNodeID(e.from); }}>{e.from === node.id ? "self" : e.from}</td>
                                  <td className="mono" style={{ fontSize: 10, cursor: e.to !== node.id ? "pointer" : "default", color: e.to === node.id ? "var(--text-3)" : "var(--info)" }} onClick={() => { if (e.to !== node.id) setSelectedGraphNodeID(e.to); }}>{e.to === node.id ? "self" : e.to}</td>
                                  <td className="tiny">{e.source || e.label || "-"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : null}

                      {rt && node.kind === "agent" ? (
                        <div className="node-detail-card">
                          <div className="tiny">Quick Actions</div>
                          <div className="stack" style={{ marginTop: 4 }}>
                            <button className="btn-secondary" onClick={() => {
                              if (!window.confirm(`Restart agent "${node.id}"?`)) return;
                              postJSON("/dashboard/api/control/agents/restart", { agent_id: node.id })
                                .then(() => { addToast(`Restarted ${node.id}`, "success"); loadAgents().catch(() => {}); })
                                .catch((err) => addToast(err.message, "error"));
                            }}>Restart</button>
                            <button className="btn-secondary" onClick={() => {
                              setControlTarget(node.id);
                              setActiveView("control");
                            }}>Control</button>
                            <button className="btn-secondary" onClick={() => {
                              setSelectedAgentID(node.id);
                              setActiveView("agents");
                            }}>Inspect</button>
                          </div>
                        </div>
                      ) : null}

                      {node.system_prompt ? (
                        <details className="node-detail-card">
                          <summary className="tiny" style={{ cursor: "pointer" }}>System Prompt</summary>
                          <pre className="json" style={{ whiteSpace: "pre-wrap", maxHeight: "40vh", marginTop: 6 }}>
                            {node.system_prompt}
                          </pre>
                        </details>
                      ) : null}

                      {node.constraints && Object.keys(node.constraints).length > 0 ? (
                        <div className="node-detail-card">
                          <div className="tiny">Constraints</div>
                          <div className="json" style={{ maxHeight: 120 }}>
                            {JSON.stringify(node.constraints, null, 2)}
                          </div>
                        </div>
                      ) : null}
                    </>
                  );
                })()}
              </div>
            </section>
          </div>
        ) : null}

        {activeView === "control" ? (
          <div className="layout-two">
            <section>
              <div className="head"><h2>Control Panel</h2><span className="tiny">execute actions</span></div>
              <div className="body scroll">
                <div className="control-card">
                  <div className="tiny">Target Agent</div>
                  <select value={controlTarget} onChange={(e) => setControlTarget(e.target.value)} style={{ width: "100%", marginTop: 4 }}>
                    {targets.map((t) => <option key={t.agent_id} value={t.agent_id}>{t.agent_id} | {t.role || "-"} | {t.vertical_slug || t.status || "-"}</option>)}
                  </select>
                </div>

                <div className="control-card">
                  <div className="tiny">Directive</div>
                  <textarea style={{ width: "100%", marginTop: 4 }} placeholder="Message to agent" value={directiveMessage} onChange={(e) => setDirectiveMessage(e.target.value)} />
                  <div className="stack" style={{ marginTop: 6 }}>
                    <button disabled={!directiveMessage.trim()} onClick={() => runControl(() => postJSON("/dashboard/api/control/directive", { agent_id: controlTarget, message: directiveMessage.trim() }))}>Send Directive</button>
                  </div>
                </div>

                <div className="control-card">
                  <div className="tiny">Chat</div>
                  <textarea style={{ width: "100%", marginTop: 4 }} placeholder="Chat message" value={chatMessage} onChange={(e) => setChatMessage(e.target.value)} />
                  <div className="stack" style={{ marginTop: 6 }}>
                    <select value={chatMode} onChange={(e) => setChatMode(e.target.value)}>
                      <option value="live">live</option>
                      <option value="async">async</option>
                    </select>
                    <button disabled={!chatMessage.trim()} onClick={() => runControl(() => postJSON("/dashboard/api/control/chat", { agent_id: controlTarget, mode: chatMode, message: chatMessage.trim() }))}>Send Chat</button>
                  </div>
                </div>

                <div className="control-card">
                  <div className="tiny">Agent Recovery</div>
                  <div className="stack" style={{ marginTop: 4 }}>
                    <button className="btn-secondary" onClick={() => {
                      if (!window.confirm(`Restart agent "${controlTarget}"? This will interrupt any in-progress work.`)) return;
                      runControl(() => postJSON("/dashboard/api/control/agents/restart", { agent_id: controlTarget }));
                    }}>Restart</button>
                    <button className="btn-secondary" onClick={() => {
                      if (!window.confirm(`Replay backlog for "${controlTarget}"?`)) return;
                      runControl(() => postJSON("/dashboard/api/control/agents/replay", { agent_id: controlTarget }));
                    }}>Replay Backlog</button>
                  </div>
                </div>

                <div className="control-card">
                  <div className="tiny">Create Vertical + OpCo</div>
                  <div className="stack" style={{ marginTop: 4 }}>
                    <input placeholder="Vertical name" value={verticalName} onChange={(e) => setVerticalName(e.target.value)} />
                    <input placeholder="Geography" value={verticalGeo} onChange={(e) => setVerticalGeo(e.target.value)} />
                    <input placeholder="slug (optional)" value={verticalSlug} onChange={(e) => setVerticalSlug(e.target.value)} />
                    <button onClick={() => runControl(() => postJSON("/dashboard/api/control/verticals/create", { name: verticalName.trim(), geography: verticalGeo.trim(), slug: verticalSlug.trim() || undefined }))}>Create</button>
                  </div>
                </div>

                <div className="control-card">
                  <div className="tiny">Event Requeue</div>
                  <div className="stack" style={{ marginTop: 4 }}>
                    <input className="mono" style={{ minWidth: 180 }} placeholder="event id" value={requeueEventID} onChange={(e) => setRequeueEventID(e.target.value)} />
                    <select value={requeueAgentID} onChange={(e) => setRequeueAgentID(e.target.value)}>
                      <option value="">all delivered recipients</option>
                      {targets.map((t) => <option key={t.agent_id} value={t.agent_id}>{t.agent_id}</option>)}
                    </select>
                    <button className="btn-secondary" onClick={() => runControl(() => postJSON("/dashboard/api/control/events/requeue", { event_id: requeueEventID.trim(), agent_id: requeueAgentID || undefined }))}>Requeue</button>
                  </div>
                </div>

                <div className="control-card">
                  <div className="tiny">Org Bootstrap + Danger Zone</div>
                  <div className="stack" style={{ marginTop: 4 }}>
                    <button onClick={() => runControl(() => postJSON("/dashboard/api/control/seed-org", {}))}>Seed Org</button>
                    <button className="btn-secondary" onClick={() => runControl(() => postJSON("/dashboard/api/control/runtime", { action: "pause" }))}>Pause</button>
                    <button className="btn-secondary" onClick={() => runControl(() => postJSON("/dashboard/api/control/runtime", { action: "resume" }))}>Resume</button>
                  </div>
                  <div className="stack" style={{ marginTop: 6 }}>
                    <input className="mono" placeholder='type RESET to unlock' value={resetConfirm} onChange={(e) => setResetConfirm(e.target.value)} />
                    <button className="btn-danger" disabled={!resetOK} onClick={() => runControl(async () => {
                      const out = await postJSON("/dashboard/api/control/runtime", { action: "reset_db", confirm: (resetConfirm || "").trim(), seed_org: true });
                      setResetConfirm("");
                      return out;
                    })}>Reset DB + Seed</button>
                    <button className="btn-danger" disabled={!resetOK} onClick={() => runControl(async () => {
                      const out = await postJSON("/dashboard/api/control/runtime", { action: "reset_state", confirm: (resetConfirm || "").trim() });
                      setResetConfirm("");
                      return out;
                    })}>Wipe DB</button>
                  </div>
                </div>

                <div className="json" style={{ maxHeight: 160 }}>{JSON.stringify(controlOutput, null, 2)}</div>
              </div>
            </section>

            <section>
              <div className="head">
                <h2>Mailbox + Decisions</h2>
                <select value={mailStatus} onChange={(e) => setMailStatus(e.target.value)}>
                  <option value="all">all</option>
                  <option value="pending">pending</option>
                  <option value="approved">approved</option>
                  <option value="rejected">rejected</option>
                  <option value="timed_out">timed_out</option>
                </select>
              </div>
              <div className="body scroll">
                <div className="stack tiny">
                  <span className="badge">pending {mailbox.summary.pending || 0}</span>
                  <span className="badge">critical {mailbox.summary.critical || 0}</span>
                  <span className="badge">decided {mailbox.summary.decided || 0}</span>
                </div>

                <div className="tiny" style={{ marginTop: 8 }}>Mailbox Decision</div>
                <div className="stack" style={{ marginBottom: 8 }}>
                  <input className="mono" style={{ minWidth: 120 }} placeholder="mailbox id" value={mailboxID} onChange={(e) => setMailboxID(e.target.value)} />
                  <select value={mailboxAction} onChange={(e) => setMailboxAction(e.target.value)}>
                    <option value="approve">approve</option>
                    <option value="reject">reject</option>
                    <option value="more-data">more-data</option>
                    <option value="kill">kill</option>
                    <option value="revise">revise</option>
                    <option value="skip">skip</option>
                    <option value="respond">respond</option>
                  </select>
                  <input placeholder="notes" value={mailboxNotes} onChange={(e) => setMailboxNotes(e.target.value)} />
                  <button onClick={() => runControl(() => postJSON(`/api/mailbox/${encodeURIComponent(mailboxID.trim())}/decide`, { action: mailboxAction, notes: mailboxNotes.trim() }))}>Decide</button>
                </div>

                <div className="body scroll" style={{ maxHeight: "52vh", padding: 0 }}>
                  <table>
                    <thead><tr><th>ID</th><th>Type</th><th>Status</th><th>Priority</th><th>Agent</th><th>Age</th><th>Action</th></tr></thead>
                    <tbody>
                      {mailbox.items.length === 0 ? (
                        <tr><td colSpan={7} className="empty-state">No mailbox items</td></tr>
                      ) : mailbox.items.map((m) => {
                        const expanded = selectedMailboxItem === m.id;
                        return (
                          <React.Fragment key={m.id}>
                            <tr style={{ cursor: "pointer" }} onClick={() => setSelectedMailboxItem(expanded ? "" : m.id)}>
                              <td><CopyID id={m.id} /></td>
                              <td>{m.type}</td>
                              <td>{m.status}</td>
                              <td>{m.priority}</td>
                              <td>{m.from_agent}</td>
                              <td><span title={fmtTime(m.created_at)}>{relTime(m.created_at)}</span></td>
                              <td>
                                {m.status === "pending" ? (
                                  <div className="stack" onClick={(e) => e.stopPropagation()}>
                                    <button onClick={() => quickMailboxDecide(m.id, "approve")}>approve</button>
                                    <button className="btn-secondary" onClick={() => {
                                      if (!window.confirm(`Reject mailbox item from ${m.from_agent}?`)) return;
                                      quickMailboxDecide(m.id, "reject");
                                    }}>reject</button>
                                  </div>
                                ) : m.decided_action || "-"}
                              </td>
                            </tr>
                            {expanded ? (
                              <tr>
                                <td colSpan={7} className="agent-drop-cell">
                                  <div style={{ padding: "10px 14px" }}>
                                    <div className="tiny">Request Details</div>
                                    <pre className="json" style={{ maxHeight: 240 }}>{JSON.stringify(
                                      Object.fromEntries(Object.entries(m).filter(([k]) => !["id"].includes(k))),
                                      null, 2
                                    )}</pre>
                                  </div>
                                </td>
                              </tr>
                            ) : null}
                          </React.Fragment>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            </section>
          </div>
        ) : null}

        {activeView === "tasks" ? (
          <section>
            <div className="head">
              <h2>Human Tasks</h2>
              <div className="stack">
                <select value={taskStatus} onChange={(e) => setTaskStatus(e.target.value)}>
                  <option value="open">open</option>
                  <option value="pending_review">pending_review</option>
                  <option value="approved">approved</option>
                  <option value="assigned">assigned</option>
                  <option value="completed">completed</option>
                  <option value="rejected">rejected</option>
                  <option value="deferred">deferred</option>
                  <option value="expired">expired</option>
                  <option value="all">all</option>
                </select>
                <button onClick={() => loadTasks().catch((err) => addToast(err.message, "error"))}>Refresh</button>
                <button className="btn-secondary" onClick={() => loadTaskStats().catch((err) => addToast(err.message, "error"))}>Stats</button>
              </div>
            </div>
            {tasksResp.weekly_budget ? (
              <div className="tiny" style={{ marginBottom: 8, padding: "0 16px" }}>
                Weekly budget: {tasksResp.weekly_budget.approved_this_week || 0}/{tasksResp.weekly_budget.max_tasks_per_week || 0}
                {" "} (reset: {tasksResp.weekly_budget.reset_day || "monday"} 00:00 UTC; week start {tasksResp.weekly_budget.week_start_utc || "-"})
              </div>
            ) : null}
            {tasksStats ? (
              <div className="json" style={{ maxHeight: 160, marginBottom: 8, marginLeft: 16, marginRight: 16 }}>{JSON.stringify(tasksStats, null, 2)}</div>
            ) : null}

            <div className="row body">
              <div className="body scroll" style={{ maxHeight: "58vh", padding: 0 }}>
                <table>
                  <thead>
                    <tr>
                      <th>ID</th>
                      <th>Status</th>
                      <th>Pri</th>
                      <th>Category</th>
                      <th>Description</th>
                      <th>Requester</th>
                      <th>Created</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(tasksResp.tasks || []).length === 0 ? (
                      <tr><td colSpan={7} className="empty-state">No tasks in this status</td></tr>
                    ) : (tasksResp.tasks || []).map((t) => (
                      <tr
                        key={t.id}
                        className={selectedTaskID === t.id ? "selected" : ""}
                        onClick={() => setSelectedTaskID(selectedTaskID === t.id ? "" : t.id)}
                        style={{ cursor: "pointer" }}
                      >
                        <td><CopyID id={t.id} /></td>
                        <td><span className={`badge`}>{t.status}</span></td>
                        <td>{t.priority || "-"}</td>
                        <td>{t.category}</td>
                        <td className="tiny" style={{ maxWidth: 260 }}>{(t.description || "").slice(0, 80)}{(t.description || "").length > 80 ? "\u2026" : ""}</td>
                        <td className="mono">{t.requesting_agent}</td>
                        <td><span title={fmtTime(t.created_at)}>{relTime(t.created_at)}</span></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div>
                <div className="tiny" style={{ marginBottom: 6 }}>Selected Task</div>
                {selectedTask ? (
                  <div>
                    <div className="mono" style={{ marginBottom: 6 }}><CopyID id={selectedTask.id} len={12} /></div>
                    <div className="stack tiny" style={{ marginBottom: 4 }}>
                      <span className="badge">{selectedTask.status}</span>
                      <span>{selectedTask.category}</span>
                      <span>{selectedTask.priority}</span>
                      {selectedTask.vertical_slug ? <span>{selectedTask.vertical_slug}</span> : null}
                      {selectedTask.assigned_to ? <span>assigned: {selectedTask.assigned_to}</span> : null}
                      {selectedTask.deadline ? <span>due: <span title={fmtTime(selectedTask.deadline)}>{relTime(selectedTask.deadline)}</span></span> : null}
                    </div>
                    <div className="body" style={{ marginTop: 8 }}>
                      <div className="tiny">Description</div>
                      <div className="desc-text">{selectedTask.description}</div>
                      <div className="tiny" style={{ marginTop: 8 }}>Complete</div>
                      <textarea
                        placeholder="Result text (what happened, what you learned, next steps)..."
                        value={taskResultText}
                        onChange={(e) => setTaskResultText(e.target.value)}
                      />
                      <div className="stack" style={{ marginTop: 6 }}>
                        <select value={taskOutcome} onChange={(e) => setTaskOutcome(e.target.value)}>
                          <option value="success">success</option>
                          <option value="partial">partial</option>
                          <option value="failed">failed</option>
                        </select>
                        <label className="tiny" style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <input type="checkbox" checked={taskFollowUpNeeded} onChange={(e) => setTaskFollowUpNeeded(e.target.checked)} />
                          follow_up_needed
                        </label>
                      </div>
                      <div className="stack" style={{ marginTop: 6 }}>
                        <button className="btn-secondary" onClick={() => claimSelectedTask().catch((err) => addToast(err.message, "error"))}>Claim</button>
                        <button onClick={() => completeSelectedTask().catch((err) => addToast(err.message, "error"))}>Complete</button>
                      </div>
                      <div className="tiny" style={{ marginTop: 10 }}>Reject (human pushback)</div>
                      <textarea
                        placeholder="Why you can't do it / what blocks execution..."
                        value={taskRejectReason}
                        onChange={(e) => setTaskRejectReason(e.target.value)}
                      />
                      <div className="stack" style={{ marginTop: 6 }}>
                        <button className="btn-secondary" onClick={() => rejectSelectedTask().catch((err) => addToast(err.message, "error"))}>Reject</button>
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="empty-state">Select a task to claim/complete it.</div>
                )}
              </div>
            </div>
          </section>
        ) : null}

        {activeView === "pipeline" ? (
          <section>
            <div className="head">
              <h2>Pipeline Funnel</h2>
              <div className="stack">
                <input placeholder="trace vertical slug/id" value={traceVertical} onChange={(e) => setTraceVertical(e.target.value)} />
                <button onClick={() => loadTrace(traceVertical.trim()).catch((err) => addToast(err.message, "error"))}>Trace</button>
              </div>
            </div>
            <div className="body">
              <div className="row3">
                <div><div className="tiny">Discoveries (14d)</div><div><strong>{funnel.throughput.discoveries_14d || 0}</strong></div></div>
                <div><div className="tiny">Scoring Completion</div><div><strong>{Math.round((funnel.throughput.scoring_completion_rate || 0) * 100)}%</strong></div></div>
                <div><div className="tiny">Approved/Live vs Killed</div><div><strong>{funnel.throughput.specs_approved_or_live || 0} / {funnel.throughput.specs_killed_total || 0}</strong></div></div>
              </div>
              <div className="tiny" style={{ margin: "8px 0 4px" }}>Shard Scan Progress</div>
              <div className="body scroll" style={{ maxHeight: 210, padding: 0 }}>
                <table>
                  <thead><tr><th>Scan</th><th>Mode</th><th>Geo</th><th>Progress</th><th>Active</th><th>Failed</th><th>Stuck</th><th>Spend</th></tr></thead>
                  <tbody>
                    {(shardScans || []).length === 0 ? (
                      <tr><td colSpan={8} className="empty-state">No shard scans found</td></tr>
                    ) : (shardScans || []).map((s) => {
                      const expanded = selectedShardScanID === s.scan_id;
                      const shards = shardScanDetails[s.scan_id] || [];
                      const reportsTotal = shards.reduce((sum, sh) => sum + Number(sh.reports_count || 0), 0);
                      const highSignalTotal = shards.reduce((sum, sh) => sum + Number(sh.high_signal_count || 0), 0);
                      return (
                        <React.Fragment key={s.scan_id}>
                          <tr style={{ cursor: "pointer" }} onClick={() => {
                            const next = expanded ? "" : s.scan_id;
                            setSelectedShardScanID(next);
                            if (!expanded) {
                              loadShardScanDetail(s.scan_id).catch((err) => addToast(err.message, "error"));
                            }
                          }}>
                            <td><CopyID id={s.scan_id} len={10} /></td>
                            <td>{s.mode || "-"}</td>
                            <td>{s.geography || "-"}</td>
                            <td>{Math.round((s.progress || 0) * 100)}% ({s.shards_completed || 0}/{s.shards_total || 0})</td>
                            <td>{(s.shards_assigned || 0) + (s.shards_pending || 0)}</td>
                            <td>{s.shards_failed || 0}</td>
                            <td className={(s.shards_stuck || 0) > 0 ? "health-warn" : ""}>{s.shards_stuck || 0}</td>
                            <td className="mono">{formatDollars(s.spend_cents || 0)}</td>
                          </tr>
                          {expanded ? (
                            <tr>
                              <td colSpan={8} className="agent-drop-cell">
                                <div style={{ padding: "10px 12px" }}>
                                  <div className="stack" style={{ justifyContent: "space-between", marginBottom: 8 }}>
                                    <div className="tiny">Shard Details ({shards.length}) • Reports {reportsTotal} • High-signal {highSignalTotal}</div>
                                    <button
                                      className="btn-secondary"
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        loadShardScanDetail(s.scan_id).catch((err) => addToast(err.message, "error"));
                                      }}
                                    >
                                      Refresh
                                    </button>
                                  </div>
                                  <div className="body scroll" style={{ maxHeight: 260, padding: 0 }}>
                                    <table>
                                      <thead><tr><th>Shard</th><th>Scope</th><th>Status</th><th>Reports</th><th>High</th><th>Duration</th><th>Spend</th><th>Agent</th><th>Action</th></tr></thead>
                                      <tbody>
                                        {shards.length === 0 ? (
                                          <tr><td colSpan={9} className="empty-state">No shard details loaded</td></tr>
                                        ) : shards.map((sh) => {
                                          const stuckClass = sh.stuck_state === "critical" ? "health-bad" : sh.stuck_state === "warning" ? "health-warn" : "";
                                          return (
                                            <tr key={sh.id}>
                                              <td><CopyID id={sh.id} len={10} /></td>
                                              <td className="tiny">{shardScopeSummary(sh.scope)}</td>
                                              <td className={stuckClass || ""}>{sh.status || "-"}</td>
                                              <td className="mono">{sh.reports_count || 0}</td>
                                              <td className="mono">{sh.high_signal_count || 0}</td>
                                              <td className="mono">{formatDurationMs(sh.duration_ms)}</td>
                                              <td className="mono">{formatDollars(sh.spend_cents || 0)}</td>
                                              <td className="mono">{sh.agent_id || "-"}</td>
                                              <td>
                                                <div className="stack">
                                                  {(sh.status === "failed" || sh.status === "timed_out") ? (
                                                    <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); shardAction(s.scan_id, sh.id, "retry").catch((err) => addToast(err.message, "error")); }}>retry</button>
                                                  ) : null}
                                                  {(sh.status === "pending" || sh.status === "assigned") ? (
                                                    <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); shardAction(s.scan_id, sh.id, "cancel").catch((err) => addToast(err.message, "error")); }}>cancel</button>
                                                  ) : null}
                                                </div>
                                              </td>
                                            </tr>
                                          );
                                        })}
                                      </tbody>
                                    </table>
                                  </div>
                                </div>
                              </td>
                            </tr>
                          ) : null}
                        </React.Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              <div className="tiny" style={{ margin: "8px 0 4px" }}>Stuck Verticals</div>
              <div className="body scroll" style={{ maxHeight: 180, padding: 0 }}>
                <table>
                  <thead><tr><th>Vertical</th><th>Stage</th><th>Idle hrs</th></tr></thead>
                  <tbody>
                    {(funnel.stuck || []).length === 0 ? (
                      <tr><td colSpan={3} className="empty-state">No stuck verticals</td></tr>
                    ) : (funnel.stuck || []).map((v) => (
                      <tr key={v.id || v.slug} style={{ cursor: "pointer" }} onClick={() => { const s = v.slug || v.id; setTraceVertical(s); loadTrace(s).catch((err) => addToast(err.message, "error")); }} title="Click to trace"><td style={{ color: "var(--info)" }}>{v.slug || v.id}</td><td>{v.stage}</td><td>{v.idle_hours}</td></tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="tiny" style={{ margin: "8px 0 4px" }}>Lifecycle Trace</div>
              <div className="body scroll" style={{ maxHeight: 250, padding: 0 }}>
                <table>
                  <thead><tr><th>At</th><th>Type</th><th>Source</th><th>Pending</th></tr></thead>
                  <tbody>
                    {traceRows.length === 0 ? (
                      <tr><td colSpan={4} className="empty-state">Enter a vertical and click Trace</td></tr>
                    ) : traceRows.slice(-120).map((e) => (
                      <tr key={e.id}><td><span title={fmtTime(e.created_at)}>{relTime(e.created_at)}</span></td><td>{e.type}</td><td>{e.source_agent}</td><td>{e.pending_count}</td></tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </section>
        ) : null}

        {activeView === "holding" ? (
          <section>
            <div className="head">
              <h2>Holding</h2>
              <span className="tiny">
                {holdingData.summary.total || 0} total &middot; {holdingData.summary.in_pipeline || 0} in pipeline &middot; {holdingData.summary.killed || 0} killed
              </span>
            </div>

            {/* Campaign Status Bar */}
            <div className="body scroll" style={{ maxHeight: 220, marginBottom: 12 }}>
              <div className="tiny" style={{ marginBottom: 4 }}>Campaigns</div>
              <table>
                <thead><tr><th>ID</th><th>Mode</th><th>Geography</th><th>Status</th><th>Priority</th><th>Discoveries</th><th>Categories</th><th>Elapsed</th></tr></thead>
                <tbody>
                  {(holdingData.campaigns || []).length === 0 ? (
                    <tr><td colSpan={8} className="empty-state">No campaigns</td></tr>
                  ) : (holdingData.campaigns || []).map((c) => (
                    <tr key={c.id}>
                      <td><CopyID id={c.id} /></td>
                      <td>{c.mode}</td>
                      <td>{c.geography}{c.country ? ` (${c.country})` : ""}</td>
                      <td><span className={`tag ${c.status === "active" ? "tag-good" : c.status === "paused" ? "tag-warn" : c.status === "completed" ? "tag-info" : ""}`}>{c.status}</span></td>
                      <td className="mono">{c.priority}</td>
                      <td className="mono">{c.discoveries}</td>
                      <td className="tiny">{(c.categories || []).join(", ") || "-"}</td>
                      <td>{relTime(c.started_at || c.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Kanban Board */}
            <div className="kanban-board">
              {holdingColumns.map((col) => (
                <div key={col.key} className={`kanban-col${col.key === "killed" ? " kanban-col-killed" : ""}`}>
                  <div className="kanban-col-head">
                    <span>{col.label}</span>
                    <span className="mono">{col.items.length}</span>
                  </div>
                  <div className="kanban-col-body">
                    {col.items.length === 0 ? (
                      <div className="empty-state" style={{ padding: "12px 8px", fontSize: 11 }}>Empty</div>
                    ) : col.items.map((v) => {
                      const score = parseFloat(v.composite_score);
                      const scoreClass = !isNaN(score) ? (score >= 75 ? "tag-good" : score >= 50 ? "tag-warn" : "tag-bad") : "";
                      const ac = (holdingData.agent_counts || {})[v.id];
                      return (
                        <div
                          key={v.id}
                          className={`vertical-card${v.stage === "killed" ? " vertical-card-killed" : ""}`}
                          onClick={() => openHoldingVerticalDetail(v.id)}
                          title="Open full project details"
                        >
                          <div className="vertical-card-header">
                            <span className="vertical-card-name" title={v.name}>{v.slug || v.name}</span>
                            {v.composite_score ? <span className={`vertical-card-score ${scoreClass}`}>{v.composite_score}</span> : null}
                          </div>
                          <div className="vertical-card-meta">
                            {v.geography ? <span className="tiny">{v.geography}</span> : null}
                            <span className="tiny">{relTime(v.updated_at)}</span>
                          </div>
                          {ac ? (
                            <div className="tiny" style={{ marginTop: 4 }}>
                              agents {ac.active}/{ac.total}
                            </div>
                          ) : null}
                          {v.stage === "killed" && v.kill_reason ? (
                            <div className="vertical-card-kill tiny">{v.kill_reason}</div>
                          ) : null}
                          {col.key === "validation" ? <GateIndicator stage={v.stage} /> : null}
                          <div className="tiny" style={{ marginTop: 4, color: "var(--info)" }}>Click for full details</div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              ))}
            </div>
          </section>
        ) : null}

        {activeView === "health" ? (
          <section>
            <div className="head"><h2>Health</h2><span className="tiny">ops telemetry</span></div>
            <div className="body scroll">
              <div className="row" style={{ marginBottom: 10 }}>
                <div className="health-card">
                  <div className="tiny">System Status</div>
                  <div className="health-kv">
                    <span>Runtime</span>
                    <span className={health.runtime?.running ? "health-good" : "health-bad"}>{health.runtime?.running ? "Running" : "Stopped"}</span>
                  </div>
                  <div className="health-kv">
                    <span>Loaded Agents</span>
                    <span className="mono">{health.runtime?.loaded_agents || 0}</span>
                  </div>
                  <div className="health-kv">
                    <span>Postgres</span>
                    <span className="mono">{health.postgres?.active_connections || 0} / {health.postgres?.max_connections || 0} connections</span>
                  </div>
                </div>
                <div className="health-card">
                  <div className="tiny">Spend (24h)</div>
                  <div className="health-kv">
                    <span>API Cost</span>
                    <span className="mono">{formatDollars(health.spend?.api_cost_24h_cents)}</span>
                  </div>
                  <div className="health-kv">
                    <span>API Avg (7d)</span>
                    <span className="mono">{formatDollars(health.spend?.api_cost_daily_avg_7d_cents)}</span>
                  </div>
                  <div className="health-kv">
                    <span>Infra</span>
                    <span className="mono">{formatDollars(health.spend?.infra_cost_24h_cents)}</span>
                  </div>
                  <div className="health-kv">
                    <span>Ledger</span>
                    <span className="mono">{formatDollars(health.spend?.spend_ledger_24h_cents)}</span>
                  </div>
                </div>
              </div>
              <div className="row" style={{ marginBottom: 10 }}>
                <div className="health-card">
                  <div className="tiny">Auth</div>
                  <div className="health-kv">
                    <span>OAuth Token</span>
                    <span className={health.auth?.oauth_token_configured ? "health-good" : "health-bad"}>{health.auth?.oauth_token_configured ? "Configured" : "Missing"}</span>
                  </div>
                  <div className="health-kv">
                    <span>Errors (1h)</span>
                    <span className={(health.auth?.auth_errors_1h || 0) > 0 ? "health-bad mono" : "mono"}>{health.auth?.auth_errors_1h || 0}</span>
                  </div>
                  <div className="health-kv">
                    <span>Errors (24h)</span>
                    <span className={(health.auth?.auth_errors_24h || 0) > 0 ? "health-warn mono" : "mono"}>{health.auth?.auth_errors_24h || 0}</span>
                  </div>
                </div>
                <div className="health-card">
                  <div className="tiny">Containers</div>
                  {(health.containers || []).length === 0 ? <div className="empty-state">No container data</div> : (health.containers || []).map((x) => (
                    <div className="health-kv" key={x.name}>
                      <span>{x.name}</span>
                      <span className={x.status === "running" ? "health-good mono" : "health-warn mono"}>{x.status}</span>
                    </div>
                  ))}
                  {health.container_error ? <div className="health-bad mono" style={{ marginTop: 4 }}>{health.container_error}</div> : null}
                </div>
              </div>
              <div className="health-card">
                <div className="tiny">Vertical Health</div>
                {(health.vertical_health || []).length === 0 ? <div className="empty-state">No vertical health data</div> : (
                  <table>
                    <thead><tr><th>Vertical</th><th>Health</th><th>Deploy</th></tr></thead>
                    <tbody>
                      {(health.vertical_health || []).slice(0, 200).map((v) => (
                        <tr key={`${v.vertical_id}-${v.slug}`}>
                          <td>{v.slug}</td>
                          <td><span className={v.health_status === "healthy" ? "health-good" : "health-warn"}>{v.health_status}</span></td>
                          <td><span className="mono">{v.deploy_status}</span></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          </section>
        ) : null}
      </main>

      {modalContent ? (
        <Modal title={modalContent.title} onClose={() => setModalContent(null)} copyText={modalContent.text}>
          <div className="md-body">{renderMarkdown(modalContent.text)}</div>
        </Modal>
      ) : null}
      {holdingDetailModal.open ? (
        <Modal
          title={`Holding Vertical — ${holdingDetailModal.data?.vertical?.slug || holdingDetailModal.data?.vertical?.name || holdingDetailModal.id || ""}`}
          onClose={() => setHoldingDetailModal({ open: false, loading: false, id: "", error: "", data: null })}
          copyText={holdingDetailModal.data ? JSON.stringify(holdingDetailModal.data, null, 2) : ""}
          className="holding-detail-modal"
        >
          {holdingDetailModal.loading ? (
            <div className="empty-state">Loading vertical detail...</div>
          ) : holdingDetailModal.error ? (
            <div className="health-bad">{holdingDetailModal.error}</div>
          ) : (
            <HoldingVerticalDetail detail={holdingDetailModal.data} />
          )}
        </Modal>
      ) : null}
      <Toasts items={toasts} />
    </>
  );
}

createRoot(document.getElementById("root")).render(<App />);
