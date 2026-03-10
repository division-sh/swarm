import React from "react";
import JsonBlock from "../../../components/JsonBlock.tsx";
import MarkdownBlock from "../../../components/MarkdownBlock.tsx";
import {
  artifactIsScalar,
  formatArtifactScalar,
  hasArtifactValue,
  normalizeScoreRows,
  prettyArtifactKey,
} from "./helpers.ts";

function TextOrJsonArtifact({ value }) {
  if (artifactIsScalar(value)) {
    return <div className="holding-detail-text">{formatArtifactScalar(value)}</div>;
  }
  return <JsonBlock data={value} defaultOpen={2} />;
}

function ArtifactGeneric({ payload }) {
  if (!payload) return <div className="empty-state">No data</div>;
  if (typeof payload === "string") {
    return <div className="artifact-text-block"><MarkdownBlock text={payload} className="" /></div>;
  }
  if (Array.isArray(payload)) {
    if (payload.length === 0) return <div className="empty-state">No data</div>;
    if (payload.every((value) => artifactIsScalar(value))) {
      return (
        <div className="artifact-chip-row">
          {payload.map((value, index) => <span key={index} className="tag">{formatArtifactScalar(value)}</span>)}
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
  const scalarEntries = entries.filter(([, value]) => artifactIsScalar(value));
  const nestedEntries = entries.filter(([, value]) => !artifactIsScalar(value));

  return (
    <div className="artifact-generic">
      {scalarEntries.length > 0 ? (
        <div className="artifact-kv-list">
          {scalarEntries.map(([key, value]) => (
            <div key={key} className="artifact-kv-row">
              <span>{prettyArtifactKey(key)}</span>
              <span>{formatArtifactScalar(value)}</span>
            </div>
          ))}
        </div>
      ) : null}
      {nestedEntries.map(([key, value]) => (
        <details key={key} className="artifact-subsection">
          <summary>{prettyArtifactKey(key)}</summary>
          {typeof value === "string" ? (
            <div className="artifact-text-block"><MarkdownBlock text={value} className="" /></div>
          ) : (
            <JsonBlock data={value} defaultOpen={2} />
          )}
        </details>
      ))}
    </div>
  );
}

function ScoresArtifact({ payload }) {
  if (!payload || typeof payload !== "object") return <ArtifactGeneric payload={payload} />;
  const summaryKeys = ["result", "rubric", "mode", "composite_score", "viability_score", "market_score", "confidence", "confidence_score", "signal_strength"];
  const summary = summaryKeys
    .filter((key) => payload[key] != null && String(payload[key]).trim() !== "")
    .map((key) => [key, payload[key]]);
  const dimensionRows = normalizeScoreRows(payload.dimensions || payload.dimension_scores || payload.breakdown || payload.scores);

  return (
    <div className="artifact-generic">
      {summary.length > 0 ? (
        <div className="artifact-kv-list">
          {summary.map(([key, value]) => (
            <div key={key} className="artifact-kv-row">
              <span>{prettyArtifactKey(key)}</span>
              <span>{formatArtifactScalar(value)}</span>
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
              {dimensionRows.map((row, index) => (
                <tr key={`${row.dimension}-${index}`}>
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

function RawSignalsArtifact({ payload }) {
  if (!payload || typeof payload !== "object") return <ArtifactGeneric payload={payload} />;
  const summaryKeys = ["category", "subcategory", "geography", "mode", "signal_strength", "priority"];
  const summary = summaryKeys
    .filter((key) => payload[key] != null && String(payload[key]).trim() !== "")
    .map((key) => [key, payload[key]]);
  const opportunity = payload.opportunity_hypothesis ?? payload.opportunity ?? payload.hypothesis ?? "";
  const evidence = payload.evidence ?? payload.market_evidence ?? payload.problem_evidence ?? "";
  const automationMicro = payload.automation_micro;

  return (
    <div className="artifact-generic">
      {summary.length > 0 ? (
        <div className="artifact-kv-list">
          {summary.map(([key, value]) => (
            <div key={key} className="artifact-kv-row">
              <span>{prettyArtifactKey(key)}</span>
              <span>{formatArtifactScalar(value)}</span>
            </div>
          ))}
        </div>
      ) : null}
      {hasArtifactValue(opportunity) ? (
        <div className="artifact-text-card">
          <div className="tiny">Opportunity Hypothesis</div>
          <TextOrJsonArtifact value={opportunity} />
        </div>
      ) : null}
      {hasArtifactValue(evidence) ? (
        <div className="artifact-text-card">
          <div className="tiny">Evidence</div>
          <TextOrJsonArtifact value={evidence} />
        </div>
      ) : null}
      {automationMicro && typeof automationMicro === "object" ? (
        <details className="artifact-subsection">
          <summary>Automation-Micro Signal</summary>
          <ArtifactGeneric payload={automationMicro} />
        </details>
      ) : null}
      <details className="artifact-subsection">
        <summary>Raw Signal Payload</summary>
        <JsonBlock data={payload} defaultOpen={2} />
      </details>
    </div>
  );
}

export default function HoldingArtifactPayload({ label, payload }) {
  if (label === "Scores") return <ScoresArtifact payload={payload} />;
  if (label === "Raw Signals") return <RawSignalsArtifact payload={payload} />;
  return <ArtifactGeneric payload={payload} />;
}
