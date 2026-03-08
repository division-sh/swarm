import React from "react";
import { fmtTime } from "../../lib/format.js";

export default function HoldingSummaryPanel({ vertical, businessModel, opportunity }) {
  const value = vertical || {};

  return (
    <div className="holding-detail-summary">
      <div className="holding-detail-title">
        <strong>{value.name || value.slug || value.id || "Vertical"}</strong>
        <div className="stack">
          <span className="tag">{value.stage || "-"}</span>
          {value.mode ? <span className="tag">{value.mode}</span> : null}
          {value.composite_score ? <span className="tag tag-info">score {value.composite_score}</span> : null}
          {value.geography ? <span className="tag">{value.geography}</span> : null}
        </div>
      </div>
      <div className="row">
        <div className="health-card">
          <div className="tiny">Project Snapshot</div>
          <div className="health-kv"><span>ID</span><span className="mono">{value.id || "-"}</span></div>
          <div className="health-kv"><span>Slug</span><span className="mono">{value.slug || "-"}</span></div>
          <div className="health-kv"><span>Template</span><span>{value.template_version || "-"}</span></div>
          <div className="health-kv"><span>Created</span><span>{fmtTime(value.created_at)}</span></div>
          <div className="health-kv"><span>Updated</span><span>{fmtTime(value.updated_at)}</span></div>
          {value.approved_at ? <div className="health-kv"><span>Approved</span><span>{fmtTime(value.approved_at)}</span></div> : null}
          {value.launched_at ? <div className="health-kv"><span>Launched</span><span>{fmtTime(value.launched_at)}</span></div> : null}
        </div>
        <div className="health-card">
          <div className="tiny">Business Model + Opportunity</div>
          <div className="holding-detail-text"><strong>Business Model:</strong> {businessModel || "-"}</div>
          <div className="holding-detail-text"><strong>Opportunity:</strong> {opportunity || "-"}</div>
          {value.live_url ? <div className="holding-detail-text"><strong>Live URL:</strong> <a className="md-link" href={value.live_url} target="_blank" rel="noreferrer">{value.live_url}</a></div> : null}
          {value.kill_reason ? <div className="holding-detail-text health-bad"><strong>Kill Reason:</strong> {value.kill_reason}</div> : null}
          {value.human_notes ? <div className="holding-detail-text"><strong>Notes:</strong> {value.human_notes}</div> : null}
        </div>
      </div>
    </div>
  );
}
