import React from "react";
import CopyID from "../../components/CopyID.tsx";
import GateIndicator from "../../components/GateIndicator.tsx";
import { readPath, relTime } from "../../lib/format.ts";

export default function HoldingView({
  state,
  actions,
  portfolioFocusKey = "",
  onFocusVertical,
  onOpenFunnelTrace,
  onOpenWorkflowTrace,
  onOpenWorkflowTopology,
  portfolioDownstreamByKey = {},
  onOpenOperations,
  onOpenAgent,
}) {
  const {
    holdingData,
    holdingVisibleVerticals,
    holdingWorkflowSummary,
    holdingColumns,
    validationGateData,
    holdingSearch,
    holdingWorkflowFilter,
    holdingSort,
  } = state;
  const { setHoldingSearch, setHoldingWorkflowFilter, setHoldingSort, openHoldingVerticalDetail } = actions;

  return (
    <section>
      <div className="head">
        <h2>Holding</h2>
        <div className="stack tiny">
          <span>
            {holdingData.summary.total || 0} total &middot; {holdingData.summary.in_pipeline || 0} in pipeline &middot; {holdingData.summary.killed || 0} killed
          </span>
          <span>visible {holdingVisibleVerticals.length} &middot; drift {holdingWorkflowSummary.drift} &middot; timers {holdingWorkflowSummary.timers} &middot; revisions {holdingWorkflowSummary.revisions}</span>
        </div>
        <div className="stack">
          <input className="agent-search" placeholder="Search verticals…" value={holdingSearch} onChange={(e) => setHoldingSearch(e.target.value)} />
          <select value={holdingWorkflowFilter} onChange={(e) => setHoldingWorkflowFilter(e.target.value)}>
            <option value="all">all workflow states</option>
            <option value="drift">drift only</option>
            <option value="timers">with timers</option>
            <option value="revisions">with revisions</option>
            <option value="stale">entered stage set</option>
          </select>
          <select value={holdingSort} onChange={(e) => setHoldingSort(e.target.value)}>
            <option value="updated_desc">sort: updated</option>
            <option value="stage_age_desc">sort: oldest stage</option>
            <option value="revisions_desc">sort: revisions</option>
            <option value="timers_desc">sort: timers</option>
            <option value="score_desc">sort: score</option>
          </select>
        </div>
      </div>

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

      <div className="row" style={{ marginBottom: 12 }}>
        <div className="health-card">
          <div className="tiny">Workflow Triage</div>
          <div className="health-kv"><span>Drift</span><span className={Number(holdingData.workflow_summary?.drift || 0) > 0 ? "health-warn mono" : "mono"}>{holdingData.workflow_summary?.drift || 0}</span></div>
          <div className="health-kv"><span>With Timers</span><span className="mono">{holdingData.workflow_summary?.active_timers || 0}</span></div>
          <div className="health-kv"><span>Revisioned</span><span className="mono">{holdingData.workflow_summary?.revisioned || 0}</span></div>
          <div className="health-kv"><span>Stage Tracked</span><span className="mono">{holdingData.workflow_summary?.stage_entered_set || 0}</span></div>
        </div>
        <div className="holding-detail-section">
          <div className="tiny" style={{ marginBottom: 6 }}>Workflow Triage Preview</div>
          {holdingVisibleVerticals.length === 0 ? (
            <div className="empty-state">No visible verticals</div>
          ) : (
            <table>
              <thead><tr><th>Vertical</th><th>Stage Age</th><th>Revisions</th><th>Timers</th><th>Drift</th><th>Actions</th></tr></thead>
              <tbody>
                {holdingVisibleVerticals.slice(0, 12).map((v) => {
                  const downstream = portfolioDownstreamByKey[v.slug || v.id] || null;
                  return (
                  <tr
                    key={v.id}
                    style={{ cursor: "pointer", background: portfolioFocusKey && portfolioFocusKey === (v.slug || v.id) ? "rgba(212, 162, 74, 0.08)" : undefined }}
                    onClick={() => { onFocusVertical?.(v.slug || v.id); openHoldingVerticalDetail(v.id); }}
                  >
                    <td>{v.slug || v.name || v.id}</td>
                    <td>{v.stage_entered_at ? relTime(v.stage_entered_at) : "-"}</td>
                    <td className="mono">{readPath(v, ["revision_count"]) || "0"}</td>
                    <td className="mono">{v.active_timer_count || 0}</td>
                    <td>{v.workflow_current_stage && v.workflow_current_stage !== v.stage ? "yes" : "no"}</td>
                    <td>
                      <div className="stack">
                        <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(v.slug || v.id); onOpenFunnelTrace?.(v.slug || v.id); }}>Trace</button>
                        <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(v.slug || v.id); onOpenWorkflowTrace?.(v); }}>Workflow</button>
                        {(downstream?.summary?.tasks || downstream?.summary?.mailbox) ? <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(v.slug || v.id); onOpenOperations?.(v); }}>Ops</button> : null}
                      </div>
                    </td>
                  </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>

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
                const workflowDrift = v.workflow_current_stage && v.workflow_current_stage !== v.stage;
                const downstream = portfolioDownstreamByKey[v.slug || v.id] || null;
                return (
                  <div
                    key={v.id}
                    className={`vertical-card${v.stage === "killed" ? " vertical-card-killed" : ""}`}
                    onClick={() => { onFocusVertical?.(v.slug || v.id); openHoldingVerticalDetail(v.id); }}
                    title="Open full project details"
                    style={portfolioFocusKey && portfolioFocusKey === (v.slug || v.id) ? { boxShadow: "0 0 0 1px rgba(212, 162, 74, 0.22), 0 0 0 3px rgba(212, 162, 74, 0.08)" } : undefined}
                  >
                    <div className="vertical-card-header">
                      <span className="vertical-card-name" title={v.name}>{v.slug || v.name}</span>
                      {v.composite_score ? <span className={`vertical-card-score ${scoreClass}`}>{v.composite_score}</span> : null}
                    </div>
                    <div className="vertical-card-meta">
                      {v.geography ? <span className="tiny">{v.geography}</span> : null}
                      <span className="tiny">{relTime(v.updated_at)}</span>
                    </div>
                    {(v.workflow_version || v.stage_entered_at || v.revision_count || v.active_timer_count || workflowDrift) ? (
                      <div className="stack" style={{ marginTop: 4, gap: 4 }}>
                        {v.workflow_version ? <span className="badge mono">wf {v.workflow_version}</span> : null}
                        {v.stage_entered_at ? <span className="badge">stage {relTime(v.stage_entered_at)}</span> : null}
                        {v.revision_count ? <span className="badge mono">rev {readPath(v, ["revision_count"])}</span> : null}
                        {(v.active_timer_count || 0) > 0 ? <span className="badge mono">timers {v.active_timer_count}</span> : null}
                        {workflowDrift ? <span className="badge b-stuck">state drift</span> : null}
                      </div>
                    ) : null}
                    {ac ? (
                      <div className="tiny" style={{ marginTop: 4 }}>
                        agents {ac.active}/{ac.total}
                      </div>
                    ) : null}
                    {workflowDrift ? (
                      <div className="tiny health-warn" style={{ marginTop: 4 }}>
                        db stage {v.stage} vs workflow {v.workflow_current_stage}
                      </div>
                    ) : null}
                    {(v.workflow_current_stage || v.stage) ? (
                      <div className="tiny" style={{ marginTop: 4 }}>
                        execution {v.workflow_current_stage || v.stage}
                      </div>
                    ) : null}
                    {v.stage === "killed" && v.kill_reason ? (
                      <div className="vertical-card-kill tiny">{v.kill_reason}</div>
                    ) : null}
                    {col.key === "validation" ? <GateIndicator stage={v.stage} stages={validationGateData.stages} labels={validationGateData.labels} /> : null}
                    <div className="stack" style={{ marginTop: 6 }}>
                      <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(v.slug || v.id); onOpenFunnelTrace?.(v.slug || v.id); }}>Trace</button>
                      <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(v.slug || v.id); onOpenWorkflowTrace?.(v); }}>Workflow</button>
                      <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(v.slug || v.id); onOpenWorkflowTopology?.(v); }}>Topology</button>
                      {(downstream?.summary?.tasks || downstream?.summary?.mailbox) ? <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(v.slug || v.id); onOpenOperations?.(v); }}>Ops</button> : null}
                      {downstream?.primaryAgent ? <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(v.slug || v.id); onOpenAgent?.(downstream.primaryAgent.id || downstream.primaryAgent.agent_id || ""); }}>Agent</button> : null}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
