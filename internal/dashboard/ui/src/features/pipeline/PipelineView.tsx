import React from "react";
import CopyID from "../../components/CopyID.tsx";
import { fmtTime, formatDollars, formatDurationMs, relTime, shardScopeSummary } from "../../lib/format.ts";

export default function PipelineView({
  state,
  actions,
  portfolioFocusKey = "",
  onFocusVertical,
  portfolioDownstreamByKey = {},
  onOpenHolding,
  onOpenWorkflow,
  onOpenOperations,
}) {
  const { funnel, shardScans, shardScanDetails, traceRows, traceVertical, selectedShardScanID } = state;
  const { setTraceVertical, setSelectedShardScanID, traceVerticalFlow, loadShardScanDetail, shardAction } = actions;
  const retryScans = (shardScans || []).filter((scan) => Number(scan.shards_failed || 0) > 0 || Number(scan.shards_stuck || 0) > 0);
  const activeScans = (shardScans || []).filter((scan) => Number(scan.shards_pending || 0) > 0 || Number(scan.shards_assigned || 0) > 0);
  const focusTraceRows = traceRows.slice(-80).reverse();

  return (
    <section>
      <div className="head">
        <h2>Pipeline Funnel</h2>
        <div className="stack">
          <input aria-label="Pipeline trace vertical" placeholder="trace vertical slug/id" value={traceVertical} onChange={(e) => setTraceVertical(e.target.value)} />
          <button onClick={() => {
            const next = traceVertical.trim();
            if (!next) return;
            onFocusVertical?.(next);
            traceVerticalFlow(next).catch(() => {});
          }}>Trace</button>
        </div>
      </div>
      <div className="body">
        <div className="row3">
          <div className="health-card"><div className="tiny">Discoveries (14d)</div><div className="big-number">{funnel.throughput.discoveries_14d || 0}</div><div className="tiny">recent portfolio throughput</div></div>
          <div className="health-card"><div className="tiny">Scoring Completion</div><div className="big-number">{Math.round((funnel.throughput.scoring_completion_rate || 0) * 100)}%</div><div className="tiny">discovered {"->"} scored</div></div>
          <div className="health-card"><div className="tiny">Approved / Killed</div><div className="big-number">{funnel.throughput.specs_approved_or_live || 0} / {funnel.throughput.specs_killed_total || 0}</div><div className="tiny">approved-or-live vs killed</div></div>
        </div>

        <div className="row" style={{ marginBottom: 12, gap: 12 }}>
          <div className="holding-detail-section">
            <div className="tiny" style={{ marginBottom: 6 }}>Attention Verticals</div>
            <div className="body scroll" style={{ maxHeight: 220, padding: 0 }}>
              <table>
                <thead><tr><th>Vertical</th><th>Stage</th><th>Idle hrs</th><th>Action</th></tr></thead>
                <tbody>
                  {(funnel.stuck || []).length === 0 ? (
                    <tr><td colSpan={4} className="empty-state">No stuck verticals</td></tr>
                  ) : (funnel.stuck || []).map((v) => {
                    const key = v.slug || v.id;
                    const downstream = portfolioDownstreamByKey[key] || null;
                    return (
                      <tr
                        key={key}
                        style={{ cursor: "pointer", background: portfolioFocusKey && portfolioFocusKey === key ? "rgba(212, 162, 74, 0.08)" : undefined }}
                        onClick={() => { onFocusVertical?.(key); setTraceVertical(key); traceVerticalFlow(key).catch(() => {}); }}
                        title="Click to trace"
                      >
                        <td style={{ color: "var(--info)" }}>{key}</td>
                        <td>{v.stage}</td>
                        <td>{v.idle_hours}</td>
                        <td>
                          <div className="stack">
                            <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(key); setTraceVertical(key); traceVerticalFlow(key).catch(() => {}); }}>Trace</button>
                            <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(key); onOpenHolding?.(v); }}>Holding</button>
                            <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(key); onOpenWorkflow?.(v); }}>Workflow</button>
                            {downstream?.summary?.mailbox || downstream?.summary?.tasks ? (
                              <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); onFocusVertical?.(key); onOpenOperations?.(v); }}>Ops</button>
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

          <div className="holding-detail-section">
            <div className="tiny" style={{ marginBottom: 6 }}>Retry Needed Shard Scans</div>
            <div className="body scroll" style={{ maxHeight: 220, padding: 0 }}>
              <table>
                <thead><tr><th>Scan</th><th>Geo</th><th>Failed</th><th>Stuck</th><th>Action</th></tr></thead>
                <tbody>
                  {retryScans.length === 0 ? (
                    <tr><td colSpan={5} className="empty-state">No retry-needed scans</td></tr>
                  ) : retryScans.slice(0, 10).map((scan) => (
                    <tr key={scan.scan_id}>
                      <td><CopyID id={scan.scan_id} len={10} /></td>
                      <td>{scan.geography || "-"}</td>
                      <td className="mono">{scan.shards_failed || 0}</td>
                      <td className={(scan.shards_stuck || 0) > 0 ? "health-warn mono" : "mono"}>{scan.shards_stuck || 0}</td>
                      <td>
                        <button
                          className="btn-secondary"
                          onClick={() => {
                            const expanded = selectedShardScanID === scan.scan_id;
                            const next = expanded ? "" : scan.scan_id;
                            setSelectedShardScanID(next);
                            if (!expanded) loadShardScanDetail(scan.scan_id).catch(() => {});
                          }}
                        >
                          Inspect
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
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
                        loadShardScanDetail(s.scan_id).catch(() => {});
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
                                  loadShardScanDetail(s.scan_id).catch(() => {});
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
                                              <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); shardAction(s.scan_id, sh.id, "retry").catch(() => {}); }}>retry</button>
                                            ) : null}
                                            {(sh.status === "pending" || sh.status === "assigned") ? (
                                              <button className="btn-secondary" onClick={(e) => { e.stopPropagation(); shardAction(s.scan_id, sh.id, "cancel").catch(() => {}); }}>cancel</button>
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

        <div className="row3" style={{ marginTop: 12 }}>
          <div className="health-card">
            <div className="tiny">Active Scans</div>
            <div className="big-number">{activeScans.length}</div>
            <div className="tiny">pending or assigned shard scans</div>
          </div>
          <div className="health-card">
            <div className="tiny">Focused Vertical</div>
            <div className="big-number">{traceVertical || portfolioFocusKey || "-"}</div>
            <div className="tiny">current pipeline trace target</div>
          </div>
          <div className="health-card">
            <div className="tiny">Trace Rows</div>
            <div className="big-number">{traceRows.length}</div>
            <div className="tiny">loaded lifecycle events</div>
          </div>
        </div>

        <div className="tiny" style={{ margin: "8px 0 4px" }}>Lifecycle Trace</div>
        <div className="body scroll" style={{ maxHeight: 250, padding: 0 }}>
          <table>
            <thead><tr><th>At</th><th>Type</th><th>Source</th><th>Pending</th></tr></thead>
            <tbody>
              {traceRows.length === 0 ? (
                <tr><td colSpan={4} className="empty-state">Enter a vertical and click Trace</td></tr>
              ) : focusTraceRows.map((e) => (
                <tr key={e.id}><td><span title={fmtTime(e.created_at)}>{relTime(e.created_at)}</span></td><td>{e.type}</td><td>{e.source_agent}</td><td>{e.pending_count}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
