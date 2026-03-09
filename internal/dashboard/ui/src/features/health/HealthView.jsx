import React from "react";
import { formatDollars, readPath } from "../../lib/format.js";

function HotspotRow({ item, onOpen }) {
  return (
    <div className="health-kv">
      <div>
        <div>{item.title}</div>
        <div className="tiny">{item.detail}</div>
      </div>
      <button className="btn-secondary" onClick={() => onOpen(item.route.view, item.route.subview)}>Open</button>
    </div>
  );
}

export default function HealthView({ state, actions = {} }) {
  const { health, contractsData, contractWorkflow, contractPlatform, contractVerification, derived } = state;
  const { openView, openWorkflowTraceForVertical, openPortfolioForVertical } = actions;
  const unhealthyVerticals = Array.isArray(derived?.unhealthyVerticals) ? derived.unhealthyVerticals : [];
  const workflowWarnings = Array.isArray(derived?.warnings) ? derived.warnings : [];
  return (
    <section>
      <div className="head"><h2>Health</h2><span className="tiny">deep diagnostics + contract state</span></div>
      <div className="body scroll">
        <div className="health-card" style={{ marginBottom: 10 }}>
          <div className="tiny">Diagnostic Scope</div>
          <div style={{ marginTop: 6 }}>
            Health is now the deeper diagnostics page for infra state, auth, spend, contracts, workflow audits, and vertical deploy health.
          </div>
          <div className="tiny" style={{ marginTop: 6 }}>
            Use <span className="mono">Overview</span> for live triage and "what needs attention now".
          </div>
        </div>

        <div className="metrics-grid" style={{ marginBottom: 10 }}>
          <div className="stat-card">
            <div className="tiny">Auth Errors (24h)</div>
            <div className="stat">{derived?.authErrors24h || 0}</div>
            <button className="btn-secondary" onClick={() => openView?.("observability", "logs")}>Open Logs</button>
          </div>
          <div className="stat-card">
            <div className="tiny">Workflow Warnings</div>
            <div className="stat">{workflowWarnings.length}</div>
            <button className="btn-secondary" onClick={() => openView?.("workflow", "issues")}>Open Workflow</button>
          </div>
          <div className="stat-card">
            <div className="tiny">Unhealthy Verticals</div>
            <div className="stat">{unhealthyVerticals.length}</div>
            <button className="btn-secondary" onClick={() => openView?.("portfolio", "holding")}>Open Portfolio</button>
          </div>
          <div className="stat-card">
            <div className="tiny">Verification Gates</div>
            <div className="stat">{derived?.contractSummary?.verificationCount || 0}</div>
            <button className="btn-secondary" onClick={() => openView?.("workflow", "artifacts")}>Open Artifacts</button>
          </div>
        </div>

        <div className="holding-detail-section" style={{ marginBottom: 10 }}>
          <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
            <div className="tiny">Diagnostic Hotspots</div>
            <button className="btn-secondary" onClick={() => openView?.("overview")}>Open Overview</button>
          </div>
          {(derived?.hotspots || []).length === 0 ? (
            <div className="empty-state">No active diagnostic hotspots.</div>
          ) : (
            <div className="body" style={{ gap: 8 }}>
              {(derived?.hotspots || []).map((item) => (
                <HotspotRow key={`${item.kind}-${item.title}`} item={item} onOpen={openView} />
              ))}
            </div>
          )}
        </div>

        <div className="tiny" style={{ marginBottom: 6 }}>Infra Diagnostics</div>
        <div className="row" style={{ marginBottom: 10 }}>
          <div className="health-card">
            <div className="tiny">Runtime + Database</div>
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
            <div className="tiny">Auth + Containers</div>
            <div className="health-kv">
              <span>OAuth Token</span>
              <span className={health.auth?.oauth_token_configured ? "health-good" : "health-bad"}>{health.auth?.oauth_token_configured ? "Configured" : "Missing"}</span>
            </div>
            <div className="health-kv">
              <span>Auth Errors (1h)</span>
              <span className={(health.auth?.auth_errors_1h || 0) > 0 ? "health-bad mono" : "mono"}>{health.auth?.auth_errors_1h || 0}</span>
            </div>
            <div className="health-kv">
              <span>Auth Errors (24h)</span>
              <span className={(health.auth?.auth_errors_24h || 0) > 0 ? "health-warn mono" : "mono"}>{health.auth?.auth_errors_24h || 0}</span>
            </div>
            {(health.containers || []).length === 0 ? <div className="empty-state">No container data</div> : (health.containers || []).map((x) => (
              <div className="health-kv" key={x.name}>
                <span>{x.name}</span>
                <span className={x.status === "running" ? "health-good mono" : "health-warn mono"}>{x.status}</span>
              </div>
            ))}
            {health.container_error ? <div className="health-bad mono" style={{ marginTop: 4 }}>{health.container_error}</div> : null}
            <div className="stack" style={{ marginTop: 8 }}>
              <button className="btn-secondary" onClick={() => openView?.("observability", "logs")}>Runtime Logs</button>
              <button className="btn-secondary" onClick={() => openView?.("observability", "incidents")}>Open Incidents</button>
            </div>
          </div>
        </div>

        <div className="tiny" style={{ marginBottom: 6 }}>Spend + Contract Diagnostics</div>
        <div className="row" style={{ marginBottom: 10 }}>
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
          <div className="health-card">
            <div className="tiny">Contract Versions</div>
            <div className="health-kv">
              <span>Workflow</span>
              <span>{contractWorkflow.name || "-"}</span>
            </div>
            <div className="health-kv">
              <span>Workflow Version</span>
              <span className="mono">{contractWorkflow.version || "-"}</span>
            </div>
            <div className="health-kv">
              <span>Platform Version</span>
              <span className="mono">{contractPlatform.version || "-"}</span>
            </div>
            <div className="health-kv">
              <span>Stages</span>
              <span className="mono">{Array.isArray(contractWorkflow.stage_ids) ? contractWorkflow.stage_ids.length : 0}</span>
            </div>
            <div className="health-kv">
              <span>Transitions</span>
              <span className="mono">{contractWorkflow.transition_count || 0}</span>
            </div>
            <div className="health-kv">
              <span>Timers</span>
              <span className="mono">{contractWorkflow.timer_count || 0}</span>
            </div>
          </div>
          <div className="health-card">
            <div className="tiny">Verification Summary</div>
            <div className="health-kv">
              <span>Verification Gates</span>
              <span className="mono">{contractVerification.count || 0}</span>
            </div>
            <div className="health-kv">
              <span>Compliance Rules</span>
              <span className="mono">{contractPlatform.compliance_rule_count || 0}</span>
            </div>
            <div className="health-kv">
              <span>Must Pass Gates</span>
              <span className="mono">{readPath(contractVerification, ["priority_counts", "must_pass"]) || "0"}</span>
            </div>
            <div className="health-kv">
              <span>Status</span>
              <span>{contractVerification.status || "-"}</span>
            </div>
            <div className="health-kv">
              <span>Latest Gate Results</span>
              <span>{contractVerification.latest_results || "-"}</span>
            </div>
            <div className="stack" style={{ marginTop: 8 }}>
              <button className="btn-secondary" onClick={() => openView?.("workflow", "artifacts")}>Open Workflow Artifacts</button>
              <button className="btn-secondary" onClick={() => openView?.("workflow", "issues")}>Open Workflow Issues</button>
            </div>
          </div>
        </div>

        <div className="tiny" style={{ marginBottom: 6 }}>Workflow Diagnostics</div>
        <div className="row" style={{ marginBottom: 10 }}>
          <div className="holding-detail-section">
            <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
              <div className="tiny">Workflow Audit Warnings</div>
              <button className="btn-secondary" onClick={() => openView?.("workflow", "issues")}>Open Workflow Issues</button>
            </div>
            {contractsData.error ? (
              <div className="health-bad">{contractsData.error}</div>
            ) : workflowWarnings.length === 0 ? (
              <div className="empty-state">No workflow audit warnings</div>
            ) : (
              <table>
                <thead><tr><th>Warning</th><th>Action</th></tr></thead>
                <tbody>
                  {workflowWarnings.map((item, idx) => (
                    <tr key={`${item}-${idx}`}>
                      <td>{item}</td>
                      <td><button className="btn-secondary" onClick={() => openView?.("workflow", "issues")}>Inspect</button></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
          <div className="holding-detail-section">
            <div className="tiny" style={{ marginBottom: 6 }}>Contract Paths</div>
            <div className="health-kv"><span>Workflow Spec</span><span className="mono">{contractsData.paths?.workflow || "-"}</span></div>
            <div className="health-kv"><span>Platform Spec</span><span className="mono">{contractsData.paths?.platform || "-"}</span></div>
            <div className="health-kv"><span>Verification Gates</span><span className="mono">{contractsData.paths?.verification || "-"}</span></div>
            <div className="stack" style={{ marginTop: 8 }}>
              <button className="btn-secondary" onClick={() => openView?.("workflow", "artifacts")}>Open Workflow</button>
            </div>
          </div>
        </div>
        <div className="tiny" style={{ marginBottom: 6 }}>Vertical Deploy Health</div>
        <div className="health-card">
          {(health.vertical_health || []).length === 0 ? <div className="empty-state">No vertical health data</div> : (
            <table>
              <thead><tr><th>Vertical</th><th>Health</th><th>Deploy</th><th>Actions</th></tr></thead>
              <tbody>
                {(health.vertical_health || []).slice(0, 200).map((v) => (
                  <tr key={`${v.vertical_id}-${v.slug}`}>
                    <td><button className="btn-secondary" onClick={() => openPortfolioForVertical?.(v.slug || v.vertical_id || "")}>{v.slug}</button></td>
                    <td><span className={v.health_status === "healthy" ? "health-good" : "health-warn"}>{v.health_status}</span></td>
                    <td><span className="mono">{v.deploy_status}</span></td>
                    <td>
                      <div className="stack">
                        <button className="btn-secondary" onClick={() => openPortfolioForVertical?.(v.slug || v.vertical_id || "")}>Portfolio</button>
                        <button className="btn-secondary" onClick={() => openWorkflowTraceForVertical?.(v.slug || v.vertical_id || "")}>Workflow</button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </section>
  );
}
