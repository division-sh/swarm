import React from "react";
import JsonBlock from "../../components/JsonBlock.jsx";

export default function HealthView({
  health,
  contractsData,
  contractWorkflow,
  contractPlatform,
  contractVerification,
  formatDollars,
  readPath,
}) {
  return (
    <section>
      <div className="head"><h2>Health</h2><span className="tiny">ops telemetry + contract summary</span></div>
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
        <div className="row" style={{ marginBottom: 10 }}>
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
          </div>
        </div>
        <div className="row" style={{ marginBottom: 10 }}>
          <div className="holding-detail-section">
            <div className="tiny" style={{ marginBottom: 6 }}>Workflow Audit Warnings</div>
            {contractsData.error ? (
              <div className="health-bad">{contractsData.error}</div>
            ) : !Array.isArray(health.workflow_audit?.warnings) || health.workflow_audit.warnings.length === 0 ? (
              <div className="empty-state">No workflow audit warnings</div>
            ) : (
              <table>
                <thead><tr><th>Warning</th></tr></thead>
                <tbody>
                  {health.workflow_audit.warnings.map((item, idx) => (
                    <tr key={`${item}-${idx}`}>
                      <td>{item}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
          <div className="holding-detail-section">
            <div className="tiny" style={{ marginBottom: 6 }}>Contract Paths</div>
            <JsonBlock data={contractsData.paths || {}} defaultOpen={2} />
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
  );
}
