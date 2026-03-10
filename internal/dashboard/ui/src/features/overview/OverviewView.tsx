import React from "react";
import CopyID from "../../components/CopyID.tsx";
import { fmtTime, relTime } from "../../lib/format.ts";

function SummaryCard({ label, value, tone = "", detail = "", cta = "", onClick }) {
  return (
    <div className={`health-card ${tone}`.trim()}>
      <div className="tiny">{label}</div>
      <div style={{ fontSize: 28, fontWeight: 700, marginTop: 6 }}>{value}</div>
      {detail ? <div className="tiny" style={{ marginTop: 6 }}>{detail}</div> : null}
      {cta ? (
        <div className="stack" style={{ marginTop: 10 }}>
          <button className="btn-secondary" onClick={onClick}>{cta}</button>
        </div>
      ) : null}
    </div>
  );
}

export default function OverviewView({ state, actions }) {
  const {
    overview,
    digestResp,
    agentsResp,
    incidentsData,
    mailbox,
    tasksResp,
    health,
    funnel,
    holdingData,
    derived,
  } = state;
  const { openView } = actions;

  const stuckAgents = (agentsResp.states && agentsResp.states.stuck) || 0;
  const pendingMailbox = mailbox.summary?.pending || 0;
  const openTasks = Array.isArray(tasksResp.tasks) ? tasksResp.tasks.length : 0;
  const driftCount = holdingData.workflow_summary?.drift || 0;
  const workflowWarnings = Array.isArray(health.workflow_audit?.warnings) ? health.workflow_audit.warnings : [];
  const topStuckAgents = (agentsResp.agents || []).filter((agent) => agent.state === "stuck").slice(0, 8);
  const triageVerticals = (holdingData.verticals || [])
    .filter((vertical) => vertical.workflow_current_stage !== vertical.stage || (vertical.active_timer_count || 0) > 0 || Number(vertical.revision_count || 0) > 0)
    .slice(0, 8);
  const recentIncidents = (incidentsData || []).slice(0, 8);
  const digestText = digestResp?.current?.text || "";
  const urgentNow = derived?.urgentNow || [];

  return (
    <section>
      <div className="head">
        <h2>Overview</h2>
        <span className="tiny">landing page for current runtime, workflow, and portfolio attention</span>
      </div>
      <div className="body scroll">
        <div className="row3" style={{ marginBottom: 10 }}>
          <SummaryCard
            label="Runtime"
            value={health.runtime?.running ? "Running" : "Stopped"}
            tone={health.runtime?.running ? "" : "health-bad"}
            detail={`${overview.agents_active || 0} active agents`}
            cta="Open Health"
            onClick={() => openView("health")}
          />
          <SummaryCard
            label="Stuck Agents"
            value={stuckAgents}
            tone={stuckAgents > 0 ? "health-warn" : ""}
            detail={`${overview.events_24h || 0} events in the last 24h`}
            cta="Open Agents"
            onClick={() => openView("agents")}
          />
          <SummaryCard
            label="Incidents"
            value={(incidentsData || []).length}
            tone={(incidentsData || []).length > 0 ? "health-bad" : ""}
            detail={`last refresh ${fmtTime(overview.generated_at)}`}
            cta="Open Observability"
            onClick={() => openView("observability", "incidents")}
          />
          <SummaryCard
            label="Pending Mailbox"
            value={pendingMailbox}
            tone={pendingMailbox > 0 ? "health-warn" : ""}
            detail={`${openTasks} open tasks loaded`}
            cta="Open Queue"
            onClick={() => openView("operations", "queue")}
          />
          <SummaryCard
            label="Workflow Drift"
            value={driftCount}
            tone={driftCount > 0 ? "health-warn" : ""}
            detail={`${holdingData.workflow_summary?.active_timers || 0} active timers, ${holdingData.workflow_summary?.revisioned || 0} revisioned`}
            cta="Open Portfolio"
            onClick={() => openView("portfolio", "holding")}
          />
          <SummaryCard
            label="Pipeline Blockers"
            value={(funnel.stuck || []).length}
            tone={(funnel.stuck || []).length > 0 ? "health-warn" : ""}
            detail={`${funnel.throughput?.discoveries_14d || 0} discoveries in 14d`}
            cta="Open Portfolio"
            onClick={() => openView("portfolio", "pipeline")}
          />
        </div>

        <div className="holding-detail-section" style={{ marginBottom: 10 }}>
          <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
            <div className="tiny">Urgent Now</div>
            <button className="btn-secondary" onClick={() => openView("operations", "queue")}>Open Queue</button>
          </div>
          {urgentNow.length === 0 ? (
            <div className="empty-state">No urgent cross-surface items detected.</div>
          ) : (
            <table>
              <thead><tr><th>Kind</th><th>Item</th><th>Context</th><th>Action</th></tr></thead>
              <tbody>
                {urgentNow.map((item) => (
                  <tr key={`${item.kind}:${item.id}`}>
                    <td><span className="badge">{item.kind}</span></td>
                    <td>{item.title}</td>
                    <td className="tiny">{item.subtitle || "-"}</td>
                    <td><button className="btn-secondary" onClick={() => openView(item.route.view, item.route.subview)}>Open</button></td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="row" style={{ marginBottom: 10 }}>
          <div className="holding-detail-section">
            <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
              <div className="tiny">Workflow Audit Warnings</div>
              <button className="btn-secondary" onClick={() => openView("health")}>Open Health</button>
            </div>
            {workflowWarnings.length === 0 ? (
              <div className="empty-state">No workflow audit warnings</div>
            ) : (
              <table>
                <thead><tr><th>Warning</th></tr></thead>
                <tbody>
                  {workflowWarnings.slice(0, 8).map((warning, index) => (
                    <tr key={`${warning}-${index}`}>
                      <td>{warning}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
          <div className="holding-detail-section">
            <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
              <div className="tiny">Mailbox and Task Queue</div>
              <button className="btn-secondary" onClick={() => openView("operations", "queue")}>Open Queue</button>
            </div>
            <div className="health-kv"><span>Pending Mailbox</span><span className="mono">{pendingMailbox}</span></div>
            <div className="health-kv"><span>Approved</span><span className="mono">{mailbox.summary?.approved || 0}</span></div>
            <div className="health-kv"><span>Rejected</span><span className="mono">{mailbox.summary?.rejected || 0}</span></div>
            <div className="health-kv"><span>Deferred</span><span className="mono">{mailbox.summary?.deferred || 0}</span></div>
            <div className="health-kv"><span>Open Tasks Loaded</span><span className="mono">{openTasks}</span></div>
            {tasksResp.weekly_budget ? (
              <div className="tiny" style={{ marginTop: 8 }}>
                Weekly budget {tasksResp.weekly_budget.approved_this_week || 0}/{tasksResp.weekly_budget.max_tasks_per_week || 0}
              </div>
            ) : null}
          </div>
        </div>

        <div className="row" style={{ marginBottom: 10 }}>
          <div className="holding-detail-section">
            <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
              <div className="tiny">Stuck Agents</div>
              <button className="btn-secondary" onClick={() => openView("agents")}>Open Agents</button>
            </div>
            {topStuckAgents.length === 0 ? (
              <div className="empty-state">No stuck agents</div>
            ) : (
              <table>
                <thead><tr><th>Agent</th><th>Role</th><th>Vertical</th><th>Pending</th></tr></thead>
                <tbody>
                  {topStuckAgents.map((agent) => (
                    <tr key={agent.id}>
                      <td><CopyID id={agent.id} len={12} /></td>
                      <td>{agent.role || "-"}</td>
                      <td>{agent.vertical_slug || agent.vertical_id || "holding"}</td>
                      <td className="mono">
                        <button className="btn-secondary" onClick={() => openView("agents")}>{agent.pending_events || 0} pending</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
          <div className="holding-detail-section">
            <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
              <div className="tiny">Workflow Triage</div>
              <button className="btn-secondary" onClick={() => openView("portfolio", "holding")}>Open Portfolio</button>
            </div>
            {triageVerticals.length === 0 ? (
              <div className="empty-state">No triage hotspots</div>
            ) : (
              <table>
                <thead><tr><th>Vertical</th><th>Stage Age</th><th>Timers</th><th>Drift</th></tr></thead>
                <tbody>
                  {triageVerticals.map((vertical) => (
                    <tr key={vertical.id}>
                      <td><button className="btn-secondary" onClick={() => openView("portfolio", "holding")}>{vertical.slug || vertical.name || vertical.id}</button></td>
                      <td>{vertical.stage_entered_at ? relTime(vertical.stage_entered_at) : "-"}</td>
                      <td className="mono">{vertical.active_timer_count || 0}</td>
                      <td>{vertical.workflow_current_stage && vertical.workflow_current_stage !== vertical.stage ? "yes" : "no"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        <div className="row" style={{ marginBottom: 10 }}>
          <div className="holding-detail-section">
            <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
              <div className="tiny">Recent Incidents</div>
              <button className="btn-secondary" onClick={() => openView("observability", "incidents")}>Open Observability</button>
            </div>
            {recentIncidents.length === 0 ? (
              <div className="empty-state">No incidents in current window</div>
            ) : (
              <table>
                <thead><tr><th>Code</th><th>Count</th><th>Last Seen</th></tr></thead>
                <tbody>
                  {recentIncidents.map((incident) => (
                    <tr key={incident.code}>
                      <td className="mono"><button className="btn-secondary" onClick={() => openView("observability", "incidents")}>{incident.code}</button></td>
                      <td className="mono">{incident.count || 0}</td>
                      <td title={fmtTime(incident.last_seen)}>{relTime(incident.last_seen)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
          <div className="holding-detail-section">
            <div className="stack" style={{ justifyContent: "space-between", marginBottom: 6 }}>
              <div className="tiny">Executive Snapshot</div>
              <button className="btn-secondary" onClick={() => openView("health")}>Open Health</button>
            </div>
            <div className="tiny" style={{ marginBottom: 6 }}>
              Last compiled {digestResp?.last_compiled?.at ? relTime(digestResp.last_compiled.at) : "never"}
            </div>
            <pre className="json" style={{ whiteSpace: "pre-wrap", maxHeight: 220 }}>
              {digestText ? digestText.slice(0, 1800) : "No digest available."}
            </pre>
          </div>
        </div>
      </div>
    </section>
  );
}
