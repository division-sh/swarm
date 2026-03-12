import React from "react";
import JsonBlock from "../../components/JsonBlock.tsx";
import { fmtTime, readPath, relTime } from "../../lib/format.ts";

export default function HoldingWorkflowPanel({ workflowState, workflowStateError }) {
  const activeTimers = Array.isArray(workflowState && workflowState.timer_state)
    ? workflowState.timer_state.filter((timer) => !timer.cancelled)
    : [];

  return (
    <div className="holding-detail-section">
      <div className="tiny" style={{ marginBottom: 6 }}>Workflow State</div>
      {workflowStateError ? (
        <div className="health-warn" style={{ marginBottom: 8 }}>{workflowStateError}</div>
      ) : null}
      {!workflowState ? (
        <div className="empty-state">No persisted workflow instance</div>
      ) : (
        <>
          <div className="row">
            <div className="health-card">
              <div className="tiny">Runtime State</div>
              <div className="health-kv"><span>Workflow</span><span>{workflowState.workflow_name || "-"}</span></div>
              <div className="health-kv"><span>Version</span><span className="mono">{workflowState.workflow_version || "-"}</span></div>
              <div className="health-kv"><span>Current State</span><span>{workflowState.current_state || "-"}</span></div>
              <div className="health-kv"><span>Entered Stage</span><span title={fmtTime(workflowState.entered_stage_at)}>{relTime(workflowState.entered_stage_at)}</span></div>
              <div className="health-kv"><span>Transitions</span><span className="mono">{workflowState.transition_count || 0}</span></div>
              <div className="health-kv"><span>Active Timers</span><span className="mono">{workflowState.active_timer_count || 0}</span></div>
            </div>
            <div className="health-card">
              <div className="tiny">Workflow Metadata</div>
              <div className="health-kv"><span>Instance</span><span className="mono">{workflowState.instance_id || "-"}</span></div>
              <div className="health-kv"><span>Created</span><span>{fmtTime(workflowState.created_at)}</span></div>
              <div className="health-kv"><span>Updated</span><span>{fmtTime(workflowState.updated_at)}</span></div>
              <div className="health-kv"><span>Revision Count</span><span className="mono">{readPath(workflowState, ["metadata", "revision_count"]) || "0"}</span></div>
              <div className="health-kv"><span>Metadata Keys</span><span className="mono">{workflowState.metadata && typeof workflowState.metadata === "object" ? Object.keys(workflowState.metadata).length : 0}</span></div>
              <div className="health-kv"><span>State Buckets</span><span className="mono">{workflowState.state_buckets && typeof workflowState.state_buckets === "object" ? Object.keys(workflowState.state_buckets).length : 0}</span></div>
            </div>
          </div>

          <div className="row">
            <div className="holding-detail-section">
              <div className="tiny" style={{ marginBottom: 6 }}>Transition History</div>
              {!Array.isArray(workflowState.transition_history) || workflowState.transition_history.length === 0 ? (
                <div className="empty-state">No transitions recorded</div>
              ) : (
                <table>
                  <thead><tr><th>When</th><th>Transition</th><th>From</th><th>To</th></tr></thead>
                  <tbody>
                    {workflowState.transition_history.slice(-20).reverse().map((item, index) => (
                      <tr key={`${item.transition_id || "transition"}-${index}`}>
                        <td title={fmtTime(item.fired_at)}>{relTime(item.fired_at)}</td>
                        <td className="mono">{item.transition_id || "-"}</td>
                        <td>{item.from || "-"}</td>
                        <td>{item.to || "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
            <div className="holding-detail-section">
              <div className="tiny" style={{ marginBottom: 6 }}>Timers</div>
              {activeTimers.length === 0 ? (
                <div className="empty-state">No active timers</div>
              ) : (
                <table>
                  <thead><tr><th>Timer</th><th>Created</th><th>Fires</th><th>Status</th></tr></thead>
                  <tbody>
                    {activeTimers.slice(0, 20).map((timer, index) => (
                      <tr key={`${timer.timer_id || "timer"}-${index}`}>
                        <td className="mono">{timer.timer_id || "-"}</td>
                        <td title={fmtTime(timer.created_at)}>{relTime(timer.created_at)}</td>
                        <td title={fmtTime(timer.fires_at)}>{relTime(timer.fires_at)}</td>
                        <td>{timer.cancelled ? "cancelled" : "active"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>

          <div className="row">
            <div className="holding-detail-section">
              <div className="tiny" style={{ marginBottom: 6 }}>State Buckets</div>
              <JsonBlock data={workflowState.state_buckets || {}} defaultOpen={2} />
            </div>
            <div className="holding-detail-section">
              <div className="tiny" style={{ marginBottom: 6 }}>Workflow Metadata JSON</div>
              <JsonBlock data={workflowState.metadata || {}} defaultOpen={2} />
            </div>
          </div>
        </>
      )}
    </div>
  );
}
