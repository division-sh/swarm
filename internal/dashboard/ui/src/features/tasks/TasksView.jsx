import React from "react";
import CopyID from "../../components/CopyID.jsx";

export default function TasksView({
  domain,
  controls,
  actions,
  helpers,
}) {
  const { tasksResp, tasksStats, selectedTask } = domain;
  const {
    taskStatus,
    setTaskStatus,
    selectedTaskID,
    setSelectedTaskID,
    taskResultText,
    setTaskResultText,
    taskOutcome,
    setTaskOutcome,
    taskFollowUpNeeded,
    setTaskFollowUpNeeded,
    taskRejectReason,
    setTaskRejectReason,
  } = controls;
  const { refreshTasks, loadTaskStats, claimSelectedTask, completeSelectedTask, rejectSelectedTask } = actions;
  const { fmtTime, relTime } = helpers;

  return (
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
          <button onClick={() => refreshTasks().catch(() => {})}>Refresh</button>
          <button className="btn-secondary" onClick={() => loadTaskStats().catch(() => {})}>Stats</button>
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
                  <td><span className="badge">{t.status}</span></td>
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
                  <button className="btn-secondary" onClick={() => claimSelectedTask().catch(() => {})}>Claim</button>
                  <button onClick={() => completeSelectedTask().catch(() => {})}>Complete</button>
                </div>
                <div className="tiny" style={{ marginTop: 10 }}>Reject (human pushback)</div>
                <textarea
                  placeholder="Why you can't do it / what blocks execution..."
                  value={taskRejectReason}
                  onChange={(e) => setTaskRejectReason(e.target.value)}
                />
                <div className="stack" style={{ marginTop: 6 }}>
                  <button className="btn-secondary" onClick={() => rejectSelectedTask().catch(() => {})}>Reject</button>
                </div>
              </div>
            </div>
          ) : (
            <div className="empty-state">Select a task to claim/complete it.</div>
          )}
        </div>
      </div>
    </section>
  );
}
