import React from "react";
import CopyID from "../../components/CopyID.jsx";
import { fmtTime, relTime } from "../../lib/format.js";
import { deriveTasksDerivedState } from "./useTasksDerivedState.js";

export default function TasksView({ state, actions, onOpenWorkflowTrace, onOpenPortfolio, onOpenRelatedMailboxForVertical }) {
  const { tasksResp, tasksStats, selectedTask, taskStatus, selectedTaskID, taskResultText, taskOutcome, taskFollowUpNeeded, taskRejectReason } = state;
  const { setTaskStatus, setSelectedTaskID, setTaskResultText, setTaskOutcome, setTaskFollowUpNeeded, setTaskRejectReason, refreshTasks, loadTaskStats, claimSelectedTask, completeSelectedTask, rejectSelectedTask } = actions;
  const derived = deriveTasksDerivedState({ tasksResp, tasksStats });

  function openTask(task) {
    setSelectedTaskID(task?.id || "");
  }

  const guidance = selectedTask
    ? selectedTask.status === "pending_review"
      ? "This task is waiting on human review. Complete it when the review package is resolved."
      : selectedTask.status === "assigned"
        ? "This task is already assigned. Claim only if you are taking over ownership."
        : "Use Complete when the work is done and Reject when the request cannot be executed safely."
    : "";

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

      <div className="metrics-grid" style={{ marginBottom: 12 }}>
        <div className={`metric-card${derived.summary.actionable > 0 ? " warn" : ""}`}>
          <div className="metric-label">Actionable</div>
          <div className="metric-value">{derived.summary.actionable}</div>
          <div className="tiny">{derived.summary.overdue} overdue, {derived.summary.review} review</div>
        </div>
        <div className={`metric-card${derived.summary.assigned > 0 ? " warn" : ""}`}>
          <div className="metric-label">Assigned</div>
          <div className="metric-value">{derived.summary.assigned}</div>
          <div className="tiny">{derived.summary.loaded} loaded in this filter</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Completed</div>
          <div className="metric-value">{derived.summary.completed}</div>
          <div className="tiny">{derived.summary.rejected} rejected</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Weekly Budget</div>
          <div className="metric-value">{derived.summary.budgetUsed}/{derived.summary.budgetMax || "-"}</div>
          <div className="tiny">reset {derived.budget.resetDay} · week start {derived.budget.weekStart || "-"}</div>
        </div>
      </div>

      <div className="row body" style={{ marginBottom: 12 }}>
        <div className="card">
          <div className="tiny" style={{ marginBottom: 8 }}>Needs Attention</div>
          <div className="body" style={{ gap: 8 }}>
            {derived.queue.overdue.length > 0 ? derived.queue.overdue.map((task) => (
              <div key={task.id} className="health-kv">
                <div>
                  <div>{task.description || task.category || task.id}</div>
                  <div className="tiny">{[task.vertical_slug, task.requesting_agent, task.deadline ? `due ${relTime(task.deadline)}` : ""].filter(Boolean).join(" · ")}</div>
                </div>
                <button className="btn-secondary" onClick={() => openTask(task)}>Open</button>
              </div>
            )) : (
              <div className="empty-state">No overdue tasks in this filter.</div>
            )}
          </div>
        </div>

        <div className="card">
          <div className="tiny" style={{ marginBottom: 8 }}>Review Queue</div>
          <div className="body" style={{ gap: 8 }}>
            {derived.queue.review.length > 0 ? derived.queue.review.map((task) => (
              <div key={task.id} className="health-kv">
                <div>
                  <div>{task.description || task.category || task.id}</div>
                  <div className="tiny">{[task.vertical_slug, task.requesting_agent, task.priority].filter(Boolean).join(" · ")}</div>
                </div>
                <button className="btn-secondary" onClick={() => openTask(task)}>Review</button>
              </div>
            )) : (
              <div className="empty-state">No review tasks in this filter.</div>
            )}
          </div>
        </div>
      </div>

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
                <div className="card" style={{ marginBottom: 8 }}>
                  <div className="tiny" style={{ marginBottom: 4 }}>Operator Guidance</div>
                  <div className="tiny">{guidance}</div>
                </div>
                {selectedTask.vertical_slug ? (
                  <div className="stack" style={{ marginBottom: 8 }}>
                    <button className="btn-secondary" onClick={() => onOpenWorkflowTrace?.(selectedTask.vertical_slug)}>Workflow</button>
                    <button className="btn-secondary" onClick={() => onOpenPortfolio?.(selectedTask.vertical_slug)}>Portfolio</button>
                    <button className="btn-secondary" onClick={() => onOpenRelatedMailboxForVertical?.(selectedTask.vertical_slug)}>Related Mailbox</button>
                  </div>
                ) : null}
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
