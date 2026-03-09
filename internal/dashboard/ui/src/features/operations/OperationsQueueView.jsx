import React from "react";
import CopyID from "../../components/CopyID.jsx";
import { fmtTime, relTime } from "../../lib/format.js";

export default function OperationsQueueView({
  derived,
  onOpenMailbox,
  onOpenTask,
}) {
  const items = derived.queue.unified || [];

  return (
    <section>
      <div className="head">
        <h2>Needs Action</h2>
        <span className="tiny">Unified human intervention queue across mailbox and tasks.</span>
      </div>

      <div className="metrics-grid" style={{ marginBottom: 12 }}>
        <div className={`metric-card${derived.summary.pendingMailbox > 0 ? " warn" : ""}`}>
          <div className="metric-label">Mailbox</div>
          <div className="metric-value">{derived.summary.pendingMailbox}</div>
          <div className="tiny">{derived.summary.criticalMailbox} critical</div>
        </div>
        <div className={`metric-card${derived.summary.actionableTasks > 0 ? " warn" : ""}`}>
          <div className="metric-label">Tasks</div>
          <div className="metric-value">{derived.summary.actionableTasks}</div>
          <div className="tiny">{derived.summary.overdueTasks} overdue</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Focus</div>
          <div className="metric-value">{derived.focus ? derived.focus.type : "none"}</div>
          <div className="tiny">{derived.focus ? derived.focus.title : "Select an item below"}</div>
        </div>
      </div>

      <div className="body scroll" style={{ maxHeight: "62vh", padding: 0 }}>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Kind</th>
              <th>Title</th>
              <th>Priority</th>
              <th>Status</th>
              <th>Vertical</th>
              <th>Agent</th>
              <th>Age</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {items.length === 0 ? (
              <tr><td colSpan={9} className="empty-state">No mailbox items or actionable tasks require intervention.</td></tr>
            ) : items.map((item) => (
              <tr key={`${item.kind}:${item.id}`}>
                <td><CopyID id={item.id} /></td>
                <td><span className="badge">{item.kind}</span></td>
                <td>
                  <div>{item.title}</div>
                  <div className="tiny">{item.deadline ? `due ${relTime(item.deadline)}` : item.kind === "mailbox" ? "human decision" : "human task"}</div>
                </td>
                <td>{item.priority || "-"}</td>
                <td>{item.status || "-"}</td>
                <td className="mono">{item.vertical || "-"}</td>
                <td className="mono">{item.agent || "-"}</td>
                <td><span title={fmtTime(item.created_at)}>{relTime(item.created_at)}</span></td>
                <td>
                  {item.kind === "mailbox" ? (
                    <button onClick={() => onOpenMailbox(item.record)}>Open Mailbox</button>
                  ) : (
                    <button onClick={() => onOpenTask(item.record)}>Open Task</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
