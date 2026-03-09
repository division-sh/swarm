import React from "react";
import { relTime } from "../../lib/format.js";

function SummaryCard({ label, value, detail, tone = "" }) {
  return (
    <div className={`metric-card${tone ? ` ${tone}` : ""}`}>
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}</div>
      {detail ? <div className="tiny">{detail}</div> : null}
    </div>
  );
}

function ActionRow({ label, meta, actionLabel, onAction }) {
  return (
    <div className="health-kv">
      <div>
        <div>{label}</div>
        {meta ? <div className="tiny">{meta}</div> : null}
      </div>
      <button className="btn-secondary" onClick={onAction}>{actionLabel}</button>
    </div>
  );
}

export default function OperationsTriageSummary({
  derived,
  onOpenMailbox,
  onOpenTask,
}) {
  const { summary, queue, focus, related } = derived;

  return (
    <section className="card" style={{ marginBottom: 12 }}>
      <div className="head">
        <h2>Operations Triage</h2>
        <span className="tiny">Mailbox decisions and human tasks in one operator queue.</span>
      </div>

      <div className="metrics-grid" style={{ marginBottom: 12 }}>
        <SummaryCard label="Pending Mailbox" value={summary.pendingMailbox} detail={`${summary.criticalMailbox} critical`} tone={summary.pendingMailbox > 0 ? "warn" : ""} />
        <SummaryCard label="Actionable Tasks" value={summary.actionableTasks} detail={`${summary.overdueTasks} overdue`} tone={summary.actionableTasks > 0 ? "warn" : ""} />
        <SummaryCard label="Review Tasks" value={summary.reviewTasks} detail={`${summary.highPriorityTasks} high priority`} />
        <SummaryCard label="Current Focus" value={focus ? focus.type : "none"} detail={focus ? focus.title : "Select a mailbox item or task"} />
      </div>

      <div className="row body">
        <div>
          <div className="tiny" style={{ marginBottom: 8 }}>Needs Action</div>
          <div className="body" style={{ gap: 10 }}>
            {queue.mailbox.length > 0 ? (
              <div className="card">
                <div className="tiny" style={{ marginBottom: 8 }}>Mailbox Decisions</div>
                <div className="body" style={{ gap: 8 }}>
                  {queue.mailbox.map((item) => (
                    <ActionRow
                      key={item.id}
                      label={item.summary || item.type || item.id}
                      meta={[item.from_agent, item.vertical_slug || item.vertical_id, item.priority].filter(Boolean).join(" · ")}
                      actionLabel="Open Mailbox"
                      onAction={() => onOpenMailbox(item)}
                    />
                  ))}
                </div>
              </div>
            ) : null}

            {queue.tasks.length > 0 ? (
              <div className="card">
                <div className="tiny" style={{ marginBottom: 8 }}>Human Tasks</div>
                <div className="body" style={{ gap: 8 }}>
                  {queue.tasks.map((task) => (
                    <ActionRow
                      key={task.id}
                      label={task.description || task.category || task.id}
                      meta={[task.requesting_agent, task.vertical_slug, task.priority, task.deadline ? `due ${relTime(task.deadline)}` : ""].filter(Boolean).join(" · ")}
                      actionLabel="Open Task"
                      onAction={() => onOpenTask(task)}
                    />
                  ))}
                </div>
              </div>
            ) : null}

            {queue.mailbox.length === 0 && queue.tasks.length === 0 ? (
              <div className="empty-state">No pending mailbox items or actionable tasks.</div>
            ) : null}
          </div>
        </div>

        <div>
          <div className="tiny" style={{ marginBottom: 8 }}>Current Focus</div>
          {focus ? (
            <div className="card">
              <div className="body" style={{ gap: 8 }}>
                <div className="health-kv"><span>Type</span><span className="badge">{focus.type}</span></div>
                <div className="health-kv"><span>Status</span><span>{focus.status || "-"}</span></div>
                <div className="health-kv"><span>Vertical</span><span className="mono">{focus.vertical || "-"}</span></div>
                <div className="health-kv"><span>Agent</span><span className="mono">{focus.agent || "-"}</span></div>
                <div className="tiny">{focus.title}</div>
              </div>
            </div>
          ) : (
            <div className="empty-state">Select a mailbox item or task to keep operator context visible across the tab.</div>
          )}

          {focus ? (
            <div className="card" style={{ marginTop: 10 }}>
              <div className="tiny" style={{ marginBottom: 8 }}>Related Queue</div>
              <div className="body" style={{ gap: 8 }}>
                <div className="health-kv"><span>Related Mailbox</span><span>{related.mailbox.length}</span></div>
                <div className="health-kv"><span>Related Tasks</span><span>{related.tasks.length}</span></div>
                {related.mailbox[0] ? (
                  <ActionRow
                    label={related.mailbox[0].summary || related.mailbox[0].type || related.mailbox[0].id}
                    meta={related.mailbox[0].from_agent || related.mailbox[0].vertical_slug || related.mailbox[0].vertical_id || ""}
                    actionLabel="Mailbox"
                    onAction={() => onOpenMailbox(related.mailbox[0])}
                  />
                ) : null}
                {related.tasks[0] ? (
                  <ActionRow
                    label={related.tasks[0].description || related.tasks[0].category || related.tasks[0].id}
                    meta={related.tasks[0].requesting_agent || related.tasks[0].vertical_slug || ""}
                    actionLabel="Task"
                    onAction={() => onOpenTask(related.tasks[0])}
                  />
                ) : null}
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </section>
  );
}
