import React from "react";

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
  onOpenQueue,
  onOpenControl,
  onOpenTasksView,
}) {
  const { summary, focus, related } = derived;

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
        <div className="card">
          <div className="tiny" style={{ marginBottom: 8 }}>Workspace Shortcuts</div>
          <div className="body" style={{ gap: 8 }}>
            <ActionRow label="Needs Action Queue" meta="Merged mailbox and human task urgency list." actionLabel="Open Queue" onAction={onOpenQueue} />
            <ActionRow label="Mailbox Decisions" meta={`${summary.pendingMailbox} pending items`} actionLabel="Open Mailbox" onAction={onOpenControl} />
            <ActionRow label="Human Tasks" meta={`${summary.actionableTasks} actionable tasks`} actionLabel="Open Tasks" onAction={onOpenTasksView} />
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
