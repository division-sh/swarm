import React from "react";

export default function PortfolioDownstreamCard({
  focusSummary,
  downstream,
  onOpenOperations,
  onOpenTask,
  onOpenAgent,
}) {
  const current = downstream?.current;
  const vertical = focusSummary?.vertical;
  const primaryTask = current?.primaryTask;
  const primaryMailbox = current?.primaryMailbox;
  const primaryAgent = current?.primaryAgent;

  return (
    <div className="health-card" style={{ marginBottom: 10 }}>
      <div className="stack" style={{ justifyContent: "space-between", marginBottom: 8 }}>
        <div>
          <div className="tiny">Downstream Context</div>
          <div>{vertical ? `${vertical.slug || vertical.name || vertical.id} handoff state` : "Select a vertical to inspect downstream context"}</div>
        </div>
        <div className="stack tiny">
          <span className="badge">tasks {current?.summary.tasks || 0}</span>
          <span className="badge">mailbox {current?.summary.mailbox || 0}</span>
          <span className="badge">agents {current?.summary.agents || 0}</span>
        </div>
      </div>

      <div className="row" style={{ marginBottom: 8 }}>
        <div className="health-card">
          <div className="health-kv"><span>Primary Task</span><span className="mono">{primaryTask?.id || "-"}</span></div>
          <div className="health-kv"><span>Status</span><span>{primaryTask?.status || "-"}</span></div>
          <div className="health-kv"><span>Category</span><span>{primaryTask?.category || "-"}</span></div>
        </div>
        <div className="health-card">
          <div className="health-kv"><span>Primary Mailbox</span><span className="mono">{primaryMailbox?.id || "-"}</span></div>
          <div className="health-kv"><span>Status</span><span>{primaryMailbox?.status || "-"}</span></div>
          <div className="health-kv"><span>Priority</span><span>{primaryMailbox?.priority || "-"}</span></div>
        </div>
        <div className="health-card">
          <div className="health-kv"><span>Primary Agent</span><span className="mono">{primaryAgent?.id || primaryAgent?.agent_id || "-"}</span></div>
          <div className="health-kv"><span>Role</span><span>{primaryAgent?.role || "-"}</span></div>
          <div className="health-kv"><span>State</span><span>{primaryAgent?.state || primaryAgent?.status || "-"}</span></div>
        </div>
      </div>

      <div className="stack">
        <button className="btn-secondary" disabled={!vertical} onClick={onOpenOperations}>Open Operations</button>
        <button className="btn-secondary" disabled={!primaryTask} onClick={() => onOpenTask?.(primaryTask)}>Open Task</button>
        <button className="btn-secondary" disabled={!primaryAgent} onClick={() => onOpenAgent?.(primaryAgent?.id || primaryAgent?.agent_id || "")}>Open Agent</button>
      </div>
    </div>
  );
}
