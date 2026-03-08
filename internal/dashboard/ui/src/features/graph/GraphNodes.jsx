import React from "react";
import { Handle, Position } from "@xyflow/react";
import StatusDot from "../../components/StatusDot.jsx";
import { roleKeyFromAgentID } from "./graphTypes.js";

export function AgentNode({ data, selected }) {
  const role = data.role || roleKeyFromAgentID(data.id, "");
  const runtime = data.runtime;
  const state = runtime ? runtime.state : null;
  const isStuck = state === "stuck";
  const isLR = data.layoutDir !== "TB";
  const centered = data.forceLayout;
  const fill =
    data.group === "holding"
      ? "var(--graph-holding)"
      : data.group === "template"
        ? "var(--graph-template)"
        : "var(--graph-opco)";

  return (
    <div className={`rf-node rf-agent ${selected ? "selected" : ""} ${isStuck ? "rf-stuck" : ""} ${data.dimmed ? "rf-dimmed" : ""} ${data.highlighted ? "rf-highlighted" : ""}`} style={{ background: fill }}>
      <Handle type="target" position={centered ? Position.Top : (isLR ? Position.Left : Position.Top)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <Handle type="source" position={centered ? Position.Bottom : (isLR ? Position.Right : Position.Bottom)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <div className="rf-top">
        <div className="rf-role">{state ? <StatusDot state={state} /> : null}{role}</div>
        <div className="rf-status">{data.status || "-"}</div>
      </div>
      <div className="rf-node-meta">
        <span className="rf-chip">{data.group || "graph"}</span>
        {data.vertical_slug ? <span className="rf-chip rf-chip-soft">{data.vertical_slug}</span> : null}
        {runtime?.near_breaker ? <span className="rf-chip rf-chip-warn">breaker</span> : null}
      </div>
      <div className="rf-id mono">{data.id}</div>
      {runtime ? (
        <div className="rf-stats">
          {runtime.turn_limit > 0 ? (
            <div className="rf-mini-turn">
              <div className={`rf-mini-fill${Math.round((runtime.turn_count / runtime.turn_limit) * 100) >= 90 ? " warn" : ""}`} style={{ width: `${Math.min(100, Math.round((runtime.turn_count / runtime.turn_limit) * 100))}%` }} />
            </div>
          ) : null}
          <span className="rf-stat">{runtime.turn_count || 0}/{runtime.turn_limit || 0}t</span>
          <span className="rf-stat">{((runtime.total_tokens_24h || 0) / 1000).toFixed(0)}k</span>
          {(runtime.pending_events || 0) > 0 ? <span className="rf-pending">{runtime.pending_events}</span> : null}
        </div>
      ) : null}
      {isStuck && runtime.stuck_reason ? <div className="rf-stuck-reason" title={runtime.stuck_reason}>{runtime.stuck_reason}</div> : null}
    </div>
  );
}

export function EventNode({ data, selected }) {
  const isLR = data.layoutDir !== "TB";
  const centered = data.forceLayout;
  return (
    <div className={`rf-node rf-event ${selected ? "selected" : ""} ${data.dimmed ? "rf-dimmed" : ""} ${data.highlighted ? "rf-highlighted" : ""}`} style={{ background: "var(--graph-event)" }}>
      <Handle type="target" position={centered ? Position.Top : (isLR ? Position.Left : Position.Top)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <Handle type="source" position={centered ? Position.Bottom : (isLR ? Position.Right : Position.Bottom)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <div className="rf-node-meta">
        <span className="rf-chip rf-chip-soft">event</span>
      </div>
      <div className="rf-event-label mono">{data.label || data.id}</div>
      {data.subscriberCount > 0 ? <div className="rf-event-count">{data.subscriberCount} sub{data.subscriberCount !== 1 ? "s" : ""}</div> : null}
    </div>
  );
}

export function ControlNode({ data, selected }) {
  const isLR = data.layoutDir !== "TB";
  const centered = data.forceLayout;
  const kind = data.kind || "system";
  const systemClass = kind === "system" ? "rf-control-system" : kind === "mailbox" ? "rf-control-mailbox" : kind === "human" ? "rf-control-human" : "";
  return (
    <div className={`rf-node rf-control ${systemClass} ${selected ? "selected" : ""} ${data.dimmed ? "rf-dimmed" : ""} ${data.highlighted ? "rf-highlighted" : ""}`}>
      <Handle type="target" position={centered ? Position.Top : (isLR ? Position.Left : Position.Top)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <Handle type="source" position={centered ? Position.Bottom : (isLR ? Position.Right : Position.Bottom)} className={`rf-handle ${centered ? "rf-handle-center" : ""}`} />
      <div className="rf-node-meta">
        <span className="rf-chip rf-chip-soft">{kind.toUpperCase()}</span>
      </div>
      <div className="rf-control-kind">{kind.toUpperCase()}</div>
      <div className="rf-control-label">{data.label || data.id}</div>
      <div className="rf-id mono">{data.id}</div>
    </div>
  );
}
