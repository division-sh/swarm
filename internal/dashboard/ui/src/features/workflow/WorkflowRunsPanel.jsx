import React, { useMemo, useState } from "react";
import DataTable from "../../components/DataTable.jsx";
import { fmtTime, relTime } from "../../lib/format.ts";

function deriveRuns(verticals, events) {
  const eventGroups = new Map();
  for (const event of events || []) {
    const key = String(event.vertical_id || "").trim();
    if (!key) continue;
    if (!eventGroups.has(key)) eventGroups.set(key, []);
    eventGroups.get(key).push(event);
  }

  return (verticals || []).map((vertical) => {
    const key = String(vertical.id || "").trim();
    const rows = [...(eventGroups.get(key) || [])].sort((a, b) => `${b.timestamp || ""}`.localeCompare(`${a.timestamp || ""}`));
    const latest = rows[0] || null;
    const intercepted = rows.filter((row) => row.intercepted).length;
    const passthrough = rows.filter((row) => row.passthrough).length;
    const drift = vertical.workflow_current_stage && vertical.workflow_current_stage !== vertical.stage;
    const revisions = Number(vertical.revision_count || 0);
    const timers = Number(vertical.active_timer_count || 0);
    const issueScore = (drift ? 5 : 0) + Math.min(revisions, 4) + Math.min(timers, 4) + Math.min(intercepted, 5) + Math.min(passthrough, 3);
    return {
      ...vertical,
      run_event_count: rows.length,
      latest_event_at: latest?.timestamp || "",
      latest_event_type: latest?.event_type || "",
      intercepted_count: intercepted,
      passthrough_count: passthrough,
      issue_score: issueScore,
      drift,
    };
  }).sort((a, b) => (
    Number(b.issue_score || 0) - Number(a.issue_score || 0)
      || `${b.latest_event_at || ""}`.localeCompare(`${a.latest_event_at || ""}`)
      || `${a.slug || a.name || a.id || ""}`.localeCompare(`${b.slug || b.name || b.id || ""}`)
  ));
}

export default function WorkflowRunsPanel(props) {
  const params = props.params || {};
  const flow = params.flow;
  const graph = params.graph;
  const openTopologyForVertical = params.openTopologyForVertical;
  const openFlowForVertical = params.openFlowForVertical;
  const [filter, setFilter] = useState("attention");
  const flowState = flow?.state || {};
  const graphState = graph?.state || {};

  const runs = useMemo(
    () => deriveRuns(flowState.verticals || [], flowState.flowEvents || []),
    [flowState.flowEvents, flowState.verticals],
  );

  const selectedVertical = flowState.flowVertical || graphState.graphVertical || "";
  const filteredRuns = useMemo(() => {
    switch (filter) {
      case "all":
        return runs;
      case "active":
        return runs.filter((run) => run.run_event_count > 0);
      case "drift":
        return runs.filter((run) => run.drift);
      case "timers":
        return runs.filter((run) => Number(run.active_timer_count || 0) > 0);
      case "attention":
      default:
        return runs.filter((run) => Number(run.issue_score || 0) > 0);
    }
  }, [filter, runs]);

  const columns = useMemo(() => ([
    {
      accessorKey: "slug",
      header: "Run",
      cell: ({ row }) => (
        <div className="stack" style={{ gap: 4 }}>
          <button
            className="btn-secondary mono"
            onClick={() => openFlowForVertical?.(row.original.id, "")}
          >
            {row.original.slug || row.original.name || row.original.id}
          </button>
          <span className="tiny mono">{row.original.id}</span>
        </div>
      ),
    },
    {
      accessorKey: "workflow_current_stage",
      header: "Stage",
      cell: ({ row }) => (
        <div>
          <div>{row.original.workflow_current_stage || row.original.stage || "-"}</div>
          {row.original.drift ? <div className="tiny health-warn">db {row.original.stage}</div> : null}
        </div>
      ),
    },
    {
      accessorKey: "run_event_count",
      header: "Events",
      cell: ({ row }) => <span className="mono">{row.original.run_event_count || 0}</span>,
    },
    {
      accessorKey: "latest_event_at",
      header: "Latest",
      cell: ({ row }) => (
        <span title={fmtTime(row.original.latest_event_at)}>
          {row.original.latest_event_at ? relTime(row.original.latest_event_at) : "-"}
        </span>
      ),
    },
    {
      accessorKey: "issue_score",
      header: "Risk",
      cell: ({ row }) => <span className="mono">{row.original.issue_score || 0}</span>,
    },
  ]), [openFlowForVertical]);

  if (!flow || !graph) return null;

  const summary = {
    total: runs.length,
    active: runs.filter((run) => run.run_event_count > 0).length,
    drift: runs.filter((run) => run.drift).length,
    timers: runs.filter((run) => Number(run.active_timer_count || 0) > 0).length,
  };

  return (
    <div className="workflow-dock-panel">
      <div className="head">
        <h2>Runs</h2>
        <div className="stack tiny">
          <button className={filter === "attention" ? "active" : ""} onClick={() => setFilter("attention")}>attention</button>
          <button className={filter === "active" ? "active" : ""} onClick={() => setFilter("active")}>active</button>
          <button className={filter === "drift" ? "active" : ""} onClick={() => setFilter("drift")}>drift</button>
          <button className={filter === "timers" ? "active" : ""} onClick={() => setFilter("timers")}>timers</button>
          <button className={filter === "all" ? "active" : ""} onClick={() => setFilter("all")}>all</button>
        </div>
      </div>
      <div className="body scroll">
        <div className="quad-grid" style={{ marginBottom: 10 }}>
          <div className="health-card">
            <div className="tiny">Runs</div>
            <div className="big-number">{summary.total}</div>
            <div className="tiny">visible workflow runs</div>
          </div>
          <div className="health-card">
            <div className="tiny">Active</div>
            <div className="big-number">{summary.active}</div>
            <div className="tiny">with flow events</div>
          </div>
          <div className="health-card">
            <div className="tiny">Drift</div>
            <div className="big-number">{summary.drift}</div>
            <div className="tiny">db vs workflow stage mismatch</div>
          </div>
          <div className="health-card">
            <div className="tiny">Timers</div>
            <div className="big-number">{summary.timers}</div>
            <div className="tiny">active workflow timers</div>
          </div>
        </div>

        <div className="health-card" style={{ marginBottom: 10 }}>
          <div className="tiny">Run Focus</div>
          <div className="health-kv"><span>Selected Vertical</span><span className="mono">{selectedVertical || "-"}</span></div>
          <div className="health-kv"><span>Filter</span><span>{filter}</span></div>
          <div className="health-kv"><span>Visible Rows</span><span className="mono">{filteredRuns.length}</span></div>
          {selectedVertical ? (
            <div className="stack" style={{ marginTop: 8 }}>
              <button className="btn-secondary" onClick={() => openFlowForVertical?.(selectedVertical, "")}>Open Trace</button>
              <button className="btn-secondary" onClick={() => openTopologyForVertical?.(selectedVertical)}>Open Topology</button>
            </div>
          ) : null}
        </div>

        <DataTable
          columns={columns}
          data={filteredRuns.slice(0, 40)}
          emptyLabel="No workflow runs match the current filter."
          initialSorting={[{ id: "issue_score", desc: true }]}
        />
      </div>
    </div>
  );
}
