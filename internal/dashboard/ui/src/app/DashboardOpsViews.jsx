import React from "react";
import ControlView from "../features/control/ControlView.jsx";
import HealthView from "../features/health/HealthView.jsx";
import HoldingView from "../features/holding/HoldingView.jsx";
import PipelineView from "../features/pipeline/PipelineView.jsx";
import TasksView from "../features/tasks/TasksView.jsx";

export default function DashboardOpsViews({ activeView, ops, pipeline }) {
  return (
    <>
      {activeView === "control" ? (
        <ControlView state={ops.control.state} actions={ops.control.actions} />
      ) : null}

      {activeView === "tasks" ? (
        <TasksView state={ops.tasks.state} actions={ops.tasks.actions} />
      ) : null}

      {activeView === "pipeline" ? (
        <PipelineView state={pipeline.pipeline.state} actions={pipeline.pipeline.actions} />
      ) : null}

      {activeView === "holding" ? (
        <HoldingView state={pipeline.holding.state} actions={pipeline.holding.actions} />
      ) : null}

      {activeView === "health" ? (
        <HealthView state={ops.health.state} />
      ) : null}
    </>
  );
}
