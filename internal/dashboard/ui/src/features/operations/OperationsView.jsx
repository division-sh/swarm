import React, { useEffect, useState } from "react";
import ControlView from "../control/ControlView.jsx";
import TasksView from "../tasks/TasksView.jsx";

function routeToSubview(activeView) {
  if (activeView === "control" || activeView === "tasks") return activeView;
  return "";
}

export default function OperationsView({
  activeView,
  activeSubview,
  setViewRoute,
  control,
  tasks,
}) {
  const routeSubview = routeToSubview(activeView) || activeSubview;
  const [subview, setSubview] = useState(routeSubview || "control");

  useEffect(() => {
    if (!routeSubview) return;
    setSubview(routeSubview);
    if (activeView === "control" || activeView === "tasks") {
      setViewRoute("operations", routeSubview);
    }
  }, [activeView, routeSubview, setViewRoute]);

  function selectSubview(next) {
    setSubview(next);
    setViewRoute("operations", next);
  }

  const mailboxPending = control.state.mailbox?.summary?.pending || 0;
  const taskCount = Array.isArray(tasks.state.tasksResp?.tasks) ? tasks.state.tasksResp.tasks.length : 0;

  return (
    <div>
      <div className="head">
        <h2>Operations</h2>
        <div className="stack">
          <button className={subview === "control" ? "active" : ""} onClick={() => selectSubview("control")}>
            Control + Mailbox
          </button>
          <button className={subview === "tasks" ? "active" : ""} onClick={() => selectSubview("tasks")}>
            Tasks
          </button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Unified mailbox, control, and human-task workflow surface. {mailboxPending} pending mailbox items, {taskCount} loaded tasks.
      </div>
      {subview === "control" ? <ControlView state={control.state} actions={control.actions} /> : null}
      {subview === "tasks" ? <TasksView state={tasks.state} actions={tasks.actions} /> : null}
    </div>
  );
}
