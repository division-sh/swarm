import React, { useEffect, useMemo, useState } from "react";
import ControlView from "../control/ControlView.jsx";
import TasksView from "../tasks/TasksView.jsx";
import OperationsTriageSummary from "./OperationsTriageSummary.jsx";
import { deriveOperationsDerivedState } from "./useOperationsDerivedState.js";

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
  const derived = useMemo(() => deriveOperationsDerivedState({
    mailbox: control.state.mailbox,
    tasksResp: tasks.state.tasksResp,
    selectedTask: tasks.state.selectedTask,
    selectedMailboxItem: control.state.selectedMailboxItem,
  }), [
    control.state.mailbox,
    control.state.selectedMailboxItem,
    tasks.state.selectedTask,
    tasks.state.tasksResp,
  ]);

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

  function openMailbox(item) {
    control.actions.setMailStatus("all");
    control.actions.setMailboxID(item?.id || "");
    control.actions.setSelectedMailboxItem(item?.id || "");
    selectSubview("control");
  }

  function openTask(task) {
    tasks.actions.setTaskStatus("all");
    tasks.actions.setSelectedTaskID(task?.id || "");
    selectSubview("tasks");
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
      <OperationsTriageSummary derived={derived} onOpenMailbox={openMailbox} onOpenTask={openTask} />
      {subview === "control" ? <ControlView state={control.state} actions={control.actions} /> : null}
      {subview === "tasks" ? <TasksView state={tasks.state} actions={tasks.actions} /> : null}
    </div>
  );
}
