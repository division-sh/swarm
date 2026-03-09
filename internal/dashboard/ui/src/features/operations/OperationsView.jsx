import React, { useEffect, useMemo, useState } from "react";
import OperationsTriageSummary from "./OperationsTriageSummary.jsx";
import OperationsWorkbench from "./OperationsWorkbench.jsx";
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
  pipeline,
}) {
  const routeSubview = routeToSubview(activeView) || activeSubview;
  const [subview, setSubview] = useState(routeSubview || "queue");
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

  const knownVerticals = Array.isArray(pipeline?.holding?.state?.holdingData?.verticals) ? pipeline.holding.state.holdingData.verticals : [];

  function resolveVertical(target) {
    const value = String(target || "").trim();
    if (!value) return null;
    return knownVerticals.find((vertical) => vertical.slug === value || vertical.id === value) || null;
  }

  function openWorkflowTrace(target) {
    const vertical = resolveVertical(target);
    const value = vertical?.slug || String(target || "").trim();
    if (!value) return;
    pipeline.flow.actions.setFlowView("runtime");
    pipeline.flow.actions.setFlowVertical(value);
    pipeline.flow.actions.setSelectedFlowNodeID("");
    pipeline.flow.actions.setSelectedFlowEdgeID("");
    setViewRoute("workflow", "flow");
  }

  function openPortfolio(target) {
    const vertical = resolveVertical(target);
    if (vertical?.id) {
      pipeline.holding.actions.openHoldingVerticalDetail(vertical.id);
      setViewRoute("portfolio", "holding");
      return;
    }
    setViewRoute("portfolio", "holding");
  }

  function openRelatedTaskForVertical(target) {
    const vertical = String(target || "").trim();
    if (!vertical) {
      selectSubview("tasks");
      return;
    }
    const relatedTask = (tasks.state.tasksResp?.tasks || []).find((task) => task.vertical_slug === vertical);
    tasks.actions.setTaskStatus("all");
    tasks.actions.setSelectedTaskID(relatedTask?.id || "");
    selectSubview("tasks");
  }

  function openRelatedMailboxForVertical(target) {
    const vertical = String(target || "").trim();
    const relatedMailbox = (control.state.mailbox?.items || []).find((item) => item.vertical_slug === vertical || item.vertical_id === vertical);
    control.actions.setMailStatus("all");
    control.actions.setMailboxID(relatedMailbox?.id || "");
    control.actions.setSelectedMailboxItem(relatedMailbox?.id || "");
    selectSubview("control");
  }

  const mailboxPending = control.state.mailbox?.summary?.pending || 0;
  const taskCount = Array.isArray(tasks.state.tasksResp?.tasks) ? tasks.state.tasksResp.tasks.length : 0;

  return (
    <div>
      <div className="head">
        <h2>Operations</h2>
        <span className="tiny">Human intervention console</span>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Unified mailbox, control, and human-task workflow surface. {mailboxPending} pending mailbox items, {taskCount} loaded tasks.
      </div>
      <OperationsTriageSummary
        derived={derived}
        onOpenMailbox={openMailbox}
        onOpenTask={openTask}
        onOpenQueue={() => selectSubview("queue")}
        onOpenControl={() => selectSubview("control")}
        onOpenTasksView={() => selectSubview("tasks")}
      />
      <OperationsWorkbench
        subview={subview}
        setViewRoute={setViewRoute}
        derived={derived}
        control={control}
        tasks={tasks}
        onOpenMailbox={openMailbox}
        onOpenTask={openTask}
        onOpenWorkflowTrace={openWorkflowTrace}
        onOpenPortfolio={openPortfolio}
        onOpenRelatedTaskForVertical={openRelatedTaskForVertical}
        onOpenRelatedMailboxForVertical={openRelatedMailboxForVertical}
      />
    </div>
  );
}
