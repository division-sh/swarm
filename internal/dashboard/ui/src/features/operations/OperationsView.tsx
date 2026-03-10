import React, { useEffect, useMemo, useState } from "react";
import OperationsTriageSummary from "./OperationsTriageSummary.tsx";
import OperationsWorkbench from "./OperationsWorkbench.tsx";
import { deriveOperationsDerivedState } from "./useOperationsDerivedState.ts";

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
  const [pendingMailboxSelection, setPendingMailboxSelection] = useState("");
  const [pendingTaskSelection, setPendingTaskSelection] = useState("");
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
    const nextID = item?.id || "";
    control.actions.setMailStatus("all");
    control.actions.setMailboxID(nextID);
    control.actions.setSelectedMailboxItem(nextID);
    setPendingMailboxSelection(nextID);
    selectSubview("control");
  }

  function openTask(task) {
    const nextID = task?.id || "";
    tasks.actions.setTaskStatus(String(task?.status || "all"));
    tasks.actions.setSelectedTaskID(nextID);
    setPendingTaskSelection(nextID);
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
    const nextID = relatedTask?.id || "";
    tasks.actions.setTaskStatus(String(relatedTask?.status || "all"));
    tasks.actions.setSelectedTaskID(nextID);
    setPendingTaskSelection(nextID);
    selectSubview("tasks");
  }

  function openRelatedMailboxForVertical(target) {
    const vertical = String(target || "").trim();
    const relatedMailbox = (control.state.mailbox?.items || []).find((item) => item.vertical_slug === vertical || item.vertical_id === vertical);
    const nextID = relatedMailbox?.id || "";
    control.actions.setMailStatus("all");
    control.actions.setMailboxID(nextID);
    control.actions.setSelectedMailboxItem(nextID);
    setPendingMailboxSelection(nextID);
    selectSubview("control");
  }

  useEffect(() => {
    if (subview === "control" && pendingMailboxSelection) {
      control.actions.setMailboxID(pendingMailboxSelection);
      control.actions.setSelectedMailboxItem(pendingMailboxSelection);
    }
  }, [control.actions, pendingMailboxSelection, subview]);

  useEffect(() => {
    if (subview === "tasks" && pendingTaskSelection) {
      tasks.actions.setSelectedTaskID(pendingTaskSelection);
    }
  }, [pendingTaskSelection, subview, tasks.actions]);

  useEffect(() => {
    if (pendingMailboxSelection && control.state.selectedMailboxItem === pendingMailboxSelection) {
      setPendingMailboxSelection("");
    }
  }, [control.state.selectedMailboxItem, pendingMailboxSelection]);

  useEffect(() => {
    if (pendingTaskSelection && tasks.state.selectedTaskID === pendingTaskSelection) {
      setPendingTaskSelection("");
    }
  }, [pendingTaskSelection, tasks.state.selectedTaskID]);

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
