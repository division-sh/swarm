import { DockviewReact, themeAbyssSpaced } from "dockview";
import React, { useCallback, useEffect, useMemo, useRef } from "react";
import ControlView from "../control/ControlView.tsx";
import TasksView from "../tasks/TasksView.tsx";
import OperationsQueueView from "./OperationsQueueView.tsx";

function OperationsQueuePanel(props) {
  const params = props.params || {};
  return (
    <div className="operations-dock-panel">
      <OperationsQueueView
        derived={params.derived}
        onOpenMailbox={params.onOpenMailbox}
        onOpenTask={params.onOpenTask}
      />
    </div>
  );
}

function OperationsControlPanel(props) {
  const params = props.params || {};
  return (
    <div className="operations-dock-panel">
      <ControlView
        state={params.control.state}
        actions={params.control.actions}
        onOpenWorkflowTrace={params.onOpenWorkflowTrace}
        onOpenPortfolio={params.onOpenPortfolio}
        onOpenRelatedTaskForVertical={params.onOpenRelatedTaskForVertical}
      />
    </div>
  );
}

function OperationsTasksPanel(props) {
  const params = props.params || {};
  return (
    <div className="operations-dock-panel">
      <TasksView
        state={params.tasks.state}
        actions={params.tasks.actions}
        onOpenWorkflowTrace={params.onOpenWorkflowTrace}
        onOpenPortfolio={params.onOpenPortfolio}
        onOpenRelatedMailboxForVertical={params.onOpenRelatedMailboxForVertical}
      />
    </div>
  );
}

export default function OperationsWorkbench({
  subview,
  setViewRoute,
  derived,
  control,
  tasks,
  onOpenMailbox,
  onOpenTask,
  onOpenWorkflowTrace,
  onOpenPortfolio,
  onOpenRelatedTaskForVertical,
  onOpenRelatedMailboxForVertical,
}) {
  const dockApiRef = useRef(null);
  const dockInitRef = useRef(false);
  const dockDisposerRef = useRef(null);

  const selectSubview = useCallback((next) => {
    setViewRoute("operations", next);
    dockApiRef.current?.getPanel(next)?.api?.setActive();
  }, [setViewRoute]);

  const dockComponents = useMemo(() => ({
    queue: OperationsQueuePanel,
    control: OperationsControlPanel,
    tasks: OperationsTasksPanel,
  }), []);

  const dockParams = useMemo(() => ({
    derived,
    control,
    tasks,
    onOpenMailbox,
    onOpenTask,
    onOpenWorkflowTrace,
    onOpenPortfolio,
    onOpenRelatedTaskForVertical,
    onOpenRelatedMailboxForVertical,
  }), [control, derived, onOpenMailbox, onOpenPortfolio, onOpenRelatedMailboxForVertical, onOpenRelatedTaskForVertical, onOpenTask, onOpenWorkflowTrace, tasks]);

  useEffect(() => {
    const api = dockApiRef.current;
    if (!api) return;
    ["queue", "control", "tasks"].forEach((panelID) => {
      api.getPanel(panelID)?.api?.updateParameters(dockParams);
    });
  }, [dockParams]);

  useEffect(() => {
    const api = dockApiRef.current;
    if (!api) return;
    api.getPanel(subview || "queue")?.api?.setActive();
  }, [subview]);

  useEffect(() => () => {
    dockDisposerRef.current?.dispose?.();
  }, []);

  const handleReady = useCallback((event) => {
    const api = event.api;
    dockApiRef.current = api;
    if (!dockInitRef.current) {
      dockInitRef.current = true;
      const queuePanel = api.addPanel({
        id: "queue",
        component: "queue",
        title: "Needs Action",
        params: dockParams,
      });
      api.addPanel({
        id: "control",
        component: "control",
        title: "Control + Mailbox",
        params: dockParams,
        position: {
          referencePanel: queuePanel,
          direction: "right",
        },
      });
      api.addPanel({
        id: "tasks",
        component: "tasks",
        title: "Tasks",
        params: dockParams,
        position: {
          referencePanel: queuePanel,
          direction: "below",
        },
      });
    }
    dockDisposerRef.current?.dispose?.();
    dockDisposerRef.current = api.onDidActivePanelChange((panel) => {
      const next = panel?.id || "queue";
      setViewRoute("operations", next);
    });
    api.getPanel(subview || "queue")?.api?.setActive();
  }, [dockParams, setViewRoute, subview]);

  return (
    <section className="operations-workbench-shell">
      <div className="head">
        <h2>Workbench</h2>
        <div className="stack">
          <button className={subview === "queue" ? "active" : ""} onClick={() => selectSubview("queue")}>Needs Action</button>
          <button className={subview === "control" ? "active" : ""} onClick={() => selectSubview("control")}>Control + Mailbox</button>
          <button className={subview === "tasks" ? "active" : ""} onClick={() => selectSubview("tasks")}>Tasks</button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Operations workbench for unified human action, mailbox decisions, and task execution. {derived.summary.pendingMailbox} pending mailbox items and {derived.summary.actionableTasks} actionable tasks.
      </div>
      <div className="body operations-workbench-body">
        <DockviewReact
          className="operations-dockview"
          theme={themeAbyssSpaced}
          components={dockComponents}
          onReady={handleReady}
          disableFloatingGroups
          dndEdges={false}
          tabComponents={{}}
        />
      </div>
    </section>
  );
}
