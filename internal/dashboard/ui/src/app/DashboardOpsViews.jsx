import React from "react";
import GateIndicator from "../components/GateIndicator.jsx";
import ControlView from "../features/control/ControlView.jsx";
import HealthView from "../features/health/HealthView.jsx";
import HoldingView from "../features/holding/HoldingView.jsx";
import PipelineView from "../features/pipeline/PipelineView.jsx";
import TasksView from "../features/tasks/TasksView.jsx";
import { fmtTime, formatDollars, formatDurationMs, readPath, relTime, shardScopeSummary } from "../lib/format.js";

export default function DashboardOpsViews({ app }) {
  return (
    <>
      {app.activeView === "control" ? (
        <ControlView
          domain={{ targets: app.targets, mailbox: app.mailbox, controlOutput: app.controlOutput }}
          controls={{
            controlTarget: app.controlTarget,
            setControlTarget: app.setControlTarget,
            directiveMessage: app.directiveMessage,
            setDirectiveMessage: app.setDirectiveMessage,
            chatMessage: app.chatMessage,
            setChatMessage: app.setChatMessage,
            chatMode: app.chatMode,
            setChatMode: app.setChatMode,
            verticalName: app.verticalName,
            setVerticalName: app.setVerticalName,
            verticalGeo: app.verticalGeo,
            setVerticalGeo: app.setVerticalGeo,
            verticalSlug: app.verticalSlug,
            setVerticalSlug: app.setVerticalSlug,
            requeueEventID: app.requeueEventID,
            setRequeueEventID: app.setRequeueEventID,
            requeueAgentID: app.requeueAgentID,
            setRequeueAgentID: app.setRequeueAgentID,
            resetConfirm: app.resetConfirm,
            setResetConfirm: app.setResetConfirm,
            mailStatus: app.mailStatus,
            setMailStatus: app.setMailStatus,
            mailboxID: app.mailboxID,
            setMailboxID: app.setMailboxID,
            mailboxAction: app.mailboxAction,
            setMailboxAction: app.setMailboxAction,
            mailboxNotes: app.mailboxNotes,
            setMailboxNotes: app.setMailboxNotes,
            selectedMailboxItem: app.selectedMailboxItem,
            setSelectedMailboxItem: app.setSelectedMailboxItem,
          }}
          actions={{
            sendDirective: app.sendDirective,
            sendChat: app.sendChat,
            restartControlTarget: app.restartControlTarget,
            replayControlTarget: app.replayControlTarget,
            createVertical: app.createVertical,
            requeueEvent: app.requeueEvent,
            seedOrg: app.seedOrg,
            pauseRuntime: app.pauseRuntime,
            resumeRuntime: app.resumeRuntime,
            resetDBAndSeed: app.resetDBAndSeed,
            wipeDB: app.wipeDB,
            decideMailbox: app.decideMailbox,
            quickMailboxDecide: app.quickMailboxDecide,
          }}
          helpers={{ fmtTime, relTime }}
        />
      ) : null}

      {app.activeView === "tasks" ? (
        <TasksView
          domain={{ tasksResp: app.tasksResp, tasksStats: app.tasksStats, selectedTask: app.selectedTask }}
          controls={{
            taskStatus: app.taskStatus,
            setTaskStatus: app.setTaskStatus,
            selectedTaskID: app.selectedTaskID,
            setSelectedTaskID: app.setSelectedTaskID,
            taskResultText: app.taskResultText,
            setTaskResultText: app.setTaskResultText,
            taskOutcome: app.taskOutcome,
            setTaskOutcome: app.setTaskOutcome,
            taskFollowUpNeeded: app.taskFollowUpNeeded,
            setTaskFollowUpNeeded: app.setTaskFollowUpNeeded,
            taskRejectReason: app.taskRejectReason,
            setTaskRejectReason: app.setTaskRejectReason,
          }}
          actions={{
            refreshTasks: app.loadTasks,
            loadTaskStats: app.loadTaskStats,
            claimSelectedTask: app.claimSelectedTask,
            completeSelectedTask: app.completeSelectedTask,
            rejectSelectedTask: app.rejectSelectedTask,
          }}
          helpers={{ fmtTime, relTime }}
        />
      ) : null}

      {app.activeView === "pipeline" ? (
        <PipelineView
          domain={{ funnel: app.funnel, shardScans: app.shardScans, shardScanDetails: app.shardScanDetails, traceRows: app.traceRows }}
          controls={{ traceVertical: app.traceVertical, setTraceVertical: app.setTraceVertical, selectedShardScanID: app.selectedShardScanID, setSelectedShardScanID: app.setSelectedShardScanID }}
          actions={{ traceVerticalFlow: app.loadTrace, loadShardScanDetail: app.loadShardScanDetail, shardAction: app.shardAction }}
          helpers={{ fmtTime, relTime, formatDollars, formatDurationMs, shardScopeSummary }}
        />
      ) : null}

      {app.activeView === "holding" ? (
        <HoldingView
          domain={app.holdingViewState.domain}
          controls={app.holdingViewState.controls}
          openHoldingVerticalDetail={app.openHoldingVerticalDetail}
          relTime={relTime}
          readPath={readPath}
          formatDollars={formatDollars}
          GateIndicator={GateIndicator}
        />
      ) : null}

      {app.activeView === "health" ? (
        <HealthView
          health={app.health}
          contractsData={app.contractsData}
          contractWorkflow={app.contractWorkflow}
          contractPlatform={app.contractPlatform}
          contractVerification={app.contractVerification}
          formatDollars={formatDollars}
          readPath={readPath}
        />
      ) : null}
    </>
  );
}
