import { useMemo } from "react";
import type { ControlResult, HealthResponse, LooseRecord, MailboxResponse, TargetRecord, TaskRecord, TaskStats, TasksResponse } from "../types/core.ts";
import { useControlController } from "../features/control/useControlController.ts";
import { useHealthController } from "../features/health/useHealthController.ts";
import { useTasksController } from "../features/tasks/useTasksController.ts";

type OpenView = (view: string, subview?: string) => void;
type AsyncAction = () => Promise<unknown>;
type StringSetter = (value: string) => void;
type BoolSetter = (value: boolean) => void;

type DashboardOpsControllerInput = {
  targets: TargetRecord[];
  mailbox: MailboxResponse;
  controlOutput: ControlResult;
  controlTarget: string;
  setControlTarget: StringSetter;
  directiveMessage: string;
  setDirectiveMessage: StringSetter;
  chatMessage: string;
  setChatMessage: StringSetter;
  chatMode: string;
  setChatMode: StringSetter;
  verticalName: string;
  setVerticalName: StringSetter;
  verticalGeo: string;
  setVerticalGeo: StringSetter;
  verticalSlug: string;
  setVerticalSlug: StringSetter;
  requeueEventID: string;
  setRequeueEventID: StringSetter;
  requeueAgentID: string;
  setRequeueAgentID: StringSetter;
  resetConfirm: string;
  setResetConfirm: StringSetter;
  mailStatus: string;
  setMailStatus: StringSetter;
  mailboxID: string;
  setMailboxID: StringSetter;
  mailboxAction: string;
  setMailboxAction: StringSetter;
  mailboxNotes: string;
  setMailboxNotes: StringSetter;
  selectedMailboxItem: string;
  setSelectedMailboxItem: StringSetter;
  sendDirective: AsyncAction;
  sendChat: AsyncAction;
  restartControlTarget: AsyncAction;
  replayControlTarget: AsyncAction;
  createVertical: AsyncAction;
  requeueEvent: AsyncAction;
  seedOrg: AsyncAction;
  pauseRuntime: AsyncAction;
  resumeRuntime: AsyncAction;
  resetDBAndSeed: AsyncAction;
  wipeDB: AsyncAction;
  decideMailbox: AsyncAction;
  quickMailboxDecide: (id: string, action: string) => Promise<void>;
  tasksResp: TasksResponse;
  tasksStats: TaskStats | null;
  selectedTask: TaskRecord | null;
  taskStatus: string;
  setTaskStatus: StringSetter;
  selectedTaskID: string;
  setSelectedTaskID: StringSetter;
  taskResultText: string;
  setTaskResultText: StringSetter;
  taskOutcome: string;
  setTaskOutcome: StringSetter;
  taskFollowUpNeeded: boolean;
  setTaskFollowUpNeeded: BoolSetter;
  taskRejectReason: string;
  setTaskRejectReason: StringSetter;
  loadTasks: AsyncAction;
  loadTaskStats: AsyncAction;
  claimSelectedTask: AsyncAction;
  completeSelectedTask: AsyncAction;
  rejectSelectedTask: AsyncAction;
  health: HealthResponse;
  contractsData: LooseRecord;
  contractWorkflow: LooseRecord;
  contractPlatform: LooseRecord;
  contractVerification: LooseRecord;
  openView: OpenView;
};

export function useDashboardOpsController({
  targets,
  mailbox,
  controlOutput,
  controlTarget,
  setControlTarget,
  directiveMessage,
  setDirectiveMessage,
  chatMessage,
  setChatMessage,
  chatMode,
  setChatMode,
  verticalName,
  setVerticalName,
  verticalGeo,
  setVerticalGeo,
  verticalSlug,
  setVerticalSlug,
  requeueEventID,
  setRequeueEventID,
  requeueAgentID,
  setRequeueAgentID,
  resetConfirm,
  setResetConfirm,
  mailStatus,
  setMailStatus,
  mailboxID,
  setMailboxID,
  mailboxAction,
  setMailboxAction,
  mailboxNotes,
  setMailboxNotes,
  selectedMailboxItem,
  setSelectedMailboxItem,
  sendDirective,
  sendChat,
  restartControlTarget,
  replayControlTarget,
  createVertical,
  requeueEvent,
  seedOrg,
  pauseRuntime,
  resumeRuntime,
  resetDBAndSeed,
  wipeDB,
  decideMailbox,
  quickMailboxDecide,
  tasksResp,
  tasksStats,
  selectedTask,
  taskStatus,
  setTaskStatus,
  selectedTaskID,
  setSelectedTaskID,
  taskResultText,
  setTaskResultText,
  taskOutcome,
  setTaskOutcome,
  taskFollowUpNeeded,
  setTaskFollowUpNeeded,
  taskRejectReason,
  setTaskRejectReason,
  loadTasks,
  loadTaskStats,
  claimSelectedTask,
  completeSelectedTask,
  rejectSelectedTask,
  health,
  contractsData,
  contractWorkflow,
  contractPlatform,
  contractVerification,
  openView,
}: DashboardOpsControllerInput) {
  const control = useControlController({
    targets,
    mailbox,
    controlOutput,
    controlTarget,
    directiveMessage,
    chatMessage,
    chatMode,
    verticalName,
    verticalGeo,
    verticalSlug,
    requeueEventID,
    requeueAgentID,
    resetConfirm,
    mailStatus,
    mailboxID,
    mailboxAction,
    mailboxNotes,
    selectedMailboxItem,
    setControlTarget,
    setDirectiveMessage,
    setChatMessage,
    setChatMode,
    setVerticalName,
    setVerticalGeo,
    setVerticalSlug,
    setRequeueEventID,
    setRequeueAgentID,
    setResetConfirm,
    setMailStatus,
    setMailboxID,
    setMailboxAction,
    setMailboxNotes,
    setSelectedMailboxItem,
    sendDirective,
    sendChat,
    restartControlTarget,
    replayControlTarget,
    createVertical,
    requeueEvent,
    seedOrg,
    pauseRuntime,
    resumeRuntime,
    resetDBAndSeed,
    wipeDB,
    decideMailbox,
    quickMailboxDecide,
  });

  const tasks = useTasksController({
    tasksResp,
    tasksStats,
    selectedTask,
    taskStatus,
    selectedTaskID,
    taskResultText,
    taskOutcome,
    taskFollowUpNeeded,
    taskRejectReason,
    setTaskStatus,
    setSelectedTaskID,
    setTaskResultText,
    setTaskOutcome,
    setTaskFollowUpNeeded,
    setTaskRejectReason,
    refreshTasks: loadTasks,
    loadTaskStats,
    claimSelectedTask,
    completeSelectedTask,
    rejectSelectedTask,
  });

  const healthController = useHealthController({
    health,
    contractsData,
    contractWorkflow,
    contractPlatform,
    contractVerification,
    openView,
  });

  return useMemo(() => ({
    control,
    tasks,
    health: healthController,
  }), [control, tasks, healthController]);
}
