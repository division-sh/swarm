import type { ControlResult, HealthResponse, MailboxResponse, TargetRecord, TaskRecord, TaskStats, TasksResponse } from "../types/core.ts";
import { useSelectedTask } from "../features/tasks/useSelectedTask.ts";
import { useDashboardOpsController } from "./useDashboardOpsController.ts";

type OpsAssemblyInput = {
  taskState: {
    taskStatus: string;
    setTaskStatus: (value: string) => void;
    selectedTaskID: string;
    setSelectedTaskID: (value: string) => void;
    taskResultText: string;
    setTaskResultText: (value: string) => void;
    taskOutcome: string;
    setTaskOutcome: (value: string) => void;
    taskFollowUpNeeded: boolean;
    setTaskFollowUpNeeded: (value: boolean) => void;
    taskRejectReason: string;
    setTaskRejectReason: (value: string) => void;
  };
  opsState: {
    controlOutput: ControlResult;
    controlTarget: string;
    setControlTarget: (value: string) => void;
    directiveMessage: string;
    setDirectiveMessage: (value: string) => void;
    chatMessage: string;
    setChatMessage: (value: string) => void;
    chatMode: string;
    setChatMode: (value: string) => void;
    verticalName: string;
    setVerticalName: (value: string) => void;
    verticalGeo: string;
    setVerticalGeo: (value: string) => void;
    verticalSlug: string;
    setVerticalSlug: (value: string) => void;
    requeueEventID: string;
    setRequeueEventID: (value: string) => void;
    requeueAgentID: string;
    setRequeueAgentID: (value: string) => void;
    resetConfirm: string;
    setResetConfirm: (value: string) => void;
    mailStatus: string;
    setMailStatus: (value: string) => void;
    mailboxID: string;
    setMailboxID: (value: string) => void;
    mailboxAction: string;
    setMailboxAction: (value: string) => void;
    mailboxNotes: string;
    setMailboxNotes: (value: string) => void;
  };
  loaders: {
    loadTasks: () => Promise<unknown>;
    loadTaskStats: () => Promise<unknown>;
  };
  queryData: {
    targets: TargetRecord[];
    mailbox: MailboxResponse;
    tasksResp: TasksResponse;
    tasksStats: TaskStats | null;
    health: HealthResponse;
  };
  ui: {
    selectedMailboxItem: string;
    setSelectedMailboxItem: (value: string) => void;
  };
  taskActions: {
    claimSelectedTask: () => Promise<void>;
    completeSelectedTask: () => Promise<void>;
    rejectSelectedTask: () => Promise<void>;
  };
  controlActions: {
    sendDirective: (...args: unknown[]) => Promise<void>;
    sendChat: (...args: unknown[]) => Promise<void>;
    restartControlTarget: (...args: unknown[]) => Promise<void>;
    replayControlTarget: (...args: unknown[]) => Promise<void>;
    createVertical: (...args: unknown[]) => Promise<void>;
    requeueEvent: (...args: unknown[]) => Promise<void>;
    seedOrg: () => Promise<unknown>;
    pauseRuntime: () => Promise<void>;
    resumeRuntime: () => Promise<void>;
    resetDBAndSeed: (...args: unknown[]) => Promise<void>;
    wipeDB: (...args: unknown[]) => Promise<void>;
    decideMailbox: (...args: unknown[]) => Promise<void>;
    quickMailboxDecide: (id: string, action: string) => Promise<void>;
  };
  healthContracts: {
    contractsData: Record<string, unknown>;
    contractWorkflow: Record<string, unknown>;
    contractPlatform: Record<string, unknown>;
    contractVerification: Record<string, unknown>;
  };
  openView: (view: string, subview?: string) => void;
};

export function useDashboardOpsAssembly({
  taskState,
  opsState,
  loaders,
  queryData,
  ui,
  taskActions,
  controlActions,
  healthContracts,
  openView,
}: OpsAssemblyInput) {
  const selectedTask: TaskRecord | null = useSelectedTask({
    tasks: queryData.tasksResp.tasks,
    selectedTaskID: taskState.selectedTaskID,
  });

  return useDashboardOpsController({
    targets: queryData.targets,
    mailbox: queryData.mailbox,
    controlOutput: opsState.controlOutput,
    controlTarget: opsState.controlTarget,
    setControlTarget: opsState.setControlTarget,
    directiveMessage: opsState.directiveMessage,
    setDirectiveMessage: opsState.setDirectiveMessage,
    chatMessage: opsState.chatMessage,
    setChatMessage: opsState.setChatMessage,
    chatMode: opsState.chatMode,
    setChatMode: opsState.setChatMode,
    verticalName: opsState.verticalName,
    setVerticalName: opsState.setVerticalName,
    verticalGeo: opsState.verticalGeo,
    setVerticalGeo: opsState.setVerticalGeo,
    verticalSlug: opsState.verticalSlug,
    setVerticalSlug: opsState.setVerticalSlug,
    requeueEventID: opsState.requeueEventID,
    setRequeueEventID: opsState.setRequeueEventID,
    requeueAgentID: opsState.requeueAgentID,
    setRequeueAgentID: opsState.setRequeueAgentID,
    resetConfirm: opsState.resetConfirm,
    setResetConfirm: opsState.setResetConfirm,
    mailStatus: opsState.mailStatus,
    setMailStatus: opsState.setMailStatus,
    mailboxID: opsState.mailboxID,
    setMailboxID: opsState.setMailboxID,
    mailboxAction: opsState.mailboxAction,
    setMailboxAction: opsState.setMailboxAction,
    mailboxNotes: opsState.mailboxNotes,
    setMailboxNotes: opsState.setMailboxNotes,
    selectedMailboxItem: ui.selectedMailboxItem,
    setSelectedMailboxItem: ui.setSelectedMailboxItem,
    sendDirective: controlActions.sendDirective,
    sendChat: controlActions.sendChat,
    restartControlTarget: controlActions.restartControlTarget,
    replayControlTarget: controlActions.replayControlTarget,
    createVertical: controlActions.createVertical,
    requeueEvent: controlActions.requeueEvent,
    seedOrg: controlActions.seedOrg,
    pauseRuntime: controlActions.pauseRuntime,
    resumeRuntime: controlActions.resumeRuntime,
    resetDBAndSeed: controlActions.resetDBAndSeed,
    wipeDB: controlActions.wipeDB,
    decideMailbox: controlActions.decideMailbox,
    quickMailboxDecide: controlActions.quickMailboxDecide,
    tasksResp: queryData.tasksResp,
    tasksStats: queryData.tasksStats,
    selectedTask,
    taskStatus: taskState.taskStatus,
    setTaskStatus: taskState.setTaskStatus,
    selectedTaskID: taskState.selectedTaskID,
    setSelectedTaskID: taskState.setSelectedTaskID,
    taskResultText: taskState.taskResultText,
    setTaskResultText: taskState.setTaskResultText,
    taskOutcome: taskState.taskOutcome,
    setTaskOutcome: taskState.setTaskOutcome,
    taskFollowUpNeeded: taskState.taskFollowUpNeeded,
    setTaskFollowUpNeeded: taskState.setTaskFollowUpNeeded,
    taskRejectReason: taskState.taskRejectReason,
    setTaskRejectReason: taskState.setTaskRejectReason,
    loadTasks: loaders.loadTasks,
    loadTaskStats: loaders.loadTaskStats,
    claimSelectedTask: taskActions.claimSelectedTask,
    completeSelectedTask: taskActions.completeSelectedTask,
    rejectSelectedTask: taskActions.rejectSelectedTask,
    health: queryData.health,
    contractsData: healthContracts.contractsData,
    contractWorkflow: healthContracts.contractWorkflow,
    contractPlatform: healthContracts.contractPlatform,
    contractVerification: healthContracts.contractVerification,
    openView,
  });
}
