import { useMemo } from "react";
import type { ControlResult, MailboxResponse, TargetRecord } from "../../types/core.ts";

type AsyncAction = () => Promise<unknown>;
type StringSetter = (value: string) => void;

type ControlControllerInput = {
  targets: TargetRecord[];
  mailbox: MailboxResponse;
  controlOutput: ControlResult;
  controlTarget: string;
  directiveMessage: string;
  chatMessage: string;
  chatMode: string;
  verticalName: string;
  verticalGeo: string;
  verticalSlug: string;
  requeueEventID: string;
  requeueAgentID: string;
  resetConfirm: string;
  mailStatus: string;
  mailboxID: string;
  mailboxAction: string;
  mailboxNotes: string;
  selectedMailboxItem: string;
  setControlTarget: StringSetter;
  setDirectiveMessage: StringSetter;
  setChatMessage: StringSetter;
  setChatMode: StringSetter;
  setVerticalName: StringSetter;
  setVerticalGeo: StringSetter;
  setVerticalSlug: StringSetter;
  setRequeueEventID: StringSetter;
  setRequeueAgentID: StringSetter;
  setResetConfirm: StringSetter;
  setMailStatus: StringSetter;
  setMailboxID: StringSetter;
  setMailboxAction: StringSetter;
  setMailboxNotes: StringSetter;
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
};

export function useControlController({
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
}: ControlControllerInput) {
  return useMemo(() => ({
    state: {
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
    },
    actions: {
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
    },
  }), [
    chatMessage,
    chatMode,
    controlOutput,
    controlTarget,
    createVertical,
    decideMailbox,
    directiveMessage,
    mailStatus,
    mailbox,
    mailboxAction,
    mailboxID,
    mailboxNotes,
    pauseRuntime,
    quickMailboxDecide,
    replayControlTarget,
    requeueAgentID,
    requeueEvent,
    requeueEventID,
    resetConfirm,
    resetDBAndSeed,
    restartControlTarget,
    resumeRuntime,
    seedOrg,
    selectedMailboxItem,
    sendChat,
    sendDirective,
    setChatMessage,
    setChatMode,
    setControlTarget,
    setDirectiveMessage,
    setMailStatus,
    setMailboxAction,
    setMailboxID,
    setMailboxNotes,
    setRequeueAgentID,
    setRequeueEventID,
    setResetConfirm,
    setSelectedMailboxItem,
    setVerticalGeo,
    setVerticalName,
    setVerticalSlug,
    targets,
    verticalGeo,
    verticalName,
    verticalSlug,
    wipeDB,
  ]);
}
