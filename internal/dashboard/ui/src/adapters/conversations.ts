import type { ConversationDetail, ConversationMessage, ConversationRecord, ConversationTurn } from "../types/runtime.ts";
import type { GenericConversationDetail, GenericConversationSummary } from "../types/server.ts";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? { ...(value as Record<string, unknown>) } : {};
}

function asString(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function asNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return undefined;
}

function asBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function contentToText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content.map((item) => {
    const obj = asRecord(item);
    const text = asString(obj.text);
    if (text) return text;
    return asString(obj.type);
  }).filter(Boolean).join("\n");
}

function normalizeContent(content: unknown): Array<{ text?: string; type?: string; [key: string]: unknown }> {
  if (typeof content === "string") {
    return [{ type: "text", text: content }];
  }
  if (!Array.isArray(content)) return [];
  return content.map((item) => {
    if (item && typeof item === "object") {
      return { ...(item as Record<string, unknown>) };
    }
    return { type: "text", text: String(item ?? "") };
  });
}

function normalizeMessage(value: unknown): ConversationMessage | null {
  if (typeof value === "string") {
    return { role: "assistant", content: normalizeContent(value), text: value };
  }
  const obj = asRecord(value);
  if (Object.keys(obj).length === 0) return null;
  const content = obj.content;
  const text = asString(obj.text) || contentToText(content);
  return {
    ...obj,
    role: asString(obj.role) || "assistant",
    content: normalizeContent(content),
    text,
    created_at: asString(obj.created_at),
  };
}

function readToolCalls(value: unknown): unknown[] {
  if (Array.isArray(value)) return value;
  const obj = asRecord(value);
  if (Array.isArray(obj.tool_calls)) return obj.tool_calls as unknown[];
  return [];
}

function readAssistantText(payload: unknown): string {
  if (typeof payload === "string") return payload;
  const obj = asRecord(payload);
  return (
    asString(obj.assistant_text) ||
    asString(obj.text) ||
    asString(obj.message) ||
    contentToText(obj.content)
  );
}

function readToolResult(payload: unknown, fallbackError: unknown): string {
  const obj = asRecord(payload);
  return asString(obj.tool_result) || asString(obj.result) || asString(fallbackError);
}

function normalizeTurn(value: unknown, index: number, fallbackCreatedAt: string): ConversationTurn {
  const obj = asRecord(value);
  const responsePayload = obj.response_payload;
  const turnIndex = asNumber(obj.turn_index) ?? index;
  return {
    ...obj,
    turn_index: turnIndex,
    created_at: asString(obj.updated_at) || fallbackCreatedAt,
    parse_ok: asBoolean(obj.parse_ok),
    latency_ms: asNumber(obj.latency_ms),
    retry_count: asNumber(obj.retry_count),
    tool_calls: readToolCalls(responsePayload),
    assistant_text: readAssistantText(responsePayload),
    tool_result: readToolResult(responsePayload, obj.error),
  };
}

function normalizeTurns(detail: GenericConversationDetail): ConversationTurn[] {
  const runtimeState = asRecord(detail.runtime_state);
  const turnList = Array.isArray(runtimeState.turns) ? runtimeState.turns : [];
  if (turnList.length > 0) {
    return turnList.map((item, index) => normalizeTurn(item, index, asString(detail.updated_at)));
  }
  const lastTurn = runtimeState.last_turn;
  if (!lastTurn || typeof lastTurn !== "object") return [];
  const defaultIndex = Math.max(0, (asNumber(detail.turn_count) || 1) - 1);
  return [normalizeTurn(lastTurn, defaultIndex, asString(detail.updated_at))];
}

export function adaptConversationSummary(item: GenericConversationSummary): ConversationRecord {
  return {
    id: item.agent_id,
    agent_id: item.agent_id,
    updated_at: item.updated_at,
    scope_key: item.scope_key,
    scope: item.scope,
    runtime_mode: item.runtime_mode,
    status: item.status,
    summary: item.summary,
    turn_count: item.turn_count,
    metadata: item.metadata,
  };
}

export function adaptConversationSummaries(items: GenericConversationSummary[]): ConversationRecord[] {
  return items.map(adaptConversationSummary);
}

export function adaptConversationDetail(detail: GenericConversationDetail): ConversationDetail {
  const messages = Array.isArray(detail.messages) ? detail.messages.map(normalizeMessage).filter(Boolean) as ConversationMessage[] : [];
  return {
    messages,
    turns: normalizeTurns(detail),
  };
}
