package llm

import (
	"bytes"
	"encoding/json"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
)

type TurnBlock struct {
	Kind     string          `json:"kind"`
	Title    string          `json:"title,omitempty"`
	Text     string          `json:"text,omitempty"`
	ToolName string          `json:"tool_name,omitempty"`
	Input    json.RawMessage `json:"input,omitempty"`
	Output   json.RawMessage `json:"output,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
}

type TurnBlockDispatchData struct {
	TriggerEventID   string `json:"trigger_event_id,omitempty"`
	TriggerEventType string `json:"trigger_event_type,omitempty"`
	EntityID         string `json:"entity_id,omitempty"`
	TaskID           string `json:"task_id,omitempty"`
}

type TurnBlockPublishData struct {
	EventID                     string                                  `json:"event_id,omitempty"`
	EntityID                    string                                  `json:"entity_id,omitempty"`
	ParentEventID               string                                  `json:"parent_event_id,omitempty"`
	RoutedRecipients            []runtimebus.PublishDiagnosticRecipient `json:"routed_recipients,omitempty"`
	RoutedRecipientsCount       int                                     `json:"routed_recipients_count,omitempty"`
	SubscriptionRecipients      []string                                `json:"subscription_recipients,omitempty"`
	SubscriptionRecipientsCount int                                     `json:"subscription_recipients_count,omitempty"`
}

type TurnBlockRuntimeLogData struct {
	LogLevel   string          `json:"log_level,omitempty"`
	Message    string          `json:"message,omitempty"`
	Details    json.RawMessage `json:"details,omitempty"`
	StackTrace string          `json:"stack_trace,omitempty"`
}

type TurnBlockToolLinkData struct {
	ToolUseID string `json:"tool_use_id,omitempty"`
}

type TurnSummaryToolResult struct {
	ToolName  string          `json:"tool_name,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Output    json.RawMessage `json:"output,omitempty"`
}

type TurnSummaryTurnBlockData struct {
	AssistantVisibleOutput string                  `json:"assistant_visible_output,omitempty"`
	Outcome                string                  `json:"outcome,omitempty"`
	ReasoningBlocks        []string                `json:"reasoning_blocks,omitempty"`
	ProgressUpdates        []string                `json:"progress_updates,omitempty"`
	ToolResults            []TurnSummaryToolResult `json:"tool_results,omitempty"`
}

func CanonicalizeTurnForPersistence(rec AgentTurnRecord) AgentTurnRecord {
	rec.TurnBlocks = BuildTurnBlocks(rec)
	return rec
}

func rawTurnBlockValue(value any) json.RawMessage {
	if value == nil {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func newDispatchTurnBlock(title string, data TurnBlockDispatchData) TurnBlock {
	return TurnBlock{
		Kind:  "dispatch",
		Title: strings.TrimSpace(title),
		Data:  rawTurnBlockValue(data),
	}
}

func newPublishTurnBlock(title string, data TurnBlockPublishData) TurnBlock {
	return TurnBlock{
		Kind:  "publish",
		Title: strings.TrimSpace(title),
		Data:  rawTurnBlockValue(data),
	}
}

func newRuntimeLogTurnBlock(title string, data TurnBlockRuntimeLogData) TurnBlock {
	return TurnBlock{
		Kind:  "runtime_log",
		Title: strings.TrimSpace(title),
		Data:  rawTurnBlockValue(data),
	}
}

func newToolUseTurnBlock(name string, input any, toolUseID string) TurnBlock {
	return TurnBlock{
		Kind:     "tool_use",
		ToolName: strings.TrimSpace(name),
		Input:    rawTurnBlockValue(input),
		Data:     rawTurnBlockValue(TurnBlockToolLinkData{ToolUseID: strings.TrimSpace(toolUseID)}),
	}
}

func newToolResultTurnBlock(name string, output any, toolUseID string) TurnBlock {
	return TurnBlock{
		Kind:     "tool_result",
		ToolName: strings.TrimSpace(name),
		Output:   rawTurnBlockValue(output),
		Data:     rawTurnBlockValue(TurnBlockToolLinkData{ToolUseID: strings.TrimSpace(toolUseID)}),
	}
}

func newTurnSummaryTurnBlock(data TurnSummaryTurnBlockData) TurnBlock {
	return TurnBlock{
		Kind: turnSummaryBlockKind,
		Data: rawTurnBlockValue(data),
	}
}

func (b TurnBlock) decodeData(target any) (bool, error) {
	raw := bytes.TrimSpace(b.Data)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || bytes.Equal(raw, []byte("{}")) {
		return false, nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return true, err
	}
	return true, nil
}

func (b TurnBlock) DispatchData() (TurnBlockDispatchData, bool, error) {
	var data TurnBlockDispatchData
	ok, err := b.decodeData(&data)
	return data, ok, err
}

func (b TurnBlock) PublishData() (TurnBlockPublishData, bool, error) {
	var data TurnBlockPublishData
	ok, err := b.decodeData(&data)
	return data, ok, err
}

func (b TurnBlock) RuntimeLogData() (TurnBlockRuntimeLogData, bool, error) {
	var data TurnBlockRuntimeLogData
	ok, err := b.decodeData(&data)
	return data, ok, err
}

func (b TurnBlock) TurnSummaryData() (TurnSummaryTurnBlockData, bool, error) {
	var data TurnSummaryTurnBlockData
	ok, err := b.decodeData(&data)
	return data, ok, err
}

func (b TurnBlock) ToolLinkData() (TurnBlockToolLinkData, bool, error) {
	var data TurnBlockToolLinkData
	ok, err := b.decodeData(&data)
	return data, ok, err
}
