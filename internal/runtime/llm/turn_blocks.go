package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func DecodeCanonicalRuntimeLogTurnBlocks(blocks []TurnBlock) ([]TurnBlockRuntimeLogData, error) {
	if len(blocks) == 0 {
		return nil, nil
	}
	out := make([]TurnBlockRuntimeLogData, 0)
	for _, block := range blocks {
		if strings.TrimSpace(block.Kind) != "runtime_log" {
			continue
		}
		data, ok, err := block.RuntimeLogData()
		if err != nil {
			return nil, fmt.Errorf("decode canonical runtime_log block: %w", err)
		}
		if !ok {
			return nil, fmt.Errorf("canonical runtime_log block is empty")
		}
		normalized, err := normalizeCanonicalRuntimeLogTurnBlockData(data)
		if err != nil {
			return nil, err
		}
		out = append(out, normalized)
	}
	return out, nil
}

func DecodeCanonicalRuntimeLogTurnBlocksJSON(raw []byte) ([]TurnBlockRuntimeLogData, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	var blocks []TurnBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, err
	}
	return DecodeCanonicalRuntimeLogTurnBlocks(blocks)
}

func DecodeCanonicalTurnSummaryBlocks(blocks []TurnBlock) (TurnSummaryTurnBlockData, bool, error) {
	var (
		summary      TurnSummaryTurnBlockData
		foundSummary bool
	)
	for _, block := range blocks {
		if strings.TrimSpace(block.Kind) != turnSummaryBlockKind {
			continue
		}
		if foundSummary {
			return TurnSummaryTurnBlockData{}, false, fmt.Errorf("multiple canonical turn_summary blocks")
		}
		decoded, ok, err := block.TurnSummaryData()
		if err != nil {
			return TurnSummaryTurnBlockData{}, false, fmt.Errorf("decode canonical turn_summary: %w", err)
		}
		if !ok {
			return TurnSummaryTurnBlockData{}, false, fmt.Errorf("canonical turn_summary block is empty")
		}
		summary = normalizeTurnSummaryTurnBlockData(decoded)
		if turnSummaryTurnBlockDataIsZero(summary) {
			return TurnSummaryTurnBlockData{}, false, fmt.Errorf("canonical turn_summary block is empty")
		}
		foundSummary = true
	}
	if !foundSummary {
		if turnBlocksRequireCanonicalSummary(blocks) {
			return TurnSummaryTurnBlockData{}, false, fmt.Errorf("missing canonical turn_summary for summary-bearing turn blocks")
		}
		return TurnSummaryTurnBlockData{}, false, nil
	}
	return summary, true, nil
}

func DecodeCanonicalTurnSummaryJSON(raw []byte) (TurnSummaryTurnBlockData, bool, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return TurnSummaryTurnBlockData{}, false, nil
	}
	var blocks []TurnBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return TurnSummaryTurnBlockData{}, false, err
	}
	return DecodeCanonicalTurnSummaryBlocks(blocks)
}

func normalizeTurnSummaryTurnBlockData(summary TurnSummaryTurnBlockData) TurnSummaryTurnBlockData {
	summary.AssistantVisibleOutput = strings.TrimSpace(summary.AssistantVisibleOutput)
	summary.Outcome = strings.TrimSpace(summary.Outcome)
	summary.ReasoningBlocks = trimSummaryStringSlice(summary.ReasoningBlocks)
	summary.ProgressUpdates = trimSummaryStringSlice(summary.ProgressUpdates)
	if len(summary.ToolResults) == 0 {
		summary.ToolResults = nil
	} else {
		out := make([]TurnSummaryToolResult, 0, len(summary.ToolResults))
		for _, item := range summary.ToolResults {
			item.ToolName = strings.TrimSpace(item.ToolName)
			item.ToolUseID = strings.TrimSpace(item.ToolUseID)
			out = append(out, item)
		}
		summary.ToolResults = out
	}
	return summary
}

func turnSummaryTurnBlockDataIsZero(summary TurnSummaryTurnBlockData) bool {
	return summary.AssistantVisibleOutput == "" &&
		summary.Outcome == "" &&
		len(summary.ReasoningBlocks) == 0 &&
		len(summary.ProgressUpdates) == 0 &&
		len(summary.ToolResults) == 0
}

func turnBlocksRequireCanonicalSummary(blocks []TurnBlock) bool {
	for _, block := range blocks {
		switch strings.TrimSpace(block.Kind) {
		case "assistant_text", "outcome", "reasoning", "progress", "tool_result":
			return true
		}
	}
	return false
}

func trimSummaryStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, item := range in {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeCanonicalRuntimeLogTurnBlockData(data TurnBlockRuntimeLogData) (TurnBlockRuntimeLogData, error) {
	data.LogLevel = strings.TrimSpace(data.LogLevel)
	if data.LogLevel == "" {
		return TurnBlockRuntimeLogData{}, fmt.Errorf("canonical runtime_log block log_level is required")
	}
	data.Message = strings.TrimSpace(data.Message)
	if data.Message == "" {
		return TurnBlockRuntimeLogData{}, fmt.Errorf("canonical runtime_log block message is required")
	}
	data.StackTrace = strings.TrimSpace(data.StackTrace)

	raw := bytes.TrimSpace(data.Details)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return TurnBlockRuntimeLogData{}, fmt.Errorf("canonical runtime_log block details are required")
	}
	var details map[string]any
	if err := json.Unmarshal(raw, &details); err != nil {
		return TurnBlockRuntimeLogData{}, fmt.Errorf("canonical runtime_log block details must be an object: %w", err)
	}
	if details == nil {
		return TurnBlockRuntimeLogData{}, fmt.Errorf("canonical runtime_log block details are required")
	}
	component := strings.TrimSpace(asString(details["component"]))
	if component == "" {
		return TurnBlockRuntimeLogData{}, fmt.Errorf("canonical runtime_log block details.component is required")
	}
	action := strings.TrimSpace(asString(details["action"]))
	if action == "" {
		return TurnBlockRuntimeLogData{}, fmt.Errorf("canonical runtime_log block details.action is required")
	}
	data.Details = append(json.RawMessage(nil), raw...)
	return data, nil
}
