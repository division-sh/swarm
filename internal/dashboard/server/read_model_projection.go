package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type projectedConversationRuntimeState struct {
	Summary              string
	LastTurn             *projectedConversationLastTurn
	ProviderSessionID    string
	RetryReason          string
	RetriesFromSessionID string
}

type projectedConversationLastTurn struct {
	TaskID  string `json:"task_id,omitempty"`
	ParseOK bool   `json:"parse_ok,omitempty"`
}

func decodeConversationRuntimeStateProjection(raw []byte) (projectedConversationRuntimeState, error) {
	if len(raw) == 0 {
		return projectedConversationRuntimeState{}, nil
	}
	var typed struct {
		Summary              string                         `json:"summary"`
		LastTurn             *projectedConversationLastTurn `json:"last_turn,omitempty"`
		ProviderSessionID    string                         `json:"provider_session_id,omitempty"`
		RetryReason          string                         `json:"retry_reason,omitempty"`
		RetriesFromSessionID string                         `json:"retries_from_session_id,omitempty"`
	}
	if err := json.Unmarshal(raw, &typed); err != nil {
		return projectedConversationRuntimeState{}, err
	}
	return projectedConversationRuntimeState{
		Summary:              strings.TrimSpace(typed.Summary),
		LastTurn:             typed.LastTurn,
		ProviderSessionID:    strings.TrimSpace(typed.ProviderSessionID),
		RetryReason:          strings.TrimSpace(typed.RetryReason),
		RetriesFromSessionID: strings.TrimSpace(typed.RetriesFromSessionID),
	}, nil
}

func (p projectedConversationRuntimeState) metadata() ConversationSummaryMetadata {
	return ConversationSummaryMetadata{
		ProviderSessionID:    p.ProviderSessionID,
		RetryReason:          p.RetryReason,
		RetriesFromSessionID: p.RetriesFromSessionID,
	}
}

func (p projectedConversationRuntimeState) runtimeState() ConversationRuntimeState {
	state := ConversationRuntimeState{
		Summary:              p.Summary,
		ProviderSessionID:    p.ProviderSessionID,
		RetryReason:          p.RetryReason,
		RetriesFromSessionID: p.RetriesFromSessionID,
	}
	if p.LastTurn != nil {
		state.LastTurn = &ConversationRuntimeLastTurn{
			TaskID:  strings.TrimSpace(p.LastTurn.TaskID),
			ParseOK: p.LastTurn.ParseOK,
		}
	}
	return state
}

type projectedTurnSummary struct {
	AssistantVisibleOutput string                         `json:"assistant_visible_output,omitempty"`
	Outcome                string                         `json:"outcome,omitempty"`
	ReasoningBlocks        []string                       `json:"reasoning_blocks,omitempty"`
	ProgressUpdates        []string                       `json:"progress_updates,omitempty"`
	ToolResults            []projectedTurnSummaryToolItem `json:"tool_results,omitempty"`
}

type projectedTurnSummaryToolItem struct {
	ToolName  string          `json:"tool_name,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Output    json.RawMessage `json:"output,omitempty"`
}

type projectedTurnBlockEnvelope struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data,omitempty"`
}

func decodeTurnSummaryProjection(raw []byte) (projectedTurnSummary, bool, error) {
	if len(raw) == 0 {
		return projectedTurnSummary{}, false, nil
	}
	var blocks []projectedTurnBlockEnvelope
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return projectedTurnSummary{}, false, err
	}
	for _, block := range blocks {
		if strings.TrimSpace(block.Kind) != "turn_summary" {
			continue
		}
		data := bytes.TrimSpace(block.Data)
		if len(data) == 0 || bytes.Equal(data, []byte("null")) || bytes.Equal(data, []byte("{}")) {
			return projectedTurnSummary{}, false, nil
		}
		var summary projectedTurnSummary
		if err := json.Unmarshal(data, &summary); err != nil {
			return projectedTurnSummary{}, false, fmt.Errorf("decode canonical turn_summary: %w", err)
		}
		summary = normalizeProjectedTurnSummary(summary)
		if summary.isZero() {
			return projectedTurnSummary{}, false, nil
		}
		return summary, true, nil
	}
	return projectedTurnSummary{}, false, nil
}

func normalizeProjectedTurnSummary(summary projectedTurnSummary) projectedTurnSummary {
	summary.AssistantVisibleOutput = strings.TrimSpace(summary.AssistantVisibleOutput)
	summary.Outcome = strings.TrimSpace(summary.Outcome)
	summary.ReasoningBlocks = trimStringSlice(summary.ReasoningBlocks)
	summary.ProgressUpdates = trimStringSlice(summary.ProgressUpdates)
	if len(summary.ToolResults) == 0 {
		summary.ToolResults = nil
	} else {
		out := make([]projectedTurnSummaryToolItem, 0, len(summary.ToolResults))
		for _, item := range summary.ToolResults {
			item.ToolName = strings.TrimSpace(item.ToolName)
			item.ToolUseID = strings.TrimSpace(item.ToolUseID)
			out = append(out, item)
		}
		summary.ToolResults = out
	}
	return summary
}

func (p projectedTurnSummary) isZero() bool {
	return p.AssistantVisibleOutput == "" &&
		p.Outcome == "" &&
		len(p.ReasoningBlocks) == 0 &&
		len(p.ProgressUpdates) == 0 &&
		len(p.ToolResults) == 0
}

func (p projectedTurnSummary) conversationFields() (string, string, []string, []string, []ConversationToolResult) {
	return p.AssistantVisibleOutput, p.Outcome, cloneStringSlice(p.ReasoningBlocks), cloneStringSlice(p.ProgressUpdates), p.toolResultsTransport()
}

func (p projectedTurnSummary) toolResultsTransport() []ConversationToolResult {
	if len(p.ToolResults) == 0 {
		return nil
	}
	out := make([]ConversationToolResult, 0, len(p.ToolResults))
	for _, item := range p.ToolResults {
		row := ConversationToolResult{
			ToolName: item.ToolName,
		}
		if item.ToolUseID != "" {
			row.ToolUseID = item.ToolUseID
		}
		if item.Output != nil {
			row.Output = append(json.RawMessage(nil), item.Output...)
		}
		out = append(out, row)
	}
	return out
}

func (p projectedTurnSummary) lastToolTransport(parseOK bool) (*AgentLastTool, error) {
	if len(p.ToolResults) == 0 {
		return nil, nil
	}
	last := p.ToolResults[len(p.ToolResults)-1]
	if last.ToolName == "" {
		return nil, fmt.Errorf("latest canonical tool_result is missing tool_name")
	}
	out := &AgentLastTool{
		Name: last.ToolName,
		OK:   parseOK,
	}
	if last.ToolUseID != "" {
		out.ToolUseID = last.ToolUseID
	}
	if last.Output != nil {
		trimmed := bytes.TrimSpace(last.Output)
		if len(trimmed) == 0 {
			return nil, fmt.Errorf("latest canonical tool_result output is empty")
		}
		if !json.Valid(trimmed) {
			return nil, fmt.Errorf("latest canonical tool_result output is invalid JSON")
		}
		out.Result = append(json.RawMessage(nil), trimmed...)
	}
	return out, nil
}

func trimStringSlice(in []string) []string {
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

func cloneStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	return append([]string(nil), in...)
}
