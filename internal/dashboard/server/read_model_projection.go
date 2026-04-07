package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"swarm/internal/store"
)

func projectConversationSummaryMetadata(p store.ConversationRuntimeStateDescriptor) ConversationSummaryMetadata {
	return ConversationSummaryMetadata{
		ProviderSessionID:    p.ProviderSessionID,
		RetryReason:          p.RetryReason,
		RetriesFromSessionID: p.RetriesFromSessionID,
	}
}

func projectConversationRuntimeState(p store.ConversationRuntimeStateDescriptor) ConversationRuntimeState {
	state := ConversationRuntimeState{
		Summary:              p.Summary,
		ProviderSessionID:    p.ProviderSessionID,
		RetryReason:          p.RetryReason,
		RetriesFromSessionID: p.RetriesFromSessionID,
	}
	if p.LastTurn != nil {
		state.LastTurn = &ConversationRuntimeLastTurn{
			TaskID:  p.LastTurn.TaskID,
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
