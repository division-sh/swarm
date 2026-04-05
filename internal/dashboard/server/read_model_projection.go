package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type projectedConversationRuntimeState struct {
	Summary  string
	LastTurn *projectedConversationLastTurn
	raw      map[string]any
}

type projectedConversationLastTurn struct {
	TaskID  string `json:"task_id,omitempty"`
	ParseOK bool   `json:"parse_ok,omitempty"`
}

func decodeConversationRuntimeStateProjection(raw []byte) (projectedConversationRuntimeState, error) {
	if len(raw) == 0 {
		return projectedConversationRuntimeState{raw: map[string]any{}}, nil
	}
	var projected projectedConversationRuntimeState
	if err := json.Unmarshal(raw, &projected.raw); err != nil {
		return projectedConversationRuntimeState{}, err
	}
	var typed struct {
		Summary  string                         `json:"summary"`
		LastTurn *projectedConversationLastTurn `json:"last_turn,omitempty"`
	}
	if err := json.Unmarshal(raw, &typed); err != nil {
		return projectedConversationRuntimeState{}, err
	}
	projected.Summary = strings.TrimSpace(typed.Summary)
	projected.LastTurn = typed.LastTurn
	return projected, nil
}

func (p projectedConversationRuntimeState) metadataMap() map[string]any {
	return omitKnownKeys(p.raw, "summary", "last_turn")
}

func (p projectedConversationRuntimeState) runtimeStateMap() map[string]any {
	return cloneAnyMap(p.raw)
}

type projectedTurnSummary struct {
	AssistantVisibleOutput string                         `json:"assistant_visible_output,omitempty"`
	Outcome                string                         `json:"outcome,omitempty"`
	ReasoningBlocks        []string                       `json:"reasoning_blocks,omitempty"`
	ProgressUpdates        []string                       `json:"progress_updates,omitempty"`
	ToolResults            []projectedTurnSummaryToolItem `json:"tool_results,omitempty"`
}

type projectedTurnSummaryToolItem struct {
	ToolName  string `json:"tool_name,omitempty"`
	ToolUseID string `json:"tool_use_id,omitempty"`
	Output    any    `json:"output,omitempty"`
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
	if summary.Outcome == "" {
		summary.Outcome = summary.AssistantVisibleOutput
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

func (p projectedTurnSummary) conversationFields() (string, string, []string, []string, []any) {
	return p.AssistantVisibleOutput, p.Outcome, cloneStringSlice(p.ReasoningBlocks), cloneStringSlice(p.ProgressUpdates), p.toolResultsAny()
}

func (p projectedTurnSummary) toolResultsAny() []any {
	if len(p.ToolResults) == 0 {
		return nil
	}
	out := make([]any, 0, len(p.ToolResults))
	for _, item := range p.ToolResults {
		row := map[string]any{
			"tool_name": item.ToolName,
		}
		if item.ToolUseID != "" {
			row["tool_use_id"] = item.ToolUseID
		}
		if item.Output != nil {
			row["output"] = item.Output
		}
		out = append(out, row)
	}
	return out
}

func (p projectedTurnSummary) lastToolMap(parseOK bool) map[string]any {
	if len(p.ToolResults) == 0 {
		return nil
	}
	last := p.ToolResults[len(p.ToolResults)-1]
	if last.ToolName == "" {
		return nil
	}
	out := map[string]any{
		"name": last.ToolName,
		"ok":   parseOK,
	}
	if last.Output != nil {
		out["result"] = last.Output
	}
	return out
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func omitKnownKeys(in map[string]any, keys ...string) map[string]any {
	if len(in) == 0 {
		return nil
	}
	drop := map[string]struct{}{}
	for _, key := range keys {
		drop[key] = struct{}{}
	}
	out := map[string]any{}
	for key, value := range in {
		if _, skip := drop[key]; skip {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
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
