package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	runtimellm "swarm/internal/runtime/llm"
	"swarm/internal/store"
)

func projectConversationSummaryMetadata(p store.ConversationRuntimeStateDescriptor) ConversationSummaryMetadata {
	meta := ConversationSummaryMetadata{
		ProviderSessionID:    p.ProviderSessionID,
		RetryReason:          p.RetryReason,
		RetriesFromSessionID: p.RetriesFromSessionID,
	}
	if p.Watchdog != nil {
		meta.Watchdog = &ConversationRuntimeWatchdog{
			State:         p.Watchdog.State,
			BlockingLayer: p.Watchdog.BlockingLayer,
			Action:        p.Watchdog.Action,
			Outcome:       p.Watchdog.Outcome,
			LastOutputAt:  p.Watchdog.LastOutputAt,
			RecordedAt:    p.Watchdog.RecordedAt,
		}
	}
	return meta
}

func projectLatestTurn(taskID string, parseOK bool, turnID string, turnBlocksRaw []byte) (*OperatorLiveTurn, error) {
	taskID = strings.TrimSpace(taskID)
	turnID = strings.TrimSpace(turnID)
	summary, ok, err := decodeTurnSummaryProjection(turnBlocksRaw)
	if err != nil {
		return nil, err
	}
	if !ok && taskID == "" && turnID == "" {
		return nil, nil
	}
	out := &OperatorLiveTurn{
		TurnID:  turnID,
		TaskID:  taskID,
		ParseOK: parseOK,
	}
	if ok {
		out.AssistantVisibleOutput = summary.AssistantVisibleOutput
		out.Outcome = summary.Outcome
		out.ProgressUpdates = cloneStringSlice(summary.ProgressUpdates)
		out.LastTool, err = projectedTurnSummaryLastToolTransport(summary, parseOK)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
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
	if p.Watchdog != nil {
		state.Watchdog = &ConversationRuntimeWatchdog{
			State:         p.Watchdog.State,
			BlockingLayer: p.Watchdog.BlockingLayer,
			Action:        p.Watchdog.Action,
			Outcome:       p.Watchdog.Outcome,
			LastOutputAt:  p.Watchdog.LastOutputAt,
			RecordedAt:    p.Watchdog.RecordedAt,
		}
	}
	return state
}

func decodeTurnSummaryProjection(raw []byte) (runtimellm.TurnSummaryTurnBlockData, bool, error) {
	summary, ok, err := runtimellm.DecodeCanonicalTurnSummaryJSON(raw)
	if err != nil {
		return runtimellm.TurnSummaryTurnBlockData{}, false, err
	}
	return summary, ok, nil
}

func projectedTurnSummaryConversationFields(p runtimellm.TurnSummaryTurnBlockData) (string, string, []string, []string, []ConversationToolResult) {
	return p.AssistantVisibleOutput, p.Outcome, cloneStringSlice(p.ReasoningBlocks), cloneStringSlice(p.ProgressUpdates), projectedTurnSummaryToolResultsTransport(p)
}

func projectedTurnSummaryToolResultsTransport(p runtimellm.TurnSummaryTurnBlockData) []ConversationToolResult {
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

func projectedTurnSummaryLastToolTransport(p runtimellm.TurnSummaryTurnBlockData, parseOK bool) (*AgentLastTool, error) {
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

func decodeJSONArrayInto[T any](raw []byte, target *[]T) error {
	data := bytes.TrimSpace(raw)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		*target = nil
		return nil
	}
	return json.Unmarshal(data, target)
}

func cloneStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	return append([]string(nil), in...)
}
