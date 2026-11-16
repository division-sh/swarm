package llm

import (
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
)

// CompletionAwaitCall validates a serial prefix against the immutable response;
// it does not execute a call or interpret an authored reply declaration.
func CompletionAwaitCall(payload json.RawMessage, adapter string, index int, prefix json.RawMessage, operationID, requestID string, surface managedcapabilities.Surface) (string, string, error) {
	continuation, err := completionContinuationForPayload(payload, adapter)
	if err != nil {
		return "", "", err
	}
	if continuation == nil || continuation.Projection.FrameID == "" || index < 0 || index >= len(continuation.Response.ToolCalls) {
		return "", "", fmt.Errorf("await requires an exact pending completion call and frame")
	}
	candidates, err := ReadCompletionOutputCandidates(payload, adapter, operationID, surface)
	if err != nil {
		return "", "", err
	}
	call := continuation.Response.ToolCalls[index]
	output, err := continuation.Response.ToolOutputAuthority.eventIdentity(completionToolCallIdentity(continuation.Projection.TurnCount, index, call))
	if err != nil {
		return "", "", err
	}
	callableEmit := false
	for _, candidate := range candidates {
		callableEmit = callableEmit || (candidate.EventID == output.EventID() && candidate.ToolName == call.Name)
	}
	if !callableEmit || output.EventID() != requestID {
		return "", "", fmt.Errorf("await request is not this exact callable emit's output")
	}
	if err := validateCompletionAwaitPrefix(continuation.Response.ToolCalls, index, prefix); err != nil {
		return "", "", err
	}
	return continuation.Projection.FrameID, continuation.Response.ToolCalls[index].ID, nil
}

func validateCompletionAwaitPrefix(calls []ToolCall, index int, prefix json.RawMessage) error {
	if _, err := canonicaljson.Decode(prefix); err != nil {
		return err
	}
	var entries []struct {
		CallID  string          `json:"tool_call_id"`
		Name    string          `json:"name"`
		OK      *bool           `json:"ok"`
		Result  json.RawMessage `json:"result"`
		Failure json.RawMessage `json:"failure"`
	}
	if err := json.Unmarshal(prefix, &entries); err != nil || entries == nil || len(entries) != index {
		return fmt.Errorf("await requires the complete ordered tool-result prefix")
	}
	seen := make(map[string]bool)
	for i, call := range calls {
		if call.ID == "" || call.Name == "" || seen[call.ID] {
			return fmt.Errorf("await response contains ambiguous tool calls")
		}
		seen[call.ID] = true
		if i < index && (entries[i].CallID != call.ID || entries[i].Name != call.Name || entries[i].OK == nil ||
			(*entries[i].OK && (len(entries[i].Result) == 0 || len(entries[i].Failure) != 0)) ||
			(!*entries[i].OK && (len(entries[i].Failure) == 0 || len(entries[i].Result) != 0))) {
			return fmt.Errorf("await prefix differs from its assistant call or lacks its outcome")
		}
	}
	return nil
}
