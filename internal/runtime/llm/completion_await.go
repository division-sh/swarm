package llm

import (
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// CompletionAwaitCall validates a serial prefix against the immutable response;
// it does not execute a call or interpret an authored reply declaration.
func CompletionAwaitCall(payload json.RawMessage, adapter string, index int, prefix json.RawMessage) (string, string, error) {
	continuation, err := completionContinuationForPayload(payload, adapter)
	if err != nil {
		return "", "", err
	}
	if continuation == nil || continuation.Projection.FrameID == "" || index < 0 || index >= len(continuation.Response.ToolCalls) {
		return "", "", fmt.Errorf("await requires an exact pending completion call and frame")
	}
	if _, err := canonicaljson.Decode(prefix); err != nil {
		return "", "", err
	}
	var entries []struct {
		CallID  string          `json:"tool_call_id"`
		Name    string          `json:"name"`
		OK      *bool           `json:"ok"`
		Result  json.RawMessage `json:"result"`
		Failure json.RawMessage `json:"failure"`
	}
	if err := json.Unmarshal(prefix, &entries); err != nil || entries == nil || len(entries) != index {
		return "", "", fmt.Errorf("await requires the complete ordered tool-result prefix")
	}
	seen := make(map[string]bool)
	for i, call := range continuation.Response.ToolCalls {
		if call.ID == "" || call.Name == "" || seen[call.ID] {
			return "", "", fmt.Errorf("await response contains ambiguous tool calls")
		}
		seen[call.ID] = true
		if i < index && (entries[i].CallID != call.ID || entries[i].Name != call.Name || entries[i].OK == nil ||
			(*entries[i].OK && (len(entries[i].Result) == 0 || len(entries[i].Failure) != 0)) ||
			(!*entries[i].OK && (len(entries[i].Failure) == 0 || len(entries[i].Result) != 0))) {
			return "", "", fmt.Errorf("await prefix differs from its assistant call or lacks its outcome")
		}
	}
	return continuation.Projection.FrameID, continuation.Response.ToolCalls[index].ID, nil
}
