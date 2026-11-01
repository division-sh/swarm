package operatorsurface

import (
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorread"
)

// Read the input actually recorded for this turn, not a fresh projection of
// today's config. Assistant history/reasoning and CLI arguments stay private.
func projectProviderRendering(raw []byte) (operatorread.OperatorProviderRendering, error) {
	out := operatorread.OperatorProviderRendering{Status: "unresolved", Source: "recorded_provider_request"}
	if len(raw) == 0 {
		return out, nil
	}
	var request map[string]json.RawMessage
	if err := json.Unmarshal(raw, &request); err != nil {
		return out, fmt.Errorf("decode recorded provider rendering: %w", err)
	}
	var items []json.RawMessage
	input := request["messages"]
	if len(input) == 0 {
		input = request["input"]
	}
	if len(input) > 0 {
		if err := json.Unmarshal(input, &items); err != nil {
			return out, fmt.Errorf("decode recorded provider input: %w", err)
		}
		start := len(items)
		for start > 0 {
			var item struct {
				Role string `json:"role"`
				Type string `json:"type"`
			}
			if err := json.Unmarshal(items[start-1], &item); err != nil {
				return out, err
			}
			if item.Role != "user" && item.Role != "tool" && item.Type != "function_call_output" {
				break
			}
			start--
		}
		if start < len(items) {
			out.Input, _ = json.Marshal(items[start:])
		}
	} else if prompt := request["provider_input"]; len(prompt) > 0 {
		out.Input = append(json.RawMessage(nil), prompt...)
	} else if message := request["message"]; len(message) > 0 {
		out.Input = append(json.RawMessage(nil), message...)
	}
	if len(out.Input) > 0 {
		out.Status = "recorded"
		out.Tools = append(json.RawMessage(nil), request["tools"]...)
	}
	return out, nil
}
