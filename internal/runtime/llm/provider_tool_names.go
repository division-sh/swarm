package llm

import (
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

// One common API spelling fits the providers' alphabet and Chat Completions'
// 64-character ceiling. Canonical names never change.
const providerToolNameLimit = 64

func providerToolWireName(name string) string {
	var wire strings.Builder
	for _, char := range strings.TrimSpace(name) {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' {
			wire.WriteRune(char)
		} else {
			wire.WriteByte('_')
		}
	}
	return wire.String()
}

func validateProviderToolWireName(canonical, wire string) error {
	if wire == "" || len(wire) > providerToolNameLimit {
		return runtimefailures.New(runtimefailures.ClassSchemaInvalid, "provider_tool_name_invalid", "llm-runtime", "admit_api_tool_names", map[string]any{
			"tool": canonical, "wire_name": wire, "maximum_length": providerToolNameLimit,
		})
	}
	return nil
}

func admitProviderToolNames(tools []ToolDefinition) (map[string]string, error) {
	names := make(map[string]string, len(tools))
	for _, tool := range tools {
		canonical := strings.TrimSpace(tool.Name)
		wire := providerToolWireName(canonical)
		if err := validateProviderToolWireName(canonical, wire); err != nil {
			return nil, err
		}
		if prior, found := names[wire]; found {
			return nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "provider_tool_name_collision", "llm-runtime", "admit_api_tool_names", map[string]any{
				"tool": canonical, "other_tool": prior, "wire_name": wire,
			})
		}
		names[wire] = canonical
	}
	return names, nil
}

func validateProviderReplayToolNames(messages []Message, input Message) error {
	for _, message := range append(append([]Message(nil), messages...), input) {
		for _, call := range message.ToolCalls {
			if strings.TrimSpace(call.Name) == "" {
				continue
			}
			if err := validateProviderToolWireName(call.Name, providerToolWireName(call.Name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func resolveProviderToolNames(response *Response, names map[string]string) {
	for index := range response.ToolCalls {
		wire := response.ToolCalls[index].Name
		canonical := names[wire]
		// An unresolved call has no executable canonical name. Its original
		// message and raw response still retain the provider spelling for replay.
		response.ToolCalls[index].Name = canonical
		if canonical != "" {
			response.Message.ToolCalls[index].Name = canonical
		}
	}
}
