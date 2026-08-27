package builder

import (
	"strings"
)

func coercePayload(raw any) map[string]any {
	switch typed := raw.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[key] = value
		}
		return out
	default:
		if typed == nil {
			return map[string]any{}
		}
		return map[string]any{"value": typed}
	}
}

func stringSet(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func cloneRunEvent(in RunEventEnvelope) RunEventEnvelope {
	out := make(RunEventEnvelope, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
