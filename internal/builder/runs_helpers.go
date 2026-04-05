package builder

import (
	"strings"

	"github.com/google/uuid"
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

func normalizeHumanDecision(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "approve", "approved":
		return "approved"
	case "reject", "rejected":
		return "rejected"
	case "defer", "deferred":
		return "deferred"
	default:
		return ""
	}
}

func nonEmptyOrUUID(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return uuid.NewString()
}

func payloadMap(raw any) map[string]any {
	switch typed := raw.(type) {
	case map[string]any:
		return typed
	default:
		return map[string]any{}
	}
}

func cloneRunEvent(in RunEventEnvelope) RunEventEnvelope {
	out := make(RunEventEnvelope, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
