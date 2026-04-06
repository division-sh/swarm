package eventpayload

import "strings"

func IsRuntimeOwnedCanonicalContextField(key string) bool {
	switch strings.TrimSpace(key) {
	case "entity_id", "flow_instance", "trigger_event_type", "current_state", "task_id", "timer_handle":
		return true
	default:
		return false
	}
}

func StripUndeclaredRuntimeOwnedCanonicalContext(payload map[string]any, allowed map[string]struct{}) map[string]any {
	if len(payload) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if IsRuntimeOwnedCanonicalContextField(key) {
			if _, ok := allowed[key]; !ok {
				continue
			}
		}
		out[key] = value
	}
	return out
}

func StripRuntimeOwnedCanonicalContext(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		key = strings.TrimSpace(key)
		if key == "" || IsRuntimeOwnedCanonicalContextField(key) {
			continue
		}
		out[key] = value
	}
	return out
}

func TrimToAllowedKeys(payload map[string]any, allowed map[string]struct{}) map[string]any {
	if len(payload) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok && !IsRuntimeOwnedCanonicalContextField(key) {
			continue
		}
		out[key] = value
	}
	return out
}
