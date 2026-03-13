package pipeline

import (
	"encoding/json"
	"strings"

	runtimesharedjson "empireai/internal/runtime/sharedjson"
)

const DefaultSystemNodeRetryLimit = 5

func mustJSON(v any) []byte {
	return runtimesharedjson.MustJSON(v)
}

func parsePayloadMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func payloadMap(v any) map[string]any {
	if v == nil {
		return map[string]any{}
	}
	switch typed := v.(type) {
	case map[string]any:
		return cloneMap(typed)
	default:
		var out map[string]any
		if err := json.Unmarshal(mustJSON(v), &out); err != nil || out == nil {
			return map[string]any{}
		}
		return out
	}
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func asString(v any) string {
	return strings.TrimSpace(runtimesharedjson.AsString(v))
}

func boolFromAny(v any) bool {
	switch typed := v.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			return true
		default:
			return false
		}
	default:
		return asInt(v) > 0
	}
}

func firstNonEmptyString(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func workflowExpressionLookupPath(root map[string]any, path string) (any, bool) {
	current := any(root)
	for _, segment := range strings.Split(strings.TrimSpace(path), ".") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return nil, false
		}
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := m[segment]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
}
