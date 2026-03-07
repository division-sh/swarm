package sharedjson

import (
	"encoding/json"
	"fmt"
	"strings"
)

func AsFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func SchemaAdditionalProps(raw any) bool {
	if raw == nil {
		return true
	}
	if b, ok := raw.(bool); ok {
		return b
	}
	return true
}

func IsNumeric(v any) bool {
	switch v.(type) {
	case float64, float32, int, int64, int32, uint, uint64, uint32:
		return true
	default:
		return false
	}
}

func IsInteger(v any) bool {
	switch t := v.(type) {
	case int, int64, int32, uint, uint64, uint32:
		return true
	case float64:
		return t == float64(int64(t))
	case float32:
		return t == float32(int64(t))
	default:
		return false
	}
}

func AsArray(v any) ([]any, bool) {
	switch t := v.(type) {
	case []any:
		return t, true
	case []string:
		out := make([]any, 0, len(t))
		for _, s := range t {
			out = append(out, s)
		}
		return out, true
	default:
		return nil, false
	}
}

func SchemaProperties(raw any) map[string]map[string]any {
	out := map[string]map[string]any{}
	switch t := raw.(type) {
	case map[string]any:
		for k, v := range t {
			if s, ok := v.(map[string]any); ok {
				out[k] = s
			}
		}
	}
	return out
}

func RequiredList(raw any) []string {
	switch t := raw.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, v := range t {
			if s := strings.TrimSpace(AsString(v)); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func AsString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	case fmt.Stringer:
		return t.String()
	default:
		return fmt.Sprint(v)
	}
}

func ParsePayloadMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
		return map[string]any{}
	}
	return payload
}

func MustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil || len(raw) == 0 {
		return []byte("{}")
	}
	return raw
}
