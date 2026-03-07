package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimesharedjson "empireai/internal/runtime/sharedjson"
)

func asFloat64(v any) (float64, bool) {
	return runtimesharedjson.AsFloat64(v)
}

func schemaAdditionalProps(raw any) bool {
	return runtimesharedjson.SchemaAdditionalProps(raw)
}

func isNumeric(v any) bool {
	return runtimesharedjson.IsNumeric(v)
}

func isInteger(v any) bool {
	return runtimesharedjson.IsInteger(v)
}

func asArray(v any) ([]any, bool) {
	return runtimesharedjson.AsArray(v)
}

func schemaProperties(raw any) map[string]map[string]any {
	return runtimesharedjson.SchemaProperties(raw)
}

func requiredList(raw any) []string {
	return runtimesharedjson.RequiredList(raw)
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

func parsePayloadMap(raw []byte) map[string]any {
	return runtimesharedjson.ParsePayloadMap(raw)
}

func mustJSON(v any) []byte {
	return runtimesharedjson.MustJSON(v)
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		t = strings.TrimSpace(t)
		if t == "" {
			return 0
		}
		var n int
		if _, err := fmt.Sscanf(t, "%d", &n); err == nil {
			return n
		}
	}
	return 0
}

func toStringList(raw any) []string {
	switch t := raw.(type) {
	case nil:
		return nil
	case []string:
		out := make([]string, 0, len(t))
		for _, item := range t {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			s := strings.TrimSpace(fmt.Sprintf("%v", item))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		text := strings.TrimSpace(t)
		if text == "" {
			return nil
		}
		if strings.HasPrefix(text, "[") {
			var items []string
			if err := json.Unmarshal([]byte(text), &items); err == nil {
				return toStringList(items)
			}
		}
		parts := strings.Split(text, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
		return out
	default:
		return []string{strings.TrimSpace(fmt.Sprintf("%v", raw))}
	}
}

func WeekStartUTC(now time.Time, resetDay string) time.Time {
	now = now.UTC()
	target := parseWeekday(resetDay)
	daysBack := (int(now.Weekday()) - int(target) + 7) % 7
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -daysBack)
	return start
}

func NextWeekResetUTC(now time.Time, resetDay string) time.Time {
	start := WeekStartUTC(now, resetDay)
	return start.Add(7 * 24 * time.Hour)
}

func parseWeekday(raw string) time.Weekday {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "sunday":
		return time.Sunday
	case "monday":
		return time.Monday
	case "tuesday":
		return time.Tuesday
	case "wednesday":
		return time.Wednesday
	case "thursday":
		return time.Thursday
	case "friday":
		return time.Friday
	case "saturday":
		return time.Saturday
	default:
		return time.Monday
	}
}
