package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"empireai/internal/models"
	runtimetools "empireai/internal/runtime/tools"
)

func coalesce(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func asFloat64(v any) (float64, bool) {
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

func schemaAdditionalProps(raw any) bool {
	if raw == nil {
		return true
	}
	if b, ok := raw.(bool); ok {
		return b
	}
	return true
}

func isNumeric(v any) bool {
	switch v.(type) {
	case float64, float32, int, int64, int32, uint, uint64, uint32:
		return true
	default:
		return false
	}
}

func isInteger(v any) bool {
	switch v.(type) {
	case int, int64, int32, uint, uint64, uint32:
		return true
	case float64:
		return v.(float64) == float64(int64(v.(float64)))
	case float32:
		return v.(float32) == float32(int64(v.(float32)))
	default:
		return false
	}
}

func asArray(v any) ([]any, bool) {
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

func schemaProperties(raw any) map[string]map[string]any {
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

func requiredList(raw any) []string {
	switch t := raw.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, v := range t {
			if s := strings.TrimSpace(asString(v)); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
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

func WithActor(ctx context.Context, actor models.AgentConfig) context.Context {
	return runtimetools.WithActor(ctx, actor)
}

func ActorFromContext(ctx context.Context) (models.AgentConfig, bool) {
	return runtimetools.ActorFromContext(ctx)
}
