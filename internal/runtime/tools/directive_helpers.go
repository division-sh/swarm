package tools

import (
	"encoding/json"
	"strings"

	"empireai/internal/events"
	runtimesharedjson "empireai/internal/runtime/sharedjson"
)

func inferDiscoveryMode(text string) string {
	t := strings.ToLower(strings.TrimSpace(text))
	switch {
	case strings.Contains(t, "automation_micro"),
		(strings.Contains(t, "automation") && strings.Contains(t, "micro")):
		return "saas_gap"
	case strings.Contains(t, "local service"), strings.Contains(t, "local_services"):
		return "local_services"
	case strings.Contains(t, "trend"), strings.Contains(t, "saas_trend"):
		return "saas_trend"
	default:
		return "saas_gap"
	}
}

func inferGeographyHint(text string) string {
	t := strings.TrimSpace(text)
	if t == "" {
		return ""
	}
	low := strings.ToLower(t)
	for _, geo := range []string{"paraguay", "argentina", "brazil", "mexico", "chile", "peru", "colombia", "uruguay"} {
		if strings.Contains(low, geo) {
			return geo
		}
	}
	return t
}

func extractCategoryList(payload map[string]any) []string {
	toList := func(v any) []string {
		switch t := v.(type) {
		case []any:
			out := make([]string, 0, len(t))
			for _, item := range t {
				s := strings.TrimSpace(asString(item))
				if s != "" {
					out = append(out, s)
				}
			}
			return out
		case []string:
			out := make([]string, 0, len(t))
			for _, item := range t {
				s := strings.TrimSpace(item)
				if s != "" {
					out = append(out, s)
				}
			}
			return out
		default:
			return nil
		}
	}
	if out := toList(payload["taxonomy_categories"]); len(out) > 0 {
		return out
	}
	if out := toList(payload["categories"]); len(out) > 0 {
		return out
	}
	return []string{}
}

func budgetEventTypeFromThresholdPayload(raw []byte) events.EventType {
	state := strings.ToLower(strings.TrimSpace(fieldStringFromJSON(raw, "state")))
	switch state {
	case "emergency":
		return events.EventType("budget.emergency")
	case "throttle":
		return events.EventType("budget.throttle")
	case "warning":
		return events.EventType("budget.warning")
	case "ok", "resumed":
		return events.EventType("budget.resumed")
	}
	return events.EventType("")
}

func fieldStringFromJSON(raw []byte, key string) string {
	if len(raw) == 0 || strings.TrimSpace(key) == "" {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return ""
	}
	return strings.TrimSpace(asString(obj[key]))
}

func transitionContextKey(primary events.Event, fallback events.Event) string {
	verticalID, taskID := extractContextIDs(primary)
	if strings.TrimSpace(verticalID) == "" || strings.TrimSpace(taskID) == "" {
		fallbackVertical, fallbackTask := extractContextIDs(fallback)
		if strings.TrimSpace(verticalID) == "" {
			verticalID = fallbackVertical
		}
		if strings.TrimSpace(taskID) == "" {
			taskID = fallbackTask
		}
	}
	return verticalID + "|" + taskID
}

func extractContextIDs(evt events.Event) (verticalID, taskID string) {
	verticalID = strings.TrimSpace(evt.VerticalID)
	taskID = strings.TrimSpace(evt.TaskID)
	if len(evt.Payload) == 0 {
		return verticalID, taskID
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil || payload == nil {
		return verticalID, taskID
	}
	if verticalID == "" {
		for _, key := range []string{"vertical_id", "vertical_ref"} {
			v := strings.TrimSpace(asString(payload[key]))
			if v != "" {
				verticalID = v
				break
			}
		}
	}
	if taskID == "" {
		for _, key := range []string{"task_id", "task_ref"} {
			v := strings.TrimSpace(asString(payload[key]))
			if v != "" {
				taskID = v
				break
			}
		}
	}
	return verticalID, taskID
}

func parsePayloadMap(raw []byte) map[string]any {
	return runtimesharedjson.ParsePayloadMap(raw)
}
