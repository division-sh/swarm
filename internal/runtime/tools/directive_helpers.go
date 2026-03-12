package tools

import (
	"encoding/json"
	"strings"

	"empireai/internal/events"
	runtimesharedjson "empireai/internal/runtime/sharedjson"
)

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
	verticalID = strings.TrimSpace(evt.EntityID())
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
