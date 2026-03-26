package tools

import (
	"encoding/json"
	"strings"

	"swarm/internal/events"
	runtimesharedjson "swarm/internal/runtime/sharedjson"
)

func transitionContextKey(primary events.Event, fallback events.Event) string {
	entityID, taskID := extractContextIDs(primary)
	if strings.TrimSpace(entityID) == "" || strings.TrimSpace(taskID) == "" {
		fallbackEntity, fallbackTask := extractContextIDs(fallback)
		if strings.TrimSpace(entityID) == "" {
			entityID = fallbackEntity
		}
		if strings.TrimSpace(taskID) == "" {
			taskID = fallbackTask
		}
	}
	return entityID + "|" + taskID
}

func extractContextIDs(evt events.Event) (entityID, taskID string) {
	entityID = strings.TrimSpace(evt.EntityID())
	taskID = strings.TrimSpace(evt.TaskID)
	if len(evt.Payload) == 0 {
		return entityID, taskID
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil || payload == nil {
		return entityID, taskID
	}
	if entityID == "" {
		for _, key := range []string{"entity_id"} {
			v := strings.TrimSpace(asString(payload[key]))
			if v != "" {
				entityID = v
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
	return entityID, taskID
}

func parsePayloadMap(raw []byte) map[string]any {
	return runtimesharedjson.ParsePayloadMap(raw)
}
