package platformcontext

import "strings"

const EntityRoot = "instance"

func EntityMetadata(entityID, currentState, flowInstance string) map[string]any {
	out := map[string]any{
		"id":   strings.TrimSpace(entityID),
		"path": strings.TrimSpace(flowInstance),
	}
	if stage := strings.TrimSpace(currentState); stage != "" {
		out["stage"] = stage
	}
	return out
}

func EntityFieldUnsupported(field string) bool {
	switch strings.TrimSpace(field) {
	case "entity_id", "entity_type", "workflow_name", "workflow_version", "run_id", "fields", "accumulator", "entered_state_at", "revision", "created_at", "updated_at", "name":
		return true
	default:
		return false
	}
}

func LegacyEntityMetadataField(field string) bool {
	switch strings.TrimSpace(field) {
	case "entity_id", "flow_instance", "current_state", "gates", "revision", "created_at", "updated_at", "entity_type", "workflow_name", "workflow_version":
		return true
	default:
		return false
	}
}
