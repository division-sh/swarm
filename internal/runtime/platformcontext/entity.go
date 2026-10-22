package platformcontext

import "strings"

const EntityRoot = "_entity"

func EntityMetadata(entityID, currentState, flowInstance string, gates map[string]bool) map[string]any {
	out := map[string]any{
		"id":            strings.TrimSpace(entityID),
		"current_state": strings.TrimSpace(currentState),
		"flow_instance": strings.TrimSpace(flowInstance),
		"gates":         boolMapToAnyMap(gates),
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

func boolMapToAnyMap(in map[string]bool) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key != "" {
			out[key] = value
		}
	}
	return out
}
