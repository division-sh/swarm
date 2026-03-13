package pipeline

import (
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func normalizeHandlerStateField(field string) string {
	field = strings.TrimSpace(field)
	switch {
	case strings.HasPrefix(field, "entity."):
		return strings.TrimSpace(strings.TrimPrefix(field, "entity."))
	case strings.HasPrefix(field, "metadata."):
		return strings.TrimSpace(strings.TrimPrefix(field, "metadata."))
	default:
		return field
	}
}

func handlerGuardOnFail(spec *runtimecontracts.GuardSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.OnFail)
}

func normalizeWorkflowGuardFailureAction(action string) string {
	action = strings.TrimSpace(strings.ToLower(action))
	switch action {
	case "":
		return ""
	case "block":
		return "blocked"
	default:
		return action
	}
}

func normalizeWorkflowBuiltinGuardID(id string) string {
	return strings.TrimSpace(strings.ToLower(id))
}

func normalizeWorkflowBuiltinActionID(id string) string {
	return strings.TrimSpace(strings.ToLower(id))
}

func isSupportedWorkflowGuardBuiltin(id string) bool {
	switch normalizeWorkflowBuiltinGuardID(id) {
	case "has_entity_id",
		"has_vertical_id",
		"has_human_decision",
		"not_in_terminal_state",
		"not_in_terminal_stage",
		"not_in_operating_phase",
		"revision_count_below_limit",
		"inner_revision_count_below_limit",
		"state_in_phase":
		return true
	default:
		return false
	}
}

func isSupportedWorkflowActionBuiltin(id string) bool {
	switch normalizeWorkflowBuiltinActionID(id) {
	case "increment_revision_count",
		"record_state_change",
		"update_state",
		"cancel_state_timers",
		"start_state_timers",
		"record_evidence",
		"create_flow_instance":
		return true
	default:
		return false
	}
}
