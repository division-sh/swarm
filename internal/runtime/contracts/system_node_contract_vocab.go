package contracts

import (
	"fmt"
	"strings"
)

const SystemNodeExecutionType = "system_node"

func NormalizeHandlerActionID(id string) string {
	return strings.TrimSpace(strings.ToLower(id))
}

func IsSupportedHandlerActionID(id string) bool {
	switch NormalizeHandlerActionID(id) {
	case "create_flow_instance", "record_evidence", "mailbox_write", "artifact_repo_commit":
		return true
	default:
		return false
	}
}

func ParseHandlerActionID(id string) (string, error) {
	normalized := NormalizeHandlerActionID(id)
	if normalized == "" {
		return "", nil
	}
	if !IsSupportedHandlerActionID(normalized) {
		return "", fmt.Errorf("unsupported handler action %q", id)
	}
	return normalized, nil
}

func ValidateSystemNodeExecutionType(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return fmt.Errorf("missing system node execution_type")
	}
	if strings.TrimSpace(strings.ToLower(raw)) != SystemNodeExecutionType {
		return fmt.Errorf("unsupported execution_type %q", raw)
	}
	return nil
}
