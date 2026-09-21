package contracts

import (
	"fmt"
	"strings"
)

const SystemNodeExecutionType = "system_node"

func ValidateSystemNodeExecutionType(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return fmt.Errorf("missing system node execution_type")
	}
	if strings.TrimSpace(strings.ToLower(raw)) != SystemNodeExecutionType {
		return fmt.Errorf("unsupported execution_type %q", raw)
	}
	return nil
}
