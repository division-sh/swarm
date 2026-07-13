package contracts

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

var workflowGateInputTypes = map[string]struct{}{
	"text":      {},
	"integer":   {},
	"numeric":   {},
	"boolean":   {},
	"timestamp": {},
	"uuid":      {},
}

// NormalizeWorkflowGateInputType owns the closed scalar vocabulary accepted
// by authored gates, runtime decision validation, and outcome emission checks.
func NormalizeWorkflowGateInputType(raw string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if _, ok := workflowGateInputTypes[normalized]; ok {
		return normalized, nil
	}
	return "", fmt.Errorf("unsupported stage gate input type %q; use text, integer, numeric, boolean, timestamp, or uuid", strings.TrimSpace(raw))
}

func WorkflowGateInputValueMatches(kind string, value any) bool {
	kind, err := NormalizeWorkflowGateInputType(kind)
	if err != nil {
		return false
	}
	switch kind {
	case "text":
		_, ok := value.(string)
		return ok
	case "integer":
		switch typed := value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return true
		case float64:
			return typed == float64(int64(typed))
		case json.Number:
			_, err := typed.Int64()
			return err == nil
		}
	case "numeric":
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
			return true
		}
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "timestamp":
		text, ok := value.(string)
		if !ok {
			return false
		}
		_, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(text))
		return err == nil
	case "uuid":
		text, ok := value.(string)
		if !ok {
			return false
		}
		_, err := uuid.Parse(strings.TrimSpace(text))
		return err == nil
	}
	return false
}

// WorkflowGateInputTypeCompatible compares a canonical gate input with an
// event-field scalar. Event aliases are read-only migration vocabulary; gate
// authoring always stores the canonical type returned above.
func WorkflowGateInputTypeCompatible(inputType, eventType string) bool {
	inputType, err := NormalizeWorkflowGateInputType(inputType)
	if err != nil {
		return false
	}
	return inputType == workflowGateEventTypeFamily(eventType)
}

func workflowGateEventTypeFamily(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(strings.Split(strings.TrimSpace(raw), " ")[0]))
	switch raw {
	case "text", "string":
		return "text"
	case "integer", "int", "bigint":
		return "integer"
	case "numeric", "number", "float", "double", "real":
		return "numeric"
	case "boolean", "bool":
		return "boolean"
	case "timestamp", "timestamptz":
		return "timestamp"
	case "uuid":
		return "uuid"
	default:
		if strings.HasPrefix(raw, "numeric(") && strings.HasSuffix(raw, ")") {
			return "numeric"
		}
		return ""
	}
}
