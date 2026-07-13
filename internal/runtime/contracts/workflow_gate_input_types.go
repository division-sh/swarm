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

// ValidateCanonicalWorkflowGateInputType rejects programmatic and persisted
// spellings that would otherwise hash differently from the authored canonical
// value. YAML decoding normalizes before the semantic plan is constructed.
func ValidateCanonicalWorkflowGateInputType(raw string) (string, error) {
	canonical, err := NormalizeWorkflowGateInputType(raw)
	if err != nil {
		return "", err
	}
	if raw != canonical {
		return "", fmt.Errorf("stage gate input type %q is not canonical; use %q", raw, canonical)
	}
	return canonical, nil
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

// WorkflowGateInputTypeCompatibleWithResolvedSchema compares a gate input to
// the exact resolved event-field schema. This preserves timestamp/UUID format
// identity and supports named scalar/enum fields after catalog lowering.
func WorkflowGateInputTypeCompatibleWithResolvedSchema(inputType string, schema map[string]any) bool {
	inputType, err := NormalizeWorkflowGateInputType(inputType)
	if err != nil || schema == nil {
		return false
	}
	schemaType, _ := schema["type"].(string)
	format, _ := schema["format"].(string)
	schemaType = strings.ToLower(strings.TrimSpace(schemaType))
	format = strings.ToLower(strings.TrimSpace(format))
	switch inputType {
	case "text":
		return schemaType == "string" && format == ""
	case "integer":
		return schemaType == "integer"
	case "numeric":
		return schemaType == "number"
	case "boolean":
		return schemaType == "boolean"
	case "timestamp":
		return schemaType == "string" && format == "date-time"
	case "uuid":
		return schemaType == "string" && format == "uuid"
	default:
		return false
	}
}
