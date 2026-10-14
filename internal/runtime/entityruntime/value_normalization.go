package entityruntime

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Initialize applies explicit initial values only at entity creation.
func InitialValues(contract Contract) (map[string]any, error) {
	values := map[string]any{}
	for _, name := range FieldNames(contract) {
		if initial := contract.Entity.Fields[name].Initial; initial != nil {
			normalized, err := NormalizeFieldValue(contract, name, initial)
			if err != nil {
				return nil, err
			}
			values[name] = normalized
		}
	}
	return values, nil
}

func Initialize(contract Contract, provided map[string]any) (map[string]any, error) {
	values, err := InitialValues(contract)
	if err != nil {
		return nil, err
	}
	for name, value := range provided {
		values[name] = value
	}
	return NormalizeState(contract, values)
}

// NormalizeState validates a complete snapshot without manufacturing missing
// fields or reapplying creation-time initializers.
func NormalizeState(contract Contract, provided map[string]any) (map[string]any, error) {
	out, err := normalizeAssignedFields(contract, provided)
	if err != nil {
		return nil, err
	}
	if err := validateEntityFieldEqualities("entity", contract.Entity.Fields, out); err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeAssignedFields(contract Contract, provided map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(contract.Entity.Fields))
	for _, name := range FieldNames(contract) {
		name = strings.TrimSpace(name)
		value, ok := provided[name]
		if !ok {
			continue
		}
		var err error
		value, err = NormalizeFieldValue(contract, name, value)
		if err != nil {
			return nil, err
		}
		out[name] = value
	}
	for name := range provided {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := contract.Entity.Fields[name]; !ok {
			return nil, fmt.Errorf("undeclared field %s", name)
		}
	}
	return out, nil
}

func NormalizeMetadataForFlow(source semanticview.Source, flowID string, metadata map[string]any) (map[string]any, error) {
	contract, ok := ResolveForFlow(source, flowID)
	if !ok {
		return cloneMap(metadata), nil
	}
	input := map[string]any{}
	for name := range contract.Entity.Fields {
		if value, exists := metadata[name]; exists {
			input[name] = cloneValue(value)
		}
	}
	materialized, err := NormalizeState(contract, input)
	if err != nil {
		return nil, err
	}
	out := cloneMap(metadata)
	if out == nil {
		out = map[string]any{}
	}
	for key, value := range materialized {
		out[key] = value
	}
	return out, nil
}

func NormalizeFieldValue(contract Contract, fieldName string, value any) (any, error) {
	field, err := ResolveFieldPath(contract, fieldName)
	if err != nil {
		return nil, err
	}
	normalized, err := normalizeValueForType(contract, strings.TrimSpace(field.Path), strings.TrimSpace(field.Type), value)
	if err != nil {
		return nil, err
	}
	return validateValueRefinements(contract, strings.TrimSpace(field.Path), strings.TrimSpace(field.Type), field.Refinements, normalized)
}

func normalizeValueForType(contract Contract, fieldName, typeRef string, value any) (any, error) {
	typeRef = strings.TrimSpace(typeRef)
	if value == nil {
		return nil, fieldTypeError(fieldName, "cannot be null")
	}
	raw := reflect.ValueOf(value)
	if (raw.Kind() == reflect.Map || raw.Kind() == reflect.Slice || raw.Kind() == reflect.Pointer) && raw.IsNil() {
		return nil, fieldTypeError(fieldName, "cannot be null")
	}
	switch {
	case isMapType(typeRef):
		return normalizeMapFieldValue(contract, fieldName, typeRef, value)
	case isListType(typeRef):
		return normalizeListFieldValue(contract, fieldName, typeRef, value)
	case isTextType(typeName(contract, typeRef)):
		return normalizeTextFieldValue(fieldName, value)
	case isIntegerType(contract, typeRef):
		return normalizeIntegerFieldValue(fieldName, value)
	case isNumericType(contract, typeRef):
		return normalizeNumericFieldValue(fieldName, value)
	case isBooleanType(contract, typeRef):
		return normalizeBooleanFieldValue(fieldName, value)
	case isJSONValueType(contract, typeRef):
		return normalizeJSONFieldValue(fieldName, value)
	case isJSONObjectType(contract, typeRef):
		return normalizeObjectFieldValue(fieldName, value)
	case isJSONArrayType(contract, typeRef):
		return normalizeArrayFieldValue(fieldName, value)
	case isTimestampType(contract, typeRef):
		return normalizeTimestampFieldValue(fieldName, value)
	case isUUIDType(contract, typeRef):
		return normalizeUUIDFieldValue(fieldName, value)
	case isEnumType(contract, typeRef):
		return normalizeEnumFieldValue(contract, fieldName, typeRef, value)
	case isNamedType(contract, typeRef):
		return normalizeRecordFieldValue(contract, fieldName, typeRef, value)
	default:
		return nil, fieldTypeError(fieldName, "has unsupported type "+typeRef)
	}
}
