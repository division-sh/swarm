package entityruntime

import (
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
	"github.com/google/uuid"
)

func normalizeMapFieldValue(contract Contract, fieldName, typeRef string, value any) (any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fieldTypeError(fieldName, "must be map")
	}
	keyType, valueType, ok := MapTypeParts(typeRef)
	if !ok {
		return nil, fieldTypeError(fieldName, "has unsupported map type "+typeRef)
	}
	out := make(map[string]any, len(object))
	for key, raw := range object {
		key, err := NormalizeContainedOperationKey(contract, keyType, key)
		if err != nil {
			return nil, fieldTypeError(fieldName, err.Error())
		}
		if _, duplicate := out[key]; duplicate {
			return nil, fieldTypeError(fieldName, fmt.Sprintf("duplicate normalized map key %q", key))
		}
		normalized, err := normalizeValueForType(contract, joinFieldName(fieldName, key), valueType, raw)
		if err != nil {
			return nil, err
		}
		out[key] = normalized
	}
	return out, nil
}

func normalizeListFieldValue(contract Contract, fieldName, typeRef string, value any) (any, error) {
	items, ok := listValues(value)
	if !ok {
		return nil, fieldTypeError(fieldName, "must be list")
	}
	itemType := listItemType(typeRef)
	out := make([]any, 0, len(items))
	for _, item := range items {
		normalized, err := normalizeValueForType(contract, fieldName, itemType, item)
		if err != nil {
			return nil, err
		}
		out = append(out, normalized)
	}
	return out, nil
}

func normalizeTextFieldValue(fieldName string, value any) (any, error) {
	text, ok := value.(string)
	if !ok {
		return nil, fieldTypeError(fieldName, "must be string")
	}
	return text, nil
}

func normalizeIntegerFieldValue(fieldName string, value any) (any, error) {
	if !runtimesharedjson.IsInteger(value) {
		return nil, fieldTypeError(fieldName, "must be integer")
	}
	floatValue, _ := runtimesharedjson.AsFloat64(value)
	return int64(floatValue), nil
}

func normalizeNumericFieldValue(fieldName string, value any) (any, error) {
	normalized, err := canonicaljson.NormalizeRuntimeNumber(value)
	if err != nil {
		return nil, fieldTypeError(fieldName, "must be numeric")
	}
	return normalized, nil
}

func normalizeBooleanFieldValue(fieldName string, value any) (any, error) {
	boolean, ok := value.(bool)
	if !ok {
		return nil, fieldTypeError(fieldName, "must be boolean")
	}
	return boolean, nil
}

func normalizeJSONFieldValue(fieldName string, value any) (any, error) {
	normalized, err := canonicaljson.CloneRuntimeValue(value)
	if err != nil {
		return nil, fieldTypeError(fieldName, err.Error())
	}
	if normalized == nil {
		return nil, fieldTypeError(fieldName, "cannot be null")
	}
	return normalized, nil
}

func normalizeObjectFieldValue(fieldName string, value any) (any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fieldTypeError(fieldName, "must be object")
	}
	return normalizeJSONFieldValue(fieldName, object)
}

func normalizeArrayFieldValue(fieldName string, value any) (any, error) {
	items, ok := listValues(value)
	if !ok {
		return nil, fieldTypeError(fieldName, "must be array")
	}
	return normalizeJSONFieldValue(fieldName, items)
}

func normalizeTimestampFieldValue(fieldName string, value any) (any, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), nil
	case string:
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(typed))
		if err != nil {
			return nil, fieldTypeError(fieldName, "must be RFC3339 timestamp")
		}
		return parsed.UTC(), nil
	default:
		return nil, fieldTypeError(fieldName, "must be timestamp")
	}
}

func normalizeUUIDFieldValue(fieldName string, value any) (any, error) {
	raw := strings.TrimSpace(asString(value))
	if _, err := uuid.Parse(raw); err != nil {
		return nil, fieldTypeError(fieldName, "must be uuid")
	}
	return raw, nil
}

func normalizeEnumFieldValue(contract Contract, fieldName, typeRef string, value any) (any, error) {
	raw, ok := value.(string)
	if !ok {
		return nil, fieldTypeError(fieldName, "must be enum string")
	}
	for _, allowed := range contract.Types.Enums[typeName(contract, typeRef)].Values {
		if strings.TrimSpace(allowed) == strings.TrimSpace(raw) {
			return strings.TrimSpace(raw), nil
		}
	}
	return nil, fieldTypeError(fieldName, "must match declared enum value")
}

func normalizeRecordFieldValue(contract Contract, fieldName, typeRef string, value any) (any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fieldTypeError(fieldName, "must be object")
	}
	resolved, err := resolveStructuralType(contract, typeRef)
	if err != nil {
		return nil, fieldTypeError(fieldName, err.Error())
	}
	out, err := normalizeRecordMembers(contract, fieldName, resolved.Fields, object)
	if err != nil {
		return nil, err
	}
	if err := validateRecordMemberNames(fieldName, resolved, object); err != nil {
		return nil, err
	}
	if err := validateStructuralFieldEqualities(fieldName, resolved.Fields, out); err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeRecordMembers(contract Contract, fieldName string, fields []runtimecontracts.ResolvedCatalogField, object map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(fields))
	for _, field := range fields {
		if raw, ok := object[field.Name]; ok {
			normalized, err := normalizeValueForType(contract, joinFieldName(fieldName, field.Name), field.TypeRef, raw)
			if err != nil {
				return nil, err
			}
			normalized, err = validateValueRefinements(contract, joinFieldName(fieldName, field.Name), field.TypeRef, field.Refinements, normalized)
			if err != nil {
				return nil, err
			}
			out[field.Name] = normalized
			continue
		}
		if field.IsOptional {
			continue
		}
		return nil, fieldTypeError(joinFieldName(fieldName, field.Name), "is required in the supplied record")
	}
	return out, nil
}

func validateRecordMemberNames(fieldName string, resolved runtimecontracts.ResolvedCatalogType, object map[string]any) error {
	for key := range object {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := resolved.Field(key); !ok {
			return fieldTypeError(joinFieldName(fieldName, key), "is undeclared")
		}
	}
	return nil
}
