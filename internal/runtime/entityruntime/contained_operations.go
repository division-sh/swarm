package entityruntime

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

const (
	ContainedOperationSet    = "set"
	ContainedOperationMerge  = "merge"
	ContainedOperationDelete = "delete"
	ContainedOperationAppend = "append"
	ContainedOperationUpdate = "update"
)

type ContainedOperationTarget struct {
	Raw          string
	Path         string
	RootField    string
	MapKeyType   string
	MapValueType string
	MapValuePath []string
	TargetType   string
	ListItemType string
	MapScoped    bool
}

func ResolveContainedOperationTarget(contract Contract, target, op string, hasKey, hasIndex bool) (ContainedOperationTarget, error) {
	op = strings.TrimSpace(op)
	target = strings.TrimSpace(target)
	if target == "" {
		return ContainedOperationTarget{}, fmt.Errorf("target is required")
	}
	if strings.Contains(target, "[") || strings.Contains(target, "]") {
		return ContainedOperationTarget{}, fmt.Errorf("dynamic bracket path syntax is not supported for contained operation targets")
	}
	parsed := paths.Parse(target)
	if parsed.Root != paths.RootEntity {
		return ContainedOperationTarget{}, fmt.Errorf("target %q must use entity scope", target)
	}
	if len(parsed.Segments) == 0 {
		return ContainedOperationTarget{}, fmt.Errorf("target field is required")
	}
	rootField := strings.TrimSpace(parsed.Segments[0])
	if rootField == "" {
		return ContainedOperationTarget{}, fmt.Errorf("target field is required")
	}

	if hasKey {
		return resolveMapScopedOperationTarget(contract, target, op, parsed.Segments, hasIndex)
	}
	return resolveListScopedOperationTarget(contract, target, op, parsed.Segments, hasIndex)
}

func NormalizeContainedOperationValue(contract Contract, target ContainedOperationTarget, op string, value any) (any, error) {
	op = strings.TrimSpace(op)
	switch op {
	case ContainedOperationSet:
		return normalizeValueForType(contract, target.Path, target.TargetType, value)
	case ContainedOperationMerge:
		return normalizePartialObjectValue(contract, target.Path, target.TargetType, value)
	case ContainedOperationAppend, ContainedOperationUpdate:
		return normalizeValueForType(contract, target.Path, target.ListItemType, value)
	default:
		return nil, fmt.Errorf("unsupported contained operation %q", op)
	}
}

func NormalizeContainedOperationKey(contract Contract, keyType string, value any) (string, error) {
	keyType = strings.TrimSpace(typeName(contract, keyType))
	switch {
	case isTextType(keyType), isUUIDType(contract, keyType), isEnumType(contract, keyType):
		normalized, err := normalizeValueForType(contract, "", keyType, value)
		if err != nil {
			return "", err
		}
		key := strings.TrimSpace(fmt.Sprint(normalized))
		if key == "" {
			return "", fmt.Errorf("map key cannot be empty")
		}
		return key, nil
	default:
		return "", fmt.Errorf("map key type %s is unsupported; use text, uuid, or enum", keyType)
	}
}

func NormalizeContainedOperationIndex(value any) (int, error) {
	switch typed := value.(type) {
	case int:
		if typed < 0 {
			return 0, fmt.Errorf("list index cannot be negative")
		}
		return typed, nil
	case int64:
		if typed < 0 {
			return 0, fmt.Errorf("list index cannot be negative")
		}
		return int(typed), nil
	case float64:
		if typed < 0 || typed != float64(int(typed)) {
			return 0, fmt.Errorf("list index must be a non-negative integer")
		}
		return int(typed), nil
	case string:
		raw := strings.TrimSpace(typed)
		if raw == "" {
			return 0, fmt.Errorf("list index is required")
		}
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return 0, fmt.Errorf("list index must be a non-negative integer")
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("list index must be a non-negative integer")
	}
}

func resolveMapScopedOperationTarget(contract Contract, rawTarget, op string, segments []string, hasIndex bool) (ContainedOperationTarget, error) {
	rootField := strings.TrimSpace(segments[0])
	rootDecl, err := FieldDecl(contract, rootField)
	if err != nil {
		return ContainedOperationTarget{}, err
	}
	keyType, valueType, ok := MapTypeParts(rootDecl.Type)
	if !ok || strings.TrimSpace(keyType) == "" || strings.TrimSpace(valueType) == "" {
		return ContainedOperationTarget{}, fmt.Errorf("target %s is not a declared map field", rootField)
	}
	out := ContainedOperationTarget{
		Raw:          strings.TrimSpace(rawTarget),
		Path:         strings.Join(segments, "."),
		RootField:    rootField,
		MapKeyType:   keyType,
		MapValueType: valueType,
		MapValuePath: append([]string(nil), segments[1:]...),
		TargetType:   valueType,
		MapScoped:    true,
	}
	if len(out.MapValuePath) > 0 {
		resolvedType, err := resolveTypePath(contract, valueType, out.MapValuePath)
		if err != nil {
			return ContainedOperationTarget{}, err
		}
		out.TargetType = resolvedType
	}
	itemType, err := validateOperationTargetKind(contract, out, op, hasIndex)
	if err != nil {
		return ContainedOperationTarget{}, err
	}
	out.ListItemType = itemType
	return out, nil
}

func resolveListScopedOperationTarget(contract Contract, rawTarget, op string, segments []string, hasIndex bool) (ContainedOperationTarget, error) {
	path := strings.Join(segments, ".")
	field, err := ResolveFieldPath(contract, path)
	if err != nil {
		return ContainedOperationTarget{}, err
	}
	out := ContainedOperationTarget{
		Raw:        strings.TrimSpace(rawTarget),
		Path:       path,
		RootField:  strings.TrimSpace(segments[0]),
		TargetType: field.Type,
	}
	itemType, err := validateOperationTargetKind(contract, out, op, hasIndex)
	if err != nil {
		return ContainedOperationTarget{}, err
	}
	out.ListItemType = itemType
	return out, nil
}

func validateOperationTargetKind(contract Contract, target ContainedOperationTarget, op string, hasIndex bool) (string, error) {
	switch strings.TrimSpace(op) {
	case ContainedOperationSet:
		if !target.MapScoped {
			return "", fmt.Errorf("op set requires a map key")
		}
	case ContainedOperationMerge:
		if !target.MapScoped {
			return "", fmt.Errorf("op merge requires a map key")
		}
		if !isNamedType(contract, target.TargetType) && !isJSONObjectType(contract, target.TargetType) {
			return "", fmt.Errorf("op merge target %s must resolve to an object type", target.Path)
		}
	case ContainedOperationDelete:
		if !target.MapScoped {
			return "", fmt.Errorf("op delete requires a map key")
		}
		if len(target.MapValuePath) > 0 {
			return "", fmt.Errorf("op delete removes map entries only; target %s must be the map field", target.Path)
		}
	case ContainedOperationAppend, ContainedOperationUpdate:
		if !isListType(target.TargetType) && !isJSONArrayType(contract, target.TargetType) {
			return "", fmt.Errorf("op %s target %s must resolve to a list", op, target.Path)
		}
		if strings.TrimSpace(op) == ContainedOperationUpdate && !hasIndex {
			return "", fmt.Errorf("op update requires index")
		}
		_, itemType, err := listElementType(contract, target.TargetType)
		if err != nil {
			return "", err
		}
		return itemType, nil
	default:
		return "", fmt.Errorf("unsupported contained operation %q", op)
	}
	return "", nil
}

func resolveTypePath(contract Contract, typeRef string, segments []string) (string, error) {
	currentType := strings.TrimSpace(typeRef)
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return "", fmt.Errorf("field is required")
		}
		named, ok := contract.Types.Types[typeName(contract, currentType)]
		if !ok {
			return "", fmt.Errorf("path does not resolve through a named type")
		}
		spec, ok := named.Fields[segment]
		if !ok {
			return "", fmt.Errorf("undeclared path segment %s", segment)
		}
		currentType = strings.TrimSpace(spec.Type)
	}
	return currentType, nil
}

func normalizePartialObjectValue(contract Contract, fieldName, typeRef string, value any) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fieldTypeError(fieldName, "must be object")
	}
	if isJSONObjectType(contract, typeRef) {
		return cloneMap(object), nil
	}
	named, ok := contract.Types.Types[typeName(contract, typeRef)]
	if !ok {
		return nil, fieldTypeError(fieldName, "must be object")
	}
	out := make(map[string]any, len(object))
	for key, raw := range object {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		spec, ok := named.Fields[key]
		if !ok {
			return nil, fieldTypeError(joinFieldName(fieldName, key), "is undeclared")
		}
		normalized, err := normalizeValueForType(contract, joinFieldName(fieldName, key), spec.Type, raw)
		if err != nil {
			return nil, err
		}
		out[key] = normalized
	}
	return out, nil
}

func listElementType(contract Contract, typeRef string) (bool, string, error) {
	if isJSONArrayType(contract, typeRef) {
		return true, "json", nil
	}
	if !isListType(typeRef) {
		return false, "", fmt.Errorf("type %s is not a list", typeRef)
	}
	itemType := listItemType(typeRef)
	if strings.TrimSpace(itemType) == "" {
		return false, "", fmt.Errorf("list item type is required")
	}
	return true, itemType, nil
}
