package entityruntime

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
	"github.com/google/uuid"
)

type Contract struct {
	FlowID     string
	EntityType string
	Entity     runtimecontracts.EntityContract
	Types      runtimecontracts.TypeCatalogDocument
}

type Field struct {
	Path        string
	Type        string
	LeafKind    string
	FieldDecl   runtimecontracts.EntityFieldDecl
	Refinements runtimecontracts.SchemaRefinements
}

type WriteTarget struct {
	Raw       string
	Path      string
	RootField string
	Field     Field
	Nested    bool
}

func ResolveForActor(source semanticview.Source, actorID string) (Contract, bool) {
	actorID = strings.TrimSpace(actorID)
	if source == nil || actorID == "" {
		return Contract{}, false
	}
	contractSource, ok := source.AgentContractSource(actorID)
	if !ok {
		return Contract{}, false
	}
	return ResolveForFlow(source, contractSource.FlowID)
}

func ResolveForReadTarget(source semanticview.Source, actorID, target string) (Contract, bool, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		contract, ok := ResolveForActor(source, actorID)
		return contract, ok, nil
	}
	if source == nil {
		return Contract{}, false, fmt.Errorf("workflow source is not configured")
	}
	if idx := strings.LastIndex(target, "."); idx > 0 && idx < len(target)-1 {
		flowID := strings.TrimSpace(target[:idx])
		entityType := strings.TrimSpace(target[idx+1:])
		contract, ok := ResolveForFlow(source, flowID)
		if !ok {
			return Contract{}, false, fmt.Errorf("entity_type %q does not resolve to a flow-owned entity contract", target)
		}
		if !strings.EqualFold(contract.EntityType, entityType) {
			return Contract{}, false, fmt.Errorf("entity_type %q resolves to flow %q, which owns %q", target, flowID, contract.EntityType)
		}
		return contract, true, nil
	}

	matches := make([]Contract, 0, 2)
	for _, contract := range ReadTargetContracts(source) {
		if strings.EqualFold(contract.EntityType, target) {
			matches = append(matches, contract)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], true, nil
	case 0:
		return Contract{}, false, fmt.Errorf("entity_type %q does not resolve to a flow-owned entity contract", target)
	default:
		names := make([]string, 0, len(matches))
		for _, contract := range matches {
			names = append(names, CanonicalReadTargetName(contract))
		}
		sort.Strings(names)
		return Contract{}, false, fmt.Errorf("entity_type %q is ambiguous; use one of: %s", target, strings.Join(names, ", "))
	}
}

func ReadTargetContracts(source semanticview.Source) []Contract {
	if source == nil {
		return nil
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil
	}
	flowIDs := make([]string, 0, len(source.FlowSchemaEntries()))
	for flowID := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID != "" {
			flowIDs = append(flowIDs, flowID)
		}
	}
	sort.Strings(flowIDs)
	out := make([]Contract, 0, len(flowIDs)+1)
	for _, flowID := range flowIDs {
		if contract, ok := ResolveForFlow(source, flowID); ok {
			out = append(out, contract)
		}
	}
	if _, _, ok := bundle.RootPrimaryEntityContract(); ok {
		if contract, ok := ResolveForFlow(source, ""); ok {
			out = append(out, contract)
		}
	}
	return out
}

func ReadTargetNames(source semanticview.Source) []string {
	contracts := ReadTargetContracts(source)
	names := make([]string, 0, len(contracts))
	for _, contract := range contracts {
		name := CanonicalReadTargetName(contract)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func CanonicalReadTargetName(contract Contract) string {
	entityType := strings.TrimSpace(contract.EntityType)
	flowID := strings.TrimSpace(contract.FlowID)
	if entityType == "" {
		return ""
	}
	if flowID == "" {
		return entityType
	}
	return flowID + "." + entityType
}

func ResolveForFlowInstance(source semanticview.Source, flowInstance string) (Contract, bool) {
	return ResolveForFlow(source, ResolveFlowIDForInstance(source, flowInstance))
}

func ResolveForEntityRow(source semanticview.Source, row map[string]any) (Contract, bool) {
	if len(row) == 0 {
		return Contract{}, false
	}
	return ResolveForFlowInstance(source, strings.TrimSpace(asString(row["flow_instance"])))
}

func ResolveForFlow(source semanticview.Source, flowID string) (Contract, bool) {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return Contract{}, false
	}
	flowID = strings.TrimSpace(flowID)
	if flowID != "" {
		entityType, entity, ok := bundle.FlowPrimaryEntityContract(flowID)
		if ok {
			return Contract{
				FlowID:     flowID,
				EntityType: strings.TrimSpace(entityType),
				Entity:     entity,
				Types:      bundle.ResolvedTypeCatalogForFlow(flowID),
			}, true
		}
		if rootFlowContractApplies(bundle, flowID) {
			entityType, entity, ok := bundle.RootPrimaryEntityContract()
			return Contract{
				FlowID:     flowID,
				EntityType: strings.TrimSpace(entityType),
				Entity:     entity,
				Types:      bundle.RootTypeCatalog(),
			}, ok
		}
		return Contract{}, false
	}
	entityType, entity, ok := bundle.RootPrimaryEntityContract()
	return Contract{
		FlowID:     "",
		EntityType: strings.TrimSpace(entityType),
		Entity:     entity,
		Types:      bundle.RootTypeCatalog(),
	}, ok
}

func ResolveFlowIDForInstance(source semanticview.Source, flowInstance string) string {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" || source == nil {
		return flowInstance
	}
	bundle, _ := semanticview.Bundle(source)
	if _, ok := source.FlowScopeByID(flowInstance); ok {
		return flowInstance
	}
	bestFlowID := ""
	bestMatchLen := -1
	for flowID := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		if flowID == flowInstance {
			if len(flowID) > bestMatchLen {
				bestFlowID = flowID
				bestMatchLen = len(flowID)
			}
		}
		flowPath := strings.Trim(source.FlowPath(flowID), "/")
		if flowPath == flowInstance {
			if len(flowPath) > bestMatchLen {
				bestFlowID = flowID
				bestMatchLen = len(flowPath)
			}
		}
		if flowPath != "" && strings.HasPrefix(flowInstance, flowPath+"/") {
			if len(flowPath) > bestMatchLen {
				bestFlowID = flowID
				bestMatchLen = len(flowPath)
			}
		}
	}
	if bestFlowID != "" {
		return bestFlowID
	}
	if head, _, ok := strings.Cut(flowInstance, "/"); ok {
		head = strings.TrimSpace(head)
		if _, exists := source.FlowSchemaEntries()[head]; exists {
			return head
		}
		if rootFlowContractApplies(bundle, head) {
			return head
		}
	}
	if rootFlowContractApplies(bundle, flowInstance) {
		return flowInstance
	}
	return flowInstance
}

func rootFlowContractApplies(bundle *runtimecontracts.WorkflowContractBundle, flowID string) bool {
	flowID = strings.TrimSpace(flowID)
	if bundle == nil || flowID == "" {
		return false
	}
	_, _, ok := bundle.RootPrimaryEntityContract()
	if !ok {
		return false
	}
	rootName := strings.TrimSpace(bundle.WorkflowName())
	if rootName == "" {
		return false
	}
	return flowID == rootName
}

func FieldNames(contract Contract) []string {
	if len(contract.Entity.Fields) == 0 {
		return nil
	}
	names := make([]string, 0, len(contract.Entity.Fields))
	for name := range contract.Entity.Fields {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func DeclaredValues(contract Contract, provided map[string]any) map[string]any {
	if len(provided) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(contract.Entity.Fields))
	for name := range contract.Entity.Fields {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if value, ok := provided[name]; ok {
			out[name] = cloneValue(value)
		}
	}
	return out
}

func FieldDecl(contract Contract, name string) (runtimecontracts.EntityFieldDecl, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return runtimecontracts.EntityFieldDecl{}, fmt.Errorf("field is required")
	}
	field, ok := contract.Entity.Fields[name]
	if !ok {
		return runtimecontracts.EntityFieldDecl{}, fmt.Errorf("undeclared field %s", name)
	}
	return field, nil
}

func Materialize(contract Contract, provided map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(contract.Entity.Fields))
	for name, decl := range contract.Entity.Fields {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		value, ok := provided[name]
		if !ok {
			var err error
			value, err = defaultValue(contract, decl.Type, decl.Initial)
			if err != nil {
				return nil, fmt.Errorf("materialize %s: %w", name, err)
			}
			value, err = validateValueRefinements(contract, name, decl.Type, decl.Refinements, value)
			if err != nil {
				return nil, err
			}
		} else {
			var err error
			value, err = normalizeFieldValue(contract, name, value, true)
			if err != nil {
				return nil, err
			}
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
	if err := validateEntityFieldEqualities("entity", contract.Entity.Fields, out); err != nil {
		return nil, err
	}
	return out, nil
}

func MaterializeMetadataForFlow(source semanticview.Source, flowID string, metadata map[string]any) map[string]any {
	contract, ok := ResolveForFlow(source, flowID)
	if !ok {
		return cloneMap(metadata)
	}
	input := map[string]any{}
	for name := range contract.Entity.Fields {
		if value, exists := metadata[name]; exists {
			input[name] = cloneValue(value)
		}
	}
	materialized, err := Materialize(contract, input)
	if err != nil {
		return cloneMap(metadata)
	}
	out := cloneMap(metadata)
	if out == nil {
		out = map[string]any{}
	}
	for key, value := range materialized {
		out[key] = value
	}
	return out
}

func NormalizeFieldValue(contract Contract, fieldName string, value any) (any, error) {
	return normalizeFieldValue(contract, fieldName, value, false)
}

func normalizeFieldValue(contract Contract, fieldName string, value any, allowEqualityParticipant bool) (any, error) {
	field, err := ResolveFieldPath(contract, fieldName)
	if err != nil {
		return nil, err
	}
	if !allowEqualityParticipant {
		if fieldPathParticipatesInEquality(contract, strings.TrimSpace(field.Path)) {
			return nil, fieldTypeError(strings.TrimSpace(field.Path), "cannot be written in isolation because it participates in equal_to")
		}
	}
	normalized, err := normalizeValueForType(contract, strings.TrimSpace(field.Path), strings.TrimSpace(field.Type), value)
	if err != nil {
		return nil, err
	}
	return validateValueRefinements(contract, strings.TrimSpace(field.Path), strings.TrimSpace(field.Type), field.Refinements, normalized)
}

func EntityWritePath(target string) (string, bool, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", false, fmt.Errorf("field is required")
	}
	parsed := paths.Parse(target)
	if parsed.IsZero() {
		return "", false, fmt.Errorf("field is required")
	}
	if parsed.HasExplicitRoot() {
		switch parsed.Root {
		case paths.RootEntity:
			if len(parsed.Segments) == 0 {
				return "", true, fmt.Errorf("field is required")
			}
			return strings.Join(parsed.Segments, "."), true, nil
		case paths.RootMetadata:
			return "", false, nil
		case paths.RootPlatformEntity:
			return "", false, fmt.Errorf("%s is read-only platform entity metadata and cannot be used as a write target", paths.RootPlatformEntity.String())
		default:
			return "", false, nil
		}
	}
	if len(parsed.Segments) == 0 {
		return "", false, fmt.Errorf("field is required")
	}
	return strings.Join(parsed.Segments, "."), true, nil
}

func ResolveEntityWriteTarget(contract Contract, target string) (WriteTarget, bool, error) {
	path, entityTarget, err := EntityWritePath(target)
	if err != nil || !entityTarget {
		return WriteTarget{Raw: strings.TrimSpace(target)}, entityTarget, err
	}
	field, err := ResolveFieldPath(contract, path)
	if err != nil {
		return WriteTarget{Raw: strings.TrimSpace(target), Path: path}, true, err
	}
	rootField, _, _ := strings.Cut(path, ".")
	return WriteTarget{
		Raw:       strings.TrimSpace(target),
		Path:      path,
		RootField: strings.TrimSpace(rootField),
		Field:     field,
		Nested:    strings.Contains(path, "."),
	}, true, nil
}

func ResolveFieldPath(contract Contract, path string) (Field, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Field{}, fmt.Errorf("field is required")
	}
	if strings.Contains(path, "[") || strings.Contains(path, "]") {
		return Field{}, fmt.Errorf("list index writes are not supported for path %s", path)
	}
	segments := strings.Split(path, ".")
	if len(segments) == 0 {
		return Field{}, fmt.Errorf("field is required")
	}
	name := strings.TrimSpace(segments[0])
	decl, err := FieldDecl(contract, name)
	if err != nil {
		return Field{}, err
	}
	currentType := strings.TrimSpace(decl.Type)
	currentRefinements := decl.Refinements
	for _, segment := range segments[1:] {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return Field{}, fmt.Errorf("field is required")
		}
		named, ok := contract.Types.Types[typeName(contract, currentType)]
		if !ok {
			return Field{}, fmt.Errorf("path %s does not resolve through a named type", path)
		}
		spec, ok := named.Fields[segment]
		if !ok {
			return Field{}, fmt.Errorf("undeclared path %s", path)
		}
		currentType = strings.TrimSpace(spec.Type)
		currentRefinements = spec.Refinements
	}
	kind := pathKind(contract, currentType)
	return Field{Path: path, Type: currentType, LeafKind: kind, FieldDecl: decl, Refinements: currentRefinements}, nil
}

func ResolveLeafField(contract Contract, path string) (Field, error) {
	field, err := ResolveFieldPath(contract, path)
	if err != nil {
		return Field{}, err
	}
	if field.LeafKind != "scalar" && field.LeafKind != "enum" {
		return Field{}, fmt.Errorf("path does not resolve to scalar or enum leaf")
	}
	return field, nil
}

func PathValue(value map[string]any, path string) (any, bool) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, false
	}
	current := any(value)
	for _, segment := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[strings.TrimSpace(segment)]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func defaultValue(contract Contract, typeRef string, explicit any) (any, error) {
	if explicit != nil {
		return normalizeValueForType(contract, "", typeRef, explicit)
	}
	typeRef = strings.TrimSpace(typeRef)
	switch {
	case isMapType(typeRef):
		return map[string]any{}, nil
	case isListType(typeRef):
		return []any{}, nil
	case isTextType(typeRef):
		return "", nil
	case isIntegerType(contract, typeRef):
		return int64(0), nil
	case isNumericType(contract, typeRef):
		return float64(0), nil
	case isBooleanType(contract, typeRef):
		return false, nil
	case isTimestampType(contract, typeRef), isUUIDType(contract, typeRef):
		return nil, nil
	case isJSONObjectType(contract, typeRef):
		return map[string]any{}, nil
	case isJSONArrayType(contract, typeRef):
		return []any{}, nil
	case isEnumType(contract, typeRef):
		enum := contract.Types.Enums[typeName(contract, typeRef)]
		if len(enum.Values) == 0 {
			return "", nil
		}
		return strings.TrimSpace(enum.Values[0]), nil
	case isNamedType(contract, typeRef):
		named := contract.Types.Types[typeName(contract, typeRef)]
		out := make(map[string]any, len(named.Fields))
		for name, spec := range named.Fields {
			name = strings.TrimSpace(name)
			value, err := defaultValue(contract, spec.Type, nil)
			if err != nil {
				return nil, err
			}
			value, err = validateValueRefinements(contract, name, spec.Type, spec.Refinements, value)
			if err != nil {
				return nil, err
			}
			out[name] = value
		}
		if err := validateTypeFieldEqualities(typeName(contract, typeRef), named.Fields, out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported contract type %s", typeRef)
	}
}

func normalizeValueForType(contract Contract, fieldName, typeRef string, value any) (any, error) {
	typeRef = strings.TrimSpace(typeRef)
	if value == nil {
		switch {
		case isTimestampType(contract, typeRef), isUUIDType(contract, typeRef):
			return nil, nil
		default:
			if fieldName == "" {
				return nil, fmt.Errorf("value for type %s cannot be null", typeRef)
			}
			return nil, fmt.Errorf("field %s cannot be null", fieldName)
		}
	}
	switch {
	case isMapType(typeRef):
		object, ok := value.(map[string]any)
		if !ok {
			return nil, fieldTypeError(fieldName, "must be map")
		}
		_, valueType, ok := MapTypeParts(typeRef)
		if !ok {
			return nil, fieldTypeError(fieldName, "has unsupported map type "+typeRef)
		}
		out := make(map[string]any, len(object))
		for key, raw := range object {
			key = strings.TrimSpace(key)
			if key == "" {
				return nil, fieldTypeError(fieldName, "map key cannot be empty")
			}
			normalized, err := normalizeValueForType(contract, joinFieldName(fieldName, key), valueType, raw)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	case isListType(typeRef):
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
	case isTextType(typeRef):
		text, ok := value.(string)
		if !ok {
			return nil, fieldTypeError(fieldName, "must be string")
		}
		return text, nil
	case isIntegerType(contract, typeRef):
		if !runtimesharedjson.IsInteger(value) {
			return nil, fieldTypeError(fieldName, "must be integer")
		}
		floatValue, _ := runtimesharedjson.AsFloat64(value)
		return int64(floatValue), nil
	case isNumericType(contract, typeRef):
		if !runtimesharedjson.IsNumeric(value) {
			return nil, fieldTypeError(fieldName, "must be numeric")
		}
		floatValue, _ := runtimesharedjson.AsFloat64(value)
		return floatValue, nil
	case isBooleanType(contract, typeRef):
		boolean, ok := value.(bool)
		if !ok {
			return nil, fieldTypeError(fieldName, "must be boolean")
		}
		return boolean, nil
	case isJSONObjectType(contract, typeRef):
		object, ok := value.(map[string]any)
		if !ok {
			return nil, fieldTypeError(fieldName, "must be object")
		}
		return cloneMap(object), nil
	case isJSONArrayType(contract, typeRef):
		items, ok := listValues(value)
		if !ok {
			return nil, fieldTypeError(fieldName, "must be array")
		}
		return cloneValue(items), nil
	case isTimestampType(contract, typeRef):
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
	case isUUIDType(contract, typeRef):
		raw := strings.TrimSpace(asString(value))
		if _, err := uuid.Parse(raw); err != nil {
			return nil, fieldTypeError(fieldName, "must be uuid")
		}
		return raw, nil
	case isEnumType(contract, typeRef):
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
	case isNamedType(contract, typeRef):
		object, ok := value.(map[string]any)
		if !ok {
			return nil, fieldTypeError(fieldName, "must be object")
		}
		named := contract.Types.Types[typeName(contract, typeRef)]
		out := make(map[string]any, len(named.Fields))
		for key, spec := range named.Fields {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if raw, ok := object[key]; ok {
				normalized, err := normalizeValueForType(contract, joinFieldName(fieldName, key), spec.Type, raw)
				if err != nil {
					return nil, err
				}
				normalized, err = validateValueRefinements(contract, joinFieldName(fieldName, key), spec.Type, spec.Refinements, normalized)
				if err != nil {
					return nil, err
				}
				out[key] = normalized
				continue
			}
			defaulted, err := defaultValue(contract, spec.Type, nil)
			if err != nil {
				return nil, err
			}
			defaulted, err = validateValueRefinements(contract, joinFieldName(fieldName, key), spec.Type, spec.Refinements, defaulted)
			if err != nil {
				return nil, err
			}
			out[key] = defaulted
		}
		for key := range object {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if _, ok := named.Fields[key]; !ok {
				return nil, fieldTypeError(joinFieldName(fieldName, key), "is undeclared")
			}
		}
		if err := validateTypeFieldEqualities(fieldName, named.Fields, out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fieldTypeError(fieldName, "has unsupported type "+typeRef)
	}
}

func validateValueRefinements(contract Contract, fieldName, typeRef string, refinements runtimecontracts.SchemaRefinements, value any) (any, error) {
	if refinements.Empty() {
		return value, nil
	}
	if pattern := strings.TrimSpace(refinements.Pattern); pattern != "" {
		text, ok := value.(string)
		if !ok {
			return nil, fieldTypeError(fieldName, "must be string for pattern refinement")
		}
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fieldTypeError(fieldName, "has invalid pattern refinement")
		}
		if !compiled.MatchString(text) {
			return nil, fieldTypeError(fieldName, "must match pattern")
		}
	}
	if !refinements.Length.Empty() {
		length, ok := refinementLength(value)
		if !ok {
			return nil, fieldTypeError(fieldName, "must be string or list for length refinement")
		}
		if min := refinements.Length.Min; min != nil && length < *min {
			return nil, fieldTypeError(fieldName, fmt.Sprintf("length must be >= %d", *min))
		}
		if max := refinements.Length.Max; max != nil && length > *max {
			return nil, fieldTypeError(fieldName, fmt.Sprintf("length must be <= %d", *max))
		}
	}
	if !refinements.Range.Empty() {
		if !isIntegerType(contract, typeRef) && !isNumericType(contract, typeRef) {
			return nil, fieldTypeError(fieldName, "must be numeric for range refinement")
		}
		number, ok := runtimesharedjson.AsFloat64(value)
		if !ok {
			return nil, fieldTypeError(fieldName, "must be numeric for range refinement")
		}
		if min := refinements.Range.Min; min != nil && number < *min {
			return nil, fieldTypeError(fieldName, fmt.Sprintf("must be >= %v", *min))
		}
		if max := refinements.Range.Max; max != nil && number > *max {
			return nil, fieldTypeError(fieldName, fmt.Sprintf("must be <= %v", *max))
		}
	}
	return value, nil
}

func refinementLength(value any) (int, bool) {
	switch typed := value.(type) {
	case string:
		return utf8.RuneCountInString(typed), true
	default:
		items, ok := listValues(value)
		if !ok {
			return 0, false
		}
		return len(items), true
	}
}

func validateEntityFieldEqualities(context string, fields map[string]runtimecontracts.EntityFieldDecl, values map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	for name, field := range fields {
		name = strings.TrimSpace(name)
		target := strings.TrimSpace(field.Refinements.EqualTo)
		if name == "" || target == "" {
			continue
		}
		if err := validateEqualityValue(context, name, target, values); err != nil {
			return err
		}
	}
	return nil
}

func validateTypeFieldEqualities(context string, fields map[string]runtimecontracts.TypeFieldSpec, values map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	for name, field := range fields {
		name = strings.TrimSpace(name)
		target := strings.TrimSpace(field.Refinements.EqualTo)
		if name == "" || target == "" {
			continue
		}
		if err := validateEqualityValue(context, name, target, values); err != nil {
			return err
		}
	}
	return nil
}

func validateEqualityValue(context, name, target string, values map[string]any) error {
	left, ok := values[name]
	if !ok {
		return fieldTypeError(joinFieldName(context, name), "must equal "+joinFieldName(context, target)+" but source is missing")
	}
	right, ok := values[target]
	if !ok {
		return fieldTypeError(joinFieldName(context, name), "must equal "+joinFieldName(context, target)+" but target is missing")
	}
	if !reflect.DeepEqual(left, right) {
		return fieldTypeError(joinFieldName(context, name), "must equal "+joinFieldName(context, target))
	}
	return nil
}

func fieldPathParticipatesInEquality(contract Contract, path string) bool {
	path = strings.TrimSpace(path)
	if path == "" {
		return false
	}
	segments := strings.Split(path, ".")
	if len(segments) == 0 {
		return false
	}
	if len(segments) == 1 {
		return entityFieldParticipatesInEquality(contract.Entity.Fields, strings.TrimSpace(segments[0]))
	}
	currentType := ""
	root := strings.TrimSpace(segments[0])
	if decl, ok := contract.Entity.Fields[root]; ok {
		currentType = strings.TrimSpace(decl.Type)
	} else {
		return false
	}
	for _, segment := range segments[1 : len(segments)-1] {
		named, ok := contract.Types.Types[typeName(contract, currentType)]
		if !ok {
			return false
		}
		spec, ok := named.Fields[strings.TrimSpace(segment)]
		if !ok {
			return false
		}
		currentType = strings.TrimSpace(spec.Type)
	}
	named, ok := contract.Types.Types[typeName(contract, currentType)]
	if !ok {
		return false
	}
	return typeFieldParticipatesInEquality(named.Fields, strings.TrimSpace(segments[len(segments)-1]))
}

func entityFieldParticipatesInEquality(fields map[string]runtimecontracts.EntityFieldDecl, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for fieldName, field := range fields {
		fieldName = strings.TrimSpace(fieldName)
		target := strings.TrimSpace(field.Refinements.EqualTo)
		if fieldName == name && target != "" {
			return true
		}
		if target == name {
			return true
		}
	}
	return false
}

func typeFieldParticipatesInEquality(fields map[string]runtimecontracts.TypeFieldSpec, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for fieldName, field := range fields {
		fieldName = strings.TrimSpace(fieldName)
		target := strings.TrimSpace(field.Refinements.EqualTo)
		if fieldName == name && target != "" {
			return true
		}
		if target == name {
			return true
		}
	}
	return false
}

func leafKind(contract Contract, typeRef string) (string, error) {
	switch kind := pathKind(contract, typeRef); kind {
	case "scalar", "enum":
		return kind, nil
	default:
		return "", fmt.Errorf("path does not resolve to scalar or enum leaf")
	}
}

func pathKind(contract Contract, typeRef string) string {
	switch {
	case isTextType(typeRef):
		return "scalar"
	case isIntegerType(contract, typeRef):
		return "scalar"
	case isNumericType(contract, typeRef):
		return "scalar"
	case isBooleanType(contract, typeRef):
		return "scalar"
	case isJSONObjectType(contract, typeRef):
		return "object"
	case isJSONArrayType(contract, typeRef):
		return "list"
	case isTimestampType(contract, typeRef):
		return "scalar"
	case isUUIDType(contract, typeRef):
		return "scalar"
	case isEnumType(contract, typeRef):
		return "enum"
	case isNamedType(contract, typeRef):
		return "object"
	case isMapType(typeRef):
		return "map"
	case isListType(typeRef):
		return "list"
	default:
		return ""
	}
}

func fieldTypeError(fieldName, detail string) error {
	fieldName = strings.TrimSpace(fieldName)
	if fieldName == "" {
		return fmt.Errorf("%s", detail)
	}
	return fmt.Errorf("field %s %s", fieldName, detail)
}

func joinFieldName(parent, child string) string {
	parent = strings.TrimSpace(parent)
	child = strings.TrimSpace(child)
	if parent == "" {
		return child
	}
	if child == "" {
		return parent
	}
	return parent + "." + child
}

func typeName(contract Contract, typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	if scalar, ok := contract.Types.Scalars[typeRef]; ok {
		return strings.TrimSpace(scalar.Base)
	}
	return typeRef
}

func isNamedType(contract Contract, typeRef string) bool {
	_, ok := contract.Types.Types[typeName(contract, typeRef)]
	return ok
}

func isEnumType(contract Contract, typeRef string) bool {
	_, ok := contract.Types.Enums[typeName(contract, typeRef)]
	return ok
}

func isTextType(typeRef string) bool {
	typeRef = strings.ToLower(strings.TrimSpace(typeRef))
	return typeRef == "text" || typeRef == "string"
}

func isIntegerType(contract Contract, typeRef string) bool {
	return strings.EqualFold(typeName(contract, typeRef), "integer")
}

func isNumericType(contract Contract, typeRef string) bool {
	raw := strings.ToLower(strings.TrimSpace(typeName(contract, typeRef)))
	return raw == "numeric" || raw == "number" || raw == "float" || raw == "double" || raw == "real" || strings.HasPrefix(raw, "numeric(")
}

func isBooleanType(contract Contract, typeRef string) bool {
	return strings.EqualFold(typeName(contract, typeRef), "boolean")
}

func isJSONObjectType(contract Contract, typeRef string) bool {
	raw := strings.ToLower(strings.TrimSpace(typeName(contract, typeRef)))
	return raw == "json" || raw == "object"
}

func isJSONArrayType(contract Contract, typeRef string) bool {
	return strings.EqualFold(typeName(contract, typeRef), "array")
}

func isMapType(typeRef string) bool {
	_, _, ok := MapTypeParts(typeRef)
	return ok
}

func MapTypeParts(typeRef string) (string, string, bool) {
	typeRef = strings.TrimSpace(typeRef)
	if !strings.HasPrefix(strings.ToLower(typeRef), "map[") {
		return "", "", false
	}
	closeIdx := strings.Index(typeRef, "]")
	if closeIdx <= len("map[") {
		return "", "", false
	}
	keyType := strings.TrimSpace(typeRef[len("map["):closeIdx])
	valueType := strings.TrimSpace(typeRef[closeIdx+1:])
	if keyType == "" || valueType == "" {
		return "", "", false
	}
	return keyType, valueType, true
}

func isTimestampType(contract Contract, typeRef string) bool {
	return strings.EqualFold(typeName(contract, typeRef), "timestamp")
}

func isUUIDType(contract Contract, typeRef string) bool {
	return strings.EqualFold(typeName(contract, typeRef), "uuid")
}

func isListType(typeRef string) bool {
	typeRef = strings.TrimSpace(typeRef)
	return strings.HasPrefix(typeRef, "list<") && strings.HasSuffix(typeRef, ">") ||
		strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]") ||
		strings.HasSuffix(typeRef, "[]") ||
		strings.HasPrefix(typeRef, "[]")
}

func listItemType(typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	switch {
	case strings.HasPrefix(typeRef, "list<") && strings.HasSuffix(typeRef, ">"):
		return strings.TrimSpace(typeRef[len("list<") : len(typeRef)-1])
	case strings.HasSuffix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[:len(typeRef)-2])
	case strings.HasPrefix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[2:])
	case strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]"):
		return strings.TrimSpace(typeRef[1 : len(typeRef)-1])
	default:
		return typeRef
	}
}

func listValues(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}
	if typed, ok := value.([]any); ok {
		return typed, true
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, 0, reflected.Len())
	for i := 0; i < reflected.Len(); i++ {
		out = append(out, reflected.Index(i).Interface())
	}
	return out, true
}

func asString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return ""
	}
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, cloneValue(item))
		}
		return out
	default:
		return typed
	}
}
