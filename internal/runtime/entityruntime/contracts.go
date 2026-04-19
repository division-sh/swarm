package entityruntime

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
	runtimesharedjson "swarm/internal/runtime/sharedjson"
)

type Contract struct {
	FlowID     string
	EntityType string
	Entity     runtimecontracts.EntityContract
	Types      runtimecontracts.TypeCatalogDocument
}

type Field struct {
	Path      string
	Type      string
	LeafKind  string
	FieldDecl runtimecontracts.EntityFieldDecl
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
		entityType, entity, ok := bundle.FlowOwnedEntityContract(flowID)
		if ok {
			return Contract{
				FlowID:     flowID,
				EntityType: strings.TrimSpace(entityType),
				Entity:     entity,
				Types:      bundle.ResolvedTypeCatalogForFlow(flowID),
			}, true
		}
		if rootFlowContractApplies(bundle, flowID) {
			for entityType, entity := range bundle.RootEntityContracts() {
				return Contract{
					FlowID:     flowID,
					EntityType: strings.TrimSpace(entityType),
					Entity:     entity,
					Types:      bundle.RootTypeCatalog(),
				}, true
			}
		}
		return Contract{}, false
	}
	root := bundle.RootEntityContracts()
	if len(root) != 1 {
		return Contract{}, false
	}
	for entityType, entity := range root {
		return Contract{
			FlowID:     "",
			EntityType: strings.TrimSpace(entityType),
			Entity:     entity,
			Types:      bundle.RootTypeCatalog(),
		}, true
	}
	return Contract{}, false
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
	if len(bundle.RootEntityContracts()) != 1 {
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
		} else {
			var err error
			value, err = NormalizeFieldValue(contract, name, value)
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
	if strings.Contains(strings.TrimSpace(fieldName), ".") {
		field, err := ResolveLeafField(contract, fieldName)
		if err != nil {
			return nil, err
		}
		return normalizeValueForType(contract, strings.TrimSpace(fieldName), strings.TrimSpace(field.Type), value)
	}
	field, err := FieldDecl(contract, fieldName)
	if err != nil {
		return nil, err
	}
	return normalizeValueForType(contract, strings.TrimSpace(fieldName), strings.TrimSpace(field.Type), value)
}

func ResolveLeafField(contract Contract, path string) (Field, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Field{}, fmt.Errorf("field is required")
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
	if len(segments) == 1 {
		kind, err := leafKind(contract, currentType)
		if err != nil {
			return Field{}, err
		}
		return Field{Path: path, Type: currentType, LeafKind: kind, FieldDecl: decl}, nil
	}
	for _, segment := range segments[1:] {
		named, ok := contract.Types.Types[typeName(contract, currentType)]
		if !ok {
			return Field{}, fmt.Errorf("path %s does not resolve through a named type", path)
		}
		spec, ok := named.Fields[strings.TrimSpace(segment)]
		if !ok {
			return Field{}, fmt.Errorf("undeclared path %s", path)
		}
		currentType = strings.TrimSpace(spec.Type)
	}
	kind, err := leafKind(contract, currentType)
	if err != nil {
		return Field{}, err
	}
	return Field{Path: path, Type: currentType, LeafKind: kind, FieldDecl: decl}, nil
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
			value, err := defaultValue(contract, spec.Type, nil)
			if err != nil {
				return nil, err
			}
			out[strings.TrimSpace(name)] = value
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
				out[key] = normalized
				continue
			}
			defaulted, err := defaultValue(contract, spec.Type, nil)
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
		return out, nil
	default:
		return nil, fieldTypeError(fieldName, "has unsupported type "+typeRef)
	}
}

func leafKind(contract Contract, typeRef string) (string, error) {
	switch {
	case isTextType(typeRef):
		return "scalar", nil
	case isIntegerType(contract, typeRef):
		return "scalar", nil
	case isNumericType(contract, typeRef):
		return "scalar", nil
	case isBooleanType(contract, typeRef):
		return "scalar", nil
	case isTimestampType(contract, typeRef):
		return "scalar", nil
	case isUUIDType(contract, typeRef):
		return "scalar", nil
	case isEnumType(contract, typeRef):
		return "enum", nil
	default:
		return "", fmt.Errorf("path does not resolve to scalar or enum leaf")
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
	return raw == "numeric" || strings.HasPrefix(raw, "numeric(")
}

func isBooleanType(contract Contract, typeRef string) bool {
	return strings.EqualFold(typeName(contract, typeRef), "boolean")
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
