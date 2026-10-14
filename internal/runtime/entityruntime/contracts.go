package entityruntime

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
	IsOptional  bool
}

type WriteTarget struct {
	Raw       string
	Path      string
	RootField string
	Field     Field
	Nested    bool
}

func ResolveForActor(source semanticview.Source, actor models.AgentConfig) (Contract, bool) {
	if source == nil || strings.TrimSpace(actor.ID) == "" {
		return Contract{}, false
	}
	projection, ok := semanticview.ResolveAgentContractProjection(source, actor)
	if !ok {
		return Contract{}, false
	}
	return ResolveForFlow(source, projection.OwnerFlowID)
}

func ResolveForReadTarget(source semanticview.Source, actor models.AgentConfig, target string) (Contract, bool, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		contract, ok := ResolveForActor(source, actor)
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

// ResolveFlowIDForRuntimeInstance binds the root run instance through its
// admitted source rather than treating an arbitrary UUID as a flow name.
func ResolveFlowIDForRuntimeInstance(source semanticview.Source, runID, flowInstance string) string {
	runID = strings.TrimSpace(runID)
	flowInstance = strings.TrimSpace(flowInstance)
	if runID != "" && flowInstance == runID {
		if root, err := semanticview.AdmitRootExecutionCoordinate(source, runID); err == nil {
			return root.FlowID()
		}
	}
	return ResolveFlowIDForInstance(source, flowInstance)
}

func ResolveForRuntimeInstance(source semanticview.Source, runID, flowInstance string) (Contract, bool) {
	return ResolveForFlow(source, ResolveFlowIDForRuntimeInstance(source, runID, flowInstance))
}

func ResolveForEntityRow(source semanticview.Source, row map[string]any) (Contract, bool) {
	if len(row) == 0 {
		return Contract{}, false
	}
	return ResolveForRuntimeInstance(source, asString(row["run_id"]), asString(row["flow_instance"]))
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
	optional := decl.IsOptional
	for _, segment := range segments[1:] {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return Field{}, fmt.Errorf("field is required")
		}
		resolved, err := resolveStructuralType(contract, currentType)
		if err != nil || resolved.Kind != runtimecontracts.CatalogTypeObject {
			return Field{}, fmt.Errorf("path %s does not resolve through a named type", path)
		}
		field, ok := resolved.Field(segment)
		if !ok {
			return Field{}, fmt.Errorf("undeclared path %s", path)
		}
		currentType = strings.TrimSpace(field.TypeRef)
		currentRefinements = field.Refinements
		optional = field.IsOptional
	}
	kind, err := pathKind(contract, currentType)
	if err != nil {
		return Field{}, err
	}
	return Field{Path: path, Type: currentType, LeafKind: kind, FieldDecl: decl, Refinements: currentRefinements, IsOptional: optional}, nil
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

func resolveStructuralType(contract Contract, typeRef string) (runtimecontracts.ResolvedCatalogType, error) {
	return (runtimecontracts.CatalogTypeReference{Type: strings.TrimSpace(typeRef), Catalog: contract.Types}).Resolve()
}

func pathKind(contract Contract, typeRef string) (string, error) {
	reference := runtimecontracts.CatalogTypeReference{Type: strings.TrimSpace(typeRef), Catalog: contract.Types}
	resolved, err := reference.Resolve()
	if err != nil {
		return "", err
	}
	switch resolved.Kind {
	case runtimecontracts.CatalogTypeText:
		if resolved.Name != "" {
			return "enum", nil
		}
		return "scalar", nil
	case runtimecontracts.CatalogTypeInteger, runtimecontracts.CatalogTypeNumber, runtimecontracts.CatalogTypeBoolean:
		return "scalar", nil
	case runtimecontracts.CatalogTypeObject:
		return "object", nil
	case runtimecontracts.CatalogTypeList:
		return "list", nil
	case runtimecontracts.CatalogTypeMap:
		return "map", nil
	case runtimecontracts.CatalogTypeDynamic:
		switch reference.ComparableDomain() {
		case "object":
			return "object", nil
		case "array":
			return "list", nil
		default:
			return "json", nil
		}
	default:
		return "", fmt.Errorf("unsupported resolved field kind %q", resolved.Kind)
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
	return strings.EqualFold(typeName(contract, typeRef), "object")
}

func isJSONValueType(contract Contract, typeRef string) bool {
	return strings.EqualFold(typeName(contract, typeRef), "json")
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
	return strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]") ||
		strings.HasSuffix(typeRef, "[]") ||
		strings.HasPrefix(typeRef, "[]")
}

func listItemType(typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	switch {
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
