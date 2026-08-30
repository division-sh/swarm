package contracts

import (
	"fmt"
	"sort"
	"strings"
)

type PrimaryEntityContract struct {
	FlowID     string
	EntityType string
	Contract   EntityContract
	Types      TypeCatalogDocument
}

func (b *WorkflowContractBundle) ResolveFlowTemplateInstance(flowID string) (TemplateInstanceContract, error) {
	flowID = strings.TrimSpace(flowID)
	label := defaultPrimaryEntityFlowLabel(flowID)
	if b == nil {
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s template instance is unavailable: bundle is nil", label)
	}
	if flowID == "" {
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow <root> cannot declare a template instance key; template instances are child flow contracts")
	}
	schema, ok := b.FlowSchemas[flowID]
	if !ok {
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s template instance is unavailable: schema not found", flowID)
	}
	mode := strings.TrimSpace(schema.Mode)
	if mode != FlowModeTemplate {
		if !schema.Instance.Empty() {
			return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s declares instance but is not mode: template", flowID)
		}
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s is not mode: template", flowID)
	}
	if schema.Instance.Empty() {
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s mode: template must declare instance: <field>", flowID)
	}
	primary, err := b.ResolveFlowPrimaryEntity(flowID)
	if err != nil {
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s primary entity required for instance: %w", flowID, err)
	}
	field, err := validateTemplateInstanceField(flowID, schema.Instance, primary)
	if err != nil {
		return TemplateInstanceContract{}, err
	}
	return TemplateInstanceContract{
		FlowID:        flowID,
		Field:         field,
		PrimaryEntity: primary,
	}, nil
}

func (b *WorkflowContractBundle) ResolveFlowSingleton(flowID string) (SingletonContract, error) {
	flowID = strings.TrimSpace(flowID)
	label := defaultPrimaryEntityFlowLabel(flowID)
	if b == nil {
		return SingletonContract{}, fmt.Errorf("INVALID-SINGLETON: flow %s singleton is unavailable: bundle is nil", label)
	}
	if flowID == "" {
		return SingletonContract{}, fmt.Errorf("INVALID-SINGLETON: flow <root> cannot declare singleton cardinality; singleton flows are child flow contracts")
	}
	schema, ok := b.FlowSchemas[flowID]
	if !ok {
		return SingletonContract{}, fmt.Errorf("INVALID-SINGLETON: flow %s singleton is unavailable: schema not found", flowID)
	}
	if mode := strings.TrimSpace(schema.Mode); mode != FlowModeSingleton {
		return SingletonContract{}, fmt.Errorf("INVALID-SINGLETON: flow %s is not mode: singleton", flowID)
	}
	if !schema.Instance.Empty() {
		return SingletonContract{}, fmt.Errorf("INVALID-SINGLETON: flow %s mode: singleton must not declare template instance", flowID)
	}
	primary, err := b.ResolveFlowPrimaryEntity(flowID)
	if err != nil {
		return SingletonContract{}, fmt.Errorf("INVALID-SINGLETON: flow %s requires exactly one primary entity: %w", flowID, err)
	}
	if _, err := singletonCoordinatorContainedFields(primary); err != nil {
		return SingletonContract{}, fmt.Errorf("INVALID-SINGLETON: flow %s invalid typed entity state: %w", flowID, err)
	}
	return SingletonContract{FlowID: flowID, PrimaryEntity: primary}, nil
}

func (b *WorkflowContractBundle) ResolveFlowSingletonCoordinator(flowID string) (SingletonCoordinatorContract, error) {
	singleton, err := b.ResolveFlowSingleton(flowID)
	if err != nil {
		return SingletonCoordinatorContract{}, fmt.Errorf("INVALID-SINGLETON-COORDINATOR: %w", err)
	}
	primary := singleton.PrimaryEntity
	contained, err := singletonCoordinatorContainedFields(primary)
	if err != nil {
		return SingletonCoordinatorContract{}, fmt.Errorf("INVALID-SINGLETON-COORDINATOR: flow %s invalid contained coordinator state: %w", singleton.FlowID, err)
	}
	if len(contained) == 0 {
		return SingletonCoordinatorContract{}, fmt.Errorf("INVALID-SINGLETON-COORDINATOR: flow %s coordinator demand requires at least one typed contained map/list field on primary entity %s; agent conversation memory is not coordinator state authority", singleton.FlowID, primary.EntityType)
	}
	return SingletonCoordinatorContract{
		FlowID:         singleton.FlowID,
		PrimaryEntity:  primary,
		ContainedState: contained,
	}, nil
}

func SingletonContainedFields(primary PrimaryEntityContract) ([]SingletonCoordinatorContainedField, error) {
	return singletonCoordinatorContainedFields(primary)
}

func singletonCoordinatorContainedFields(primary PrimaryEntityContract) ([]SingletonCoordinatorContainedField, error) {
	fields := sortedEntityFieldKeys(primary.Contract.Fields)
	out := make([]SingletonCoordinatorContainedField, 0, len(fields))
	for _, field := range fields {
		decl := primary.Contract.Fields[field]
		kind, err := singletonCoordinatorContainedKind(primary, decl.Type)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field, err)
		}
		if kind == "" {
			continue
		}
		out = append(out, SingletonCoordinatorContainedField{
			Name: field,
			Type: strings.TrimSpace(decl.Type),
			Kind: kind,
		})
	}
	return out, nil
}

func singletonCoordinatorContainedKind(primary PrimaryEntityContract, typeRef string) (string, error) {
	typeRef = strings.TrimSpace(typeRef)
	if keyType, valueType, ok := singletonCoordinatorMapTypeParts(typeRef); ok {
		if err := singletonCoordinatorValidateMapKeyType(primary.Types, keyType); err != nil {
			return "", err
		}
		if err := singletonCoordinatorValidateTypeRef(primary.Types, valueType); err != nil {
			return "", fmt.Errorf("map value type %q does not resolve: %w", valueType, err)
		}
		return "map", nil
	}
	if templateInstanceIsListType(typeRef) {
		itemType := singletonCoordinatorListItemType(typeRef)
		if strings.TrimSpace(itemType) == "" {
			return "", fmt.Errorf("list item type is required")
		}
		if err := singletonCoordinatorValidateTypeRef(primary.Types, itemType); err != nil {
			return "", fmt.Errorf("list item type %q does not resolve: %w", itemType, err)
		}
		return "list", nil
	}
	return "", nil
}

func singletonCoordinatorValidateTypeRef(types TypeCatalogDocument, typeRef string) error {
	return singletonCoordinatorValidateTypeRefSeen(types, typeRef, map[string]struct{}{})
}

func singletonCoordinatorValidateTypeRefSeen(types TypeCatalogDocument, typeRef string, seen map[string]struct{}) error {
	typeRef = strings.TrimSpace(typeRef)
	if typeRef == "" {
		return fmt.Errorf("type is required")
	}
	if keyType, valueType, ok := singletonCoordinatorMapTypeParts(typeRef); ok {
		if err := singletonCoordinatorValidateMapKeyType(types, keyType); err != nil {
			return err
		}
		if err := singletonCoordinatorValidateTypeRefSeen(types, valueType, seen); err != nil {
			return fmt.Errorf("map value type %q does not resolve: %w", valueType, err)
		}
		return nil
	}
	if templateInstanceIsListType(typeRef) {
		itemType := singletonCoordinatorListItemType(typeRef)
		if strings.TrimSpace(itemType) == "" {
			return fmt.Errorf("list item type is required")
		}
		if err := singletonCoordinatorValidateTypeRefSeen(types, itemType, seen); err != nil {
			return fmt.Errorf("list item type %q does not resolve: %w", itemType, err)
		}
		return nil
	}
	if templateInstanceIsTextType(typeRef) ||
		templateInstanceIsIntegerType(types, typeRef) ||
		templateInstanceIsNumericType(types, typeRef) ||
		templateInstanceIsBooleanType(types, typeRef) ||
		templateInstanceIsJSONObjectType(types, typeRef) ||
		templateInstanceIsJSONArrayType(types, typeRef) ||
		templateInstanceIsTimestampType(types, typeRef) ||
		templateInstanceIsUUIDType(types, typeRef) ||
		templateInstanceIsEnumType(types, typeRef) {
		return nil
	}
	typeName := templateInstanceTypeName(types, typeRef)
	named, ok := types.Types[typeName]
	if !ok {
		return fmt.Errorf("unsupported contract type %s", typeRef)
	}
	if _, ok := seen[typeName]; ok {
		return nil
	}
	seen[typeName] = struct{}{}
	for field, spec := range named.Fields {
		if err := singletonCoordinatorValidateTypeRefSeen(types, spec.Type, seen); err != nil {
			return fmt.Errorf("field %s: %w", strings.TrimSpace(field), err)
		}
	}
	delete(seen, typeName)
	return nil
}

func singletonCoordinatorValidateMapKeyType(types TypeCatalogDocument, keyType string) error {
	if templateInstanceIsTextType(keyType) || templateInstanceIsUUIDType(types, keyType) || templateInstanceIsEnumType(types, keyType) {
		return nil
	}
	return fmt.Errorf("map key type %q is unsupported; use text, uuid, or enum", strings.TrimSpace(keyType))
}

func singletonCoordinatorMapTypeParts(typeRef string) (string, string, bool) {
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

func singletonCoordinatorListItemType(typeRef string) string {
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

func validateTemplateInstanceField(flowID string, field TemplateInstanceField, primary PrimaryEntityContract) (TemplateInstanceField, error) {
	flowID = strings.TrimSpace(flowID)
	if field.Empty() {
		return TemplateInstanceField{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance: <field> is required", defaultPrimaryEntityFlowLabel(flowID))
	}
	name := field.Path()
	decl, ok := primary.Contract.Fields[name]
	if !ok {
		return TemplateInstanceField{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance field %q is not declared on primary entity %s", flowID, name, primary.EntityType)
	}
	kind := templateInstanceFieldLeafKind(primary, decl.Type)
	if kind != "scalar" && kind != "enum" {
		return TemplateInstanceField{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance field %q must resolve to a scalar or enum primary-entity field", flowID, name)
	}
	return field, nil
}

func templateInstanceFieldLeafKind(primary PrimaryEntityContract, typeRef string) string {
	switch {
	case templateInstanceIsTextType(typeRef):
		return "scalar"
	case templateInstanceIsIntegerType(primary.Types, typeRef):
		return "scalar"
	case templateInstanceIsNumericType(primary.Types, typeRef):
		return "scalar"
	case templateInstanceIsBooleanType(primary.Types, typeRef):
		return "scalar"
	case templateInstanceIsJSONObjectType(primary.Types, typeRef):
		return "object"
	case templateInstanceIsJSONArrayType(primary.Types, typeRef):
		return "list"
	case templateInstanceIsTimestampType(primary.Types, typeRef):
		return "scalar"
	case templateInstanceIsUUIDType(primary.Types, typeRef):
		return "scalar"
	case templateInstanceIsEnumType(primary.Types, typeRef):
		return "enum"
	case templateInstanceIsNamedType(primary.Types, typeRef):
		return "object"
	case templateInstanceIsListType(typeRef):
		return "list"
	default:
		return ""
	}
}

func templateInstanceTypeName(types TypeCatalogDocument, typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	if scalar, ok := types.Scalars[typeRef]; ok {
		return strings.TrimSpace(scalar.Base)
	}
	return typeRef
}

func templateInstanceIsNamedType(types TypeCatalogDocument, typeRef string) bool {
	_, ok := types.Types[templateInstanceTypeName(types, typeRef)]
	return ok
}

func templateInstanceIsEnumType(types TypeCatalogDocument, typeRef string) bool {
	_, ok := types.Enums[templateInstanceTypeName(types, typeRef)]
	return ok
}

func templateInstanceIsTextType(typeRef string) bool {
	typeRef = strings.ToLower(strings.TrimSpace(typeRef))
	return typeRef == "text" || typeRef == "string"
}

func templateInstanceIsIntegerType(types TypeCatalogDocument, typeRef string) bool {
	return strings.EqualFold(templateInstanceTypeName(types, typeRef), "integer")
}

func templateInstanceIsNumericType(types TypeCatalogDocument, typeRef string) bool {
	raw := strings.ToLower(strings.TrimSpace(templateInstanceTypeName(types, typeRef)))
	return raw == "numeric" || raw == "number" || raw == "float" || raw == "double" || raw == "real" || strings.HasPrefix(raw, "numeric(")
}

func templateInstanceIsBooleanType(types TypeCatalogDocument, typeRef string) bool {
	return strings.EqualFold(templateInstanceTypeName(types, typeRef), "boolean")
}

func templateInstanceIsJSONObjectType(types TypeCatalogDocument, typeRef string) bool {
	raw := strings.ToLower(strings.TrimSpace(templateInstanceTypeName(types, typeRef)))
	return raw == "json" || raw == "object"
}

func templateInstanceIsJSONArrayType(types TypeCatalogDocument, typeRef string) bool {
	return strings.EqualFold(templateInstanceTypeName(types, typeRef), "array")
}

func templateInstanceIsTimestampType(types TypeCatalogDocument, typeRef string) bool {
	return strings.EqualFold(templateInstanceTypeName(types, typeRef), "timestamp")
}

func templateInstanceIsUUIDType(types TypeCatalogDocument, typeRef string) bool {
	return strings.EqualFold(templateInstanceTypeName(types, typeRef), "uuid")
}

func templateInstanceIsListType(typeRef string) bool {
	typeRef = strings.TrimSpace(typeRef)
	return strings.HasPrefix(typeRef, "list<") && strings.HasSuffix(typeRef, ">") ||
		strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]") ||
		strings.HasSuffix(typeRef, "[]") ||
		strings.HasPrefix(typeRef, "[]")
}

func validateWave1ContractsLoadBoundary(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	for entityType, contract := range bundle.RootEntities {
		if strings.TrimSpace(contract.Owner) != "" {
			return &LoadValidationError{Items: []error{
				errString("UNDEFINED-FIELD: root entity contract " + strings.TrimSpace(entityType) + " must not declare _owner; ownership is implied by root location"),
			}}
		}
	}
	if err := validateRootPrimaryEntityLoadBoundary(bundle); err != nil {
		return &LoadValidationError{Items: []error{err}}
	}
	for _, entities := range bundle.flowEntities {
		for entityType, contract := range entities {
			if strings.TrimSpace(contract.Owner) != "" {
				return &LoadValidationError{Items: []error{
					errString("UNDEFINED-FIELD: flow entity contract " + strings.TrimSpace(entityType) + " must not declare _owner; ownership is implied by flow location"),
				}}
			}
		}
	}
	for _, flowID := range sortedFlowSchemaIDs(bundle.FlowSchemas) {
		if err := validatePrimaryEntityLoadBoundary(bundle, flowID); err != nil {
			return &LoadValidationError{Items: []error{err}}
		}
	}
	return nil
}

func validateRootPrimaryEntityLoadBoundary(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	declared := ""
	if bundle.RootSchema != nil {
		declared = bundle.RootSchema.Entity
	}
	if strings.TrimSpace(declared) == "" && len(bundle.RootEntities) <= 1 {
		return nil
	}
	if _, err := bundle.ResolveRootPrimaryEntity(); err != nil {
		return err
	}
	return nil
}

func validatePrimaryEntityLoadBoundary(bundle *WorkflowContractBundle, flowID string) error {
	flowID = strings.TrimSpace(flowID)
	if bundle == nil || flowID == "" {
		return nil
	}
	schema, ok := bundle.FlowSchemas[flowID]
	if !ok {
		return nil
	}
	entities := bundle.flowEntities[flowID]
	if strings.TrimSpace(schema.Entity) == "" && len(entities) <= 1 {
		return nil
	}
	if _, err := bundle.ResolveFlowPrimaryEntity(flowID); err != nil {
		return err
	}
	return nil
}

func sortedFlowSchemaIDs(schemas map[string]FlowSchemaDocument) []string {
	ids := make([]string, 0, len(schemas))
	for id := range schemas {
		id = strings.TrimSpace(id)
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

type errString string

func (e errString) Error() string { return string(e) }

func (b *WorkflowContractBundle) RootTypeCatalog() TypeCatalogDocument {
	if b == nil {
		return TypeCatalogDocument{}
	}
	return cloneTypeCatalogDocument(b.RootTypes)
}

func (b *WorkflowContractBundle) RootEntityContracts() EntityContractsDocument {
	if b == nil {
		return nil
	}
	return cloneEntityContractsDocument(b.RootEntities)
}

func (b *WorkflowContractBundle) FlowTypeCatalogByID(flowID string) (TypeCatalogDocument, bool) {
	flowID = strings.TrimSpace(flowID)
	if b == nil || flowID == "" {
		return TypeCatalogDocument{}, false
	}
	doc, ok := b.flowTypes[flowID]
	return cloneTypeCatalogDocument(doc), ok
}

func (b *WorkflowContractBundle) FlowEntityContractsByID(flowID string) (EntityContractsDocument, bool) {
	flowID = strings.TrimSpace(flowID)
	if b == nil || flowID == "" {
		return nil, false
	}
	doc, ok := b.flowEntities[flowID]
	return cloneEntityContractsDocument(doc), ok
}

func (b *WorkflowContractBundle) FlowPrimaryEntityContract(flowID string) (string, EntityContract, bool) {
	resolved, err := b.ResolveFlowPrimaryEntity(flowID)
	if err != nil {
		return "", EntityContract{}, false
	}
	return resolved.EntityType, cloneEntityContract(resolved.Contract), true
}

func (b *WorkflowContractBundle) RootPrimaryEntityContract() (string, EntityContract, bool) {
	resolved, err := b.ResolveRootPrimaryEntity()
	if err != nil {
		return "", EntityContract{}, false
	}
	return resolved.EntityType, cloneEntityContract(resolved.Contract), true
}

func (b *WorkflowContractBundle) ResolveFlowPrimaryEntity(flowID string) (PrimaryEntityContract, error) {
	flowID = strings.TrimSpace(flowID)
	if b == nil {
		return PrimaryEntityContract{}, fmt.Errorf("INVALID-PRIMARY-ENTITY: flow %s primary entity is unavailable: bundle is nil", defaultPrimaryEntityFlowLabel(flowID))
	}
	if flowID == "" || flowID == "." {
		return b.ResolveRootPrimaryEntity()
	}
	schema, ok := b.FlowSchemas[flowID]
	if !ok {
		return PrimaryEntityContract{}, fmt.Errorf("INVALID-PRIMARY-ENTITY: flow %s primary entity is unavailable: schema not found", flowID)
	}
	return resolvePrimaryEntityContract(flowID, schema.Entity, b.flowEntities[flowID], b.ResolvedTypeCatalogForFlow(flowID))
}

func (b *WorkflowContractBundle) ResolveRootPrimaryEntity() (PrimaryEntityContract, error) {
	if b == nil {
		return PrimaryEntityContract{}, fmt.Errorf("INVALID-PRIMARY-ENTITY: root primary entity is unavailable: bundle is nil")
	}
	entity := ""
	if b.RootSchema != nil {
		entity = b.RootSchema.Entity
	}
	return resolvePrimaryEntityContract(".", entity, b.RootEntities, b.RootTypeCatalog())
}

func (b *WorkflowContractBundle) ResolveTestSetupPrimaryEntity(flowID, entityType string) (PrimaryEntityContract, error) {
	primary, err := b.ResolveFlowPrimaryEntity(flowID)
	if err == nil {
		return primary, nil
	}
	if strings.TrimSpace(flowID) == "." && strings.TrimSpace(entityType) == "default" && len(b.RootEntities) == 0 {
		return PrimaryEntityContract{
			FlowID:     ".",
			EntityType: "default",
			Contract:   EntityContract{Fields: map[string]EntityFieldDecl{}},
			Types:      b.RootTypeCatalog(),
		}, nil
	}
	return PrimaryEntityContract{}, err
}

func resolvePrimaryEntityContract(flowID, declared string, entities EntityContractsDocument, types TypeCatalogDocument) (PrimaryEntityContract, error) {
	flowID = strings.TrimSpace(flowID)
	declared = strings.TrimSpace(declared)
	label := defaultPrimaryEntityFlowLabel(flowID)
	keys := sortedEntityContractKeys(entities)
	if declared != "" {
		return PrimaryEntityContract{}, fmt.Errorf("INVALID-PRIMARY-ENTITY: flow %s uses schema.yaml entity %q, but normal flow authoring has a single entity authority: declare exactly one flow entity type in entities.yaml and do not restate it in schema.yaml", label, declared)
	}
	switch len(keys) {
	case 0:
		return PrimaryEntityContract{}, fmt.Errorf("INVALID-PRIMARY-ENTITY: flow %s has no declared entity types; stateful normal flows must declare exactly one entity type or be explicitly stateless/template", label)
	case 1:
		entityType := keys[0]
		return PrimaryEntityContract{
			FlowID:     flowID,
			EntityType: entityType,
			Contract:   cloneEntityContract(entities[entityType]),
			Types:      cloneTypeCatalogDocument(types),
		}, nil
	default:
		return PrimaryEntityContract{}, fmt.Errorf("INVALID-PRIMARY-ENTITY: flow %s declares multiple entity types %s; normal flow authoring supports exactly one entity type", label, strings.Join(keys, ", "))
	}
}

func sortedEntityContractKeys(entities EntityContractsDocument) []string {
	keys := make([]string, 0, len(entities))
	for key := range entities {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func sortedEntityFieldKeys(fields map[string]EntityFieldDecl) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func defaultPrimaryEntityFlowLabel(flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return "<root>"
	}
	return flowID
}

func (b *WorkflowContractBundle) ResolvedTypeCatalogForFlow(flowID string) TypeCatalogDocument {
	flowID = strings.TrimSpace(flowID)
	if b == nil {
		return TypeCatalogDocument{}
	}
	resolved := cloneTypeCatalogDocument(b.RootTypes)
	if flowID != "" {
		if flowDoc, ok := b.flowTypes[flowID]; ok {
			resolved = mergeTypeCatalogDocuments(resolved, flowDoc)
		}
	}
	return resolved
}

func (b *WorkflowContractBundle) ResolvedEntityContractsForFlow(flowID string) EntityContractsDocument {
	flowID = strings.TrimSpace(flowID)
	if b == nil {
		return nil
	}
	resolved := cloneEntityContractsDocument(b.RootEntities)
	if flowID != "" {
		if flowDoc, ok := b.flowEntities[flowID]; ok {
			resolved = mergeEntityContractsDocuments(resolved, flowDoc)
		}
	}
	return resolved
}

func mergeTypeCatalogDocuments(base, incoming TypeCatalogDocument) TypeCatalogDocument {
	out := cloneTypeCatalogDocument(base)
	if out.Scalars == nil {
		out.Scalars = map[string]ScalarTypeDecl{}
	}
	if out.Enums == nil {
		out.Enums = map[string]EnumTypeDecl{}
	}
	if out.Types == nil {
		out.Types = map[string]NamedTypeDecl{}
	}
	for key, value := range incoming.Scalars {
		out.Scalars[key] = value
	}
	for key, value := range incoming.Enums {
		out.Enums[key] = value
	}
	for key, value := range incoming.Types {
		out.Types[key] = value
	}
	return out
}

func mergeEntityContractsDocuments(base, incoming EntityContractsDocument) EntityContractsDocument {
	out := cloneEntityContractsDocument(base)
	if out == nil {
		out = EntityContractsDocument{}
	}
	for key, value := range incoming {
		out[key] = cloneEntityContract(value)
	}
	return out
}

func cloneTypeCatalogDocument(in TypeCatalogDocument) TypeCatalogDocument {
	out := TypeCatalogDocument{}
	if len(in.Scalars) > 0 {
		out.Scalars = make(map[string]ScalarTypeDecl, len(in.Scalars))
		for key, value := range in.Scalars {
			out.Scalars[key] = value
		}
	}
	if len(in.Enums) > 0 {
		out.Enums = make(map[string]EnumTypeDecl, len(in.Enums))
		for key, value := range in.Enums {
			out.Enums[key] = EnumTypeDecl{Values: append([]string{}, value.Values...), Default: value.Default}
		}
	}
	if len(in.Types) > 0 {
		out.Types = make(map[string]NamedTypeDecl, len(in.Types))
		for key, value := range in.Types {
			out.Types[key] = cloneNamedTypeDecl(value)
		}
	}
	return out
}

func cloneNamedTypeDecl(in NamedTypeDecl) NamedTypeDecl {
	out := NamedTypeDecl{
		Description: in.Description,
	}
	if len(in.Fields) > 0 {
		out.Fields = make(map[string]TypeFieldSpec, len(in.Fields))
		for key, value := range in.Fields {
			out.Fields[key] = value
		}
	}
	return out
}

func cloneEntityContractsDocument(in EntityContractsDocument) EntityContractsDocument {
	if len(in) == 0 {
		return nil
	}
	out := make(EntityContractsDocument, len(in))
	for key, value := range in {
		out[key] = cloneEntityContract(value)
	}
	return out
}

func cloneEntityContract(in EntityContract) EntityContract {
	out := EntityContract{
		Description: in.Description,
		Owner:       in.Owner,
	}
	if len(in.Fields) > 0 {
		out.Fields = make(map[string]EntityFieldDecl, len(in.Fields))
		for key, value := range in.Fields {
			if len(value.Project) > 0 {
				project := make(map[string]any, len(value.Project))
				for projectKey, projectValue := range value.Project {
					project[projectKey] = projectValue
				}
				value.Project = project
			}
			out.Fields[key] = value
		}
	}
	return out
}
