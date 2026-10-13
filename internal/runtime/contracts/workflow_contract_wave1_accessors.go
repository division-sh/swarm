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
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: exact flow identity is required")
	}
	schema, ok := b.FlowSchemaByID(flowID)
	if !ok {
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s template instance is unavailable: schema not found", flowID)
	}
	if schema.Instance.Empty() {
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s must declare instance: <field>", flowID)
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
	if !schema.Instance.Empty() {
		return SingletonContract{}, fmt.Errorf("INVALID-SINGLETON: flow %s has a template instance key, not static cardinality", flowID)
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
	resolved, err := (CatalogTypeReference{Type: typeRef, Catalog: primary.Types}).Resolve()
	if err != nil {
		return "", err
	}
	switch resolved.Kind {
	case CatalogTypeMap:
		return "map", nil
	case CatalogTypeList:
		return "list", nil
	}
	return "", nil
}

func singletonCoordinatorValidateTypeRef(types TypeCatalogDocument, typeRef string) error {
	_, err := (CatalogTypeReference{Type: typeRef, Catalog: types}).Resolve()
	return err
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
	ref := CatalogTypeReference{Type: typeRef, Catalog: primary.Types}
	resolved, err := ref.Resolve()
	if err != nil {
		return ""
	}
	switch resolved.Kind {
	case CatalogTypeText:
		if resolved.Name != "" {
			return "enum"
		}
		return "scalar"
	case CatalogTypeInteger, CatalogTypeNumber, CatalogTypeBoolean:
		return "scalar"
	case CatalogTypeObject:
		return "object"
	case CatalogTypeList:
		return "list"
	case CatalogTypeDynamic:
		return ref.ComparableDomain()
	}
	return ""
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
	if len(bundle.RootEntities) <= 1 {
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
	_, ok := bundle.FlowSchemas[flowID]
	if !ok {
		return nil
	}
	entities := bundle.flowEntities[flowID]
	if len(entities) <= 1 {
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

// StructuralType preserves declaration optionality; definite assignment is
// tracked separately at each executable read.
func (p PrimaryEntityContract) StructuralType() (ResolvedCatalogType, error) {
	fields := make([]ResolvedCatalogField, 0, len(p.Contract.Fields))
	for _, name := range sortedEntityFieldKeys(p.Contract.Fields) {
		decl := p.Contract.Fields[name]
		resolved, err := (CatalogTypeReference{Type: decl.Type, Catalog: p.Types}).Resolve()
		if err != nil {
			return ResolvedCatalogType{}, fmt.Errorf("entity %s field %s: %w", p.EntityType, name, err)
		}
		fields = append(fields, ResolvedCatalogField{Name: name, Type: resolved, TypeRef: decl.Type, IsOptional: decl.IsOptional, Refinements: decl.Refinements.Clone()})
	}
	return ResolvedCatalogType{Kind: CatalogTypeObject, Name: p.FlowID + "." + p.EntityType, Fields: fields}, nil
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
	_, ok := b.FlowSchemas[flowID]
	if !ok {
		return PrimaryEntityContract{}, fmt.Errorf("INVALID-PRIMARY-ENTITY: flow %s primary entity is unavailable: schema not found", flowID)
	}
	return resolvePrimaryEntityContract(flowID, b.flowEntities[flowID], b.ResolvedTypeCatalogForFlow(flowID))
}

func (b *WorkflowContractBundle) ResolveRootPrimaryEntity() (PrimaryEntityContract, error) {
	if b == nil {
		return PrimaryEntityContract{}, fmt.Errorf("INVALID-PRIMARY-ENTITY: root primary entity is unavailable: bundle is nil")
	}
	return resolvePrimaryEntityContract(".", b.RootEntities, b.RootTypeCatalog())
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

func resolvePrimaryEntityContract(flowID string, entities EntityContractsDocument, types TypeCatalogDocument) (PrimaryEntityContract, error) {
	flowID = strings.TrimSpace(flowID)
	label := defaultPrimaryEntityFlowLabel(flowID)
	keys := sortedEntityContractKeys(entities)
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
	incoming = cloneTypeCatalogDocument(incoming)
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
			value.Refinements = value.Refinements.Clone()
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
			value.Refinements = value.Refinements.Clone()
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
