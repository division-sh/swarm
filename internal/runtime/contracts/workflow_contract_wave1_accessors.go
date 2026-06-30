package contracts

import (
	"fmt"
	"path/filepath"
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
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s mode: template must declare instance.by, instance.on_missing, and instance.on_conflict", flowID)
	}
	primary, err := b.ResolveFlowPrimaryEntity(flowID)
	if err != nil {
		return TemplateInstanceContract{}, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s primary entity required for instance.by: %w", flowID, err)
	}
	by, err := validateTemplateInstanceBy(flowID, schema.Instance.By, primary)
	if err != nil {
		return TemplateInstanceContract{}, err
	}
	onMissing, err := validateTemplateInstancePolicy(flowID, "on_missing", schema.Instance.OnMissing, "create", "reject")
	if err != nil {
		return TemplateInstanceContract{}, err
	}
	onConflict, err := validateTemplateInstancePolicy(flowID, "on_conflict", schema.Instance.OnConflict, "reject", "reuse")
	if err != nil {
		return TemplateInstanceContract{}, err
	}
	return TemplateInstanceContract{
		FlowID:        flowID,
		By:            by,
		OnMissing:     onMissing,
		OnConflict:    onConflict,
		PrimaryEntity: primary,
	}, nil
}

func (b *WorkflowContractBundle) ResolveFlowSingletonCoordinator(flowID string) (SingletonCoordinatorContract, error) {
	flowID = strings.TrimSpace(flowID)
	label := defaultPrimaryEntityFlowLabel(flowID)
	if b == nil {
		return SingletonCoordinatorContract{}, fmt.Errorf("INVALID-SINGLETON-COORDINATOR: flow %s singleton coordinator is unavailable: bundle is nil", label)
	}
	if flowID == "" {
		return SingletonCoordinatorContract{}, fmt.Errorf("INVALID-SINGLETON-COORDINATOR: flow <root> cannot declare singleton coordinator ownership; singleton coordinators are child flow contracts")
	}
	schema, ok := b.FlowSchemas[flowID]
	if !ok {
		return SingletonCoordinatorContract{}, fmt.Errorf("INVALID-SINGLETON-COORDINATOR: flow %s singleton coordinator is unavailable: schema not found", flowID)
	}
	if mode := strings.TrimSpace(schema.Mode); mode != FlowModeSingleton {
		return SingletonCoordinatorContract{}, fmt.Errorf("INVALID-SINGLETON-COORDINATOR: flow %s is not mode: singleton", flowID)
	}
	if !schema.Instance.Empty() {
		return SingletonCoordinatorContract{}, fmt.Errorf("INVALID-SINGLETON-COORDINATOR: flow %s mode: singleton must not declare template instance", flowID)
	}
	primary, err := b.ResolveFlowPrimaryEntity(flowID)
	if err != nil {
		return SingletonCoordinatorContract{}, fmt.Errorf("INVALID-SINGLETON-COORDINATOR: flow %s primary entity required for singleton coordinator state: %w", flowID, err)
	}
	contained := singletonCoordinatorContainedFields(primary)
	if len(contained) == 0 {
		return SingletonCoordinatorContract{}, fmt.Errorf("INVALID-SINGLETON-COORDINATOR: flow %s mode: singleton must declare at least one typed contained map/list field on primary entity %s; agent conversation memory is not coordinator state authority", flowID, primary.EntityType)
	}
	return SingletonCoordinatorContract{
		FlowID:         flowID,
		PrimaryEntity:  primary,
		ContainedState: contained,
	}, nil
}

func singletonCoordinatorContainedFields(primary PrimaryEntityContract) []SingletonCoordinatorContainedField {
	fields := sortedEntityFieldKeys(primary.Contract.Fields)
	out := make([]SingletonCoordinatorContainedField, 0, len(fields))
	for _, field := range fields {
		decl := primary.Contract.Fields[field]
		kind := singletonCoordinatorContainedKind(decl.Type)
		if kind == "" {
			continue
		}
		out = append(out, SingletonCoordinatorContainedField{
			Name: field,
			Type: strings.TrimSpace(decl.Type),
			Kind: kind,
		})
	}
	return out
}

func singletonCoordinatorContainedKind(typeRef string) string {
	typeRef = strings.TrimSpace(typeRef)
	switch {
	case strings.HasPrefix(typeRef, "map["):
		return "map"
	case templateInstanceIsListType(typeRef):
		return "list"
	default:
		return ""
	}
}

func validateTemplateInstanceBy(flowID string, fields []string, primary PrimaryEntityContract) ([]string, error) {
	flowID = strings.TrimSpace(flowID)
	if len(fields) == 0 {
		return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.by is required", defaultPrimaryEntityFlowLabel(flowID))
	}
	out := make([]string, 0, len(fields))
	seen := map[string]struct{}{}
	for _, rawField := range fields {
		field := strings.TrimSpace(rawField)
		switch {
		case field == "":
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.by contains an empty field", flowID)
		case strings.Contains(field, "."):
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.by field %q must be a top-level primary-entity field", flowID, field)
		}
		if _, ok := seen[field]; ok {
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.by field %q is duplicated; composite keys must be unambiguous", flowID, field)
		}
		decl, ok := primary.Contract.Fields[field]
		if !ok {
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.by field %q is not declared on primary entity %s", flowID, field, primary.EntityType)
		}
		kind := templateInstanceFieldLeafKind(primary, decl.Type)
		if kind != "scalar" && kind != "enum" {
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.by field %q must resolve to a scalar or enum primary-entity field", flowID, field)
		}
		seen[field] = struct{}{}
		out = append(out, field)
	}
	return out, nil
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

func validateTemplateInstancePolicy(flowID, field, value string, allowed ...string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.%s is required", defaultPrimaryEntityFlowLabel(flowID), field)
	}
	for _, option := range allowed {
		if value == option {
			return value, nil
		}
	}
	return "", fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.%s value %q is unsupported; allowed values: %s", defaultPrimaryEntityFlowLabel(flowID), field, value, strings.Join(allowed, ", "))
}

func validateWave1ContractsLoadBoundary(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	if legacyScope, ok := bundleLegacyEntitySchemaScope(bundle); ok {
		return &LoadValidationError{Items: []error{
			errString("RETIRED: package.yaml entity_schema is no longer supported; migrate to entities.yaml (legacy scope: " + legacyScope + ")"),
		}}
	}
	flowScopedTypesFiles, flowScopedEntityFiles := flowScopedContractFiles(bundle)
	for _, pkg := range bundle.PackageTree {
		if strings.TrimSpace(pkg.Key) == "." {
			continue
		}
		if path := existingFile(filepath.Join(pkg.Paths.Dir, "types.yaml")); path != "" {
			if _, ok := flowScopedTypesFiles[filepath.Clean(path)]; !ok {
				return &LoadValidationError{Items: []error{
					errString("RETIRED: package-scoped types.yaml is not supported in Wave 1; move declarations to bundle root or flow scope (" + path + ")"),
				}}
			}
		}
		if path := existingFile(filepath.Join(pkg.Paths.Dir, "entities.yaml")); path != "" {
			if _, ok := flowScopedEntityFiles[filepath.Clean(path)]; !ok {
				return &LoadValidationError{Items: []error{
					errString("RETIRED: package-scoped entities.yaml is not supported in Wave 1; move declarations to bundle root or flow scope (" + path + ")"),
				}}
			}
		}
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

func flowScopedContractFiles(bundle *WorkflowContractBundle) (map[string]struct{}, map[string]struct{}) {
	typesFiles := map[string]struct{}{}
	entityFiles := map[string]struct{}{}
	if bundle == nil {
		return typesFiles, entityFiles
	}
	for _, pkg := range bundle.PackageTree {
		for _, flow := range pkg.Paths.Flows {
			if path := strings.TrimSpace(flow.TypesFile); path != "" {
				typesFiles[filepath.Clean(path)] = struct{}{}
			}
			if path := strings.TrimSpace(flow.EntitiesFile); path != "" {
				entityFiles[filepath.Clean(path)] = struct{}{}
			}
		}
	}
	return typesFiles, entityFiles
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

func bundleLegacyEntitySchemaScope(bundle *WorkflowContractBundle) (string, bool) {
	if bundle == nil {
		return "", false
	}
	for _, pkg := range bundle.PackageTree {
		if pkg.Manifest.EntitySchema.Empty() {
			continue
		}
		scope := strings.TrimSpace(pkg.Key)
		if scope == "" {
			scope = "."
		}
		return scope, true
	}
	if !bundle.Package.EntitySchema.Empty() {
		return ".", true
	}
	return "", false
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
	if flowID == "" {
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
	return resolvePrimaryEntityContract("", entity, b.RootEntities, b.RootTypeCatalog())
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
			out.Enums[key] = EnumTypeDecl{Values: append([]string{}, value.Values...)}
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
