package contracts

import "strings"

func recordContractCompatibilityUsage(bundle *WorkflowContractBundle, usage ContractCompatibilityUsage) {
	if bundle == nil {
		return
	}
	usage.Kind = strings.TrimSpace(usage.Kind)
	usage.File = strings.TrimSpace(usage.File)
	usage.Scope = strings.TrimSpace(usage.Scope)
	usage.ItemID = strings.TrimSpace(usage.ItemID)
	usage.Detail = strings.TrimSpace(usage.Detail)
	if usage.Kind == "" || usage.File == "" {
		return
	}
	bundle.Compatibility = append(bundle.Compatibility, usage)
}

func recordEventCompatibilityUsages(bundle *WorkflowContractBundle, entries map[string]EventCatalogEntry, file, scope string) {
	if bundle == nil || strings.TrimSpace(file) == "" || len(entries) == 0 {
		return
	}
	for eventType, entry := range entries {
		if !entry.UsesLegacyPayload {
			continue
		}
		recordContractCompatibilityUsage(bundle, ContractCompatibilityUsage{
			Kind:   "legacy_event_payload_block",
			File:   file,
			Scope:  scope,
			ItemID: strings.TrimSpace(eventType),
			Detail: "events.yaml payload: block loaded through Wave 1 migration compatibility",
		})
	}
}

func validateWave1ContractsLoadBoundary(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	if bundle.Package.UsesLegacyEntitySchema && (len(bundle.RootEntities) > 0 || len(bundle.projectEntities) > 0 || len(bundle.flowEntities) > 0) {
		return &LoadValidationError{Items: []error{
			errString("AMBIGUOUS-CONTRACT-GRAMMAR: package.yaml entity_schema cannot coexist with Wave 1 entities.yaml declarations in the same bundle"),
		}}
	}
	for flowID, entities := range bundle.flowEntities {
		if len(entities) > 1 {
			return &LoadValidationError{Items: []error{
				errString("INVALID-ENTITY-OWNERSHIP: flow " + strings.TrimSpace(flowID) + " declares multiple entity types; Wave 1 permits at most one entity type per flow"),
			}}
		}
		for entityType, contract := range entities {
			if strings.TrimSpace(contract.Owner) != "" {
				return &LoadValidationError{Items: []error{
					errString("UNDEFINED-FIELD: flow entity contract " + strings.TrimSpace(entityType) + " must not declare _owner; ownership is implied by flow location"),
				}}
			}
		}
	}
	return nil
}

type errString string

func (e errString) Error() string { return string(e) }

func (b *WorkflowContractBundle) CompatibilityUsages() []ContractCompatibilityUsage {
	if b == nil || len(b.Compatibility) == 0 {
		return nil
	}
	out := make([]ContractCompatibilityUsage, len(b.Compatibility))
	copy(out, b.Compatibility)
	return out
}

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

func (b *WorkflowContractBundle) ProjectTypeCatalogByKey(key string) (TypeCatalogDocument, bool) {
	key = strings.TrimSpace(key)
	if b == nil || key == "" || key == "." {
		return TypeCatalogDocument{}, false
	}
	doc, ok := b.projectTypes[key]
	return cloneTypeCatalogDocument(doc), ok
}

func (b *WorkflowContractBundle) ProjectEntityContractsByKey(key string) (EntityContractsDocument, bool) {
	key = strings.TrimSpace(key)
	if b == nil || key == "" || key == "." {
		return nil, false
	}
	doc, ok := b.projectEntities[key]
	return cloneEntityContractsDocument(doc), ok
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

func (b *WorkflowContractBundle) FlowOwnedEntityContract(flowID string) (string, EntityContract, bool) {
	entities, ok := b.FlowEntityContractsByID(flowID)
	if !ok || len(entities) == 0 {
		return "", EntityContract{}, false
	}
	for entityType, contract := range entities {
		return strings.TrimSpace(entityType), cloneEntityContract(contract), true
	}
	return "", EntityContract{}, false
}

func (b *WorkflowContractBundle) ResolvedTypeCatalogForFlow(flowID string) TypeCatalogDocument {
	flowID = strings.TrimSpace(flowID)
	if b == nil {
		return TypeCatalogDocument{}
	}
	resolved := cloneTypeCatalogDocument(b.RootTypes)
	packageKey := ""
	if flowID != "" {
		if view, ok := b.FlowViewByID(flowID); ok {
			packageKey = strings.TrimSpace(view.Paths.PackageKey)
		}
	}
	for _, key := range b.packageLineageKeys(packageKey) {
		doc, ok := b.projectTypes[key]
		if !ok {
			continue
		}
		resolved = mergeTypeCatalogDocuments(resolved, doc)
	}
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
	packageKey := ""
	if flowID != "" {
		if view, ok := b.FlowViewByID(flowID); ok {
			packageKey = strings.TrimSpace(view.Paths.PackageKey)
		}
	}
	for _, key := range b.packageLineageKeys(packageKey) {
		doc, ok := b.projectEntities[key]
		if !ok {
			continue
		}
		resolved = mergeEntityContractsDocuments(resolved, doc)
	}
	if flowID != "" {
		if flowDoc, ok := b.flowEntities[flowID]; ok {
			resolved = mergeEntityContractsDocuments(resolved, flowDoc)
		}
	}
	return resolved
}

func (b *WorkflowContractBundle) packageLineageKeys(key string) []string {
	key = strings.TrimSpace(key)
	if b == nil || key == "" || key == "." {
		return nil
	}
	parents := map[string]string{}
	for _, pkg := range b.PackageTree {
		parents[strings.TrimSpace(pkg.Key)] = strings.TrimSpace(pkg.ParentKey)
	}
	var lineage []string
	for key != "" && key != "." {
		lineage = append([]string{key}, lineage...)
		key = parents[key]
	}
	return lineage
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
			out.Fields[key] = value
		}
	}
	return out
}
