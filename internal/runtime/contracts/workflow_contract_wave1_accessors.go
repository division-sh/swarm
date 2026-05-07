package contracts

import (
	"path/filepath"
	"strings"
)

func validateWave1ContractsLoadBoundary(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	if legacyScope, ok := bundleLegacyEntitySchemaScope(bundle); ok {
		return &LoadValidationError{Items: []error{
			errString("RETIRED: package.yaml entity_schema is no longer supported; migrate to entities.yaml (legacy scope: " + legacyScope + ")"),
		}}
	}
	for _, pkg := range bundle.PackageTree {
		if strings.TrimSpace(pkg.Key) == "." {
			continue
		}
		if path := existingFile(filepath.Join(pkg.Paths.Dir, "types.yaml")); path != "" {
			return &LoadValidationError{Items: []error{
				errString("RETIRED: package-scoped types.yaml is not supported in Wave 1; move declarations to bundle root or flow scope (" + path + ")"),
			}}
		}
		if path := existingFile(filepath.Join(pkg.Paths.Dir, "entities.yaml")); path != "" {
			return &LoadValidationError{Items: []error{
				errString("RETIRED: package-scoped entities.yaml is not supported in Wave 1; move declarations to bundle root or flow scope (" + path + ")"),
			}}
		}
	}
	for entityType, contract := range bundle.RootEntities {
		if strings.TrimSpace(contract.Owner) != "" {
			return &LoadValidationError{Items: []error{
				errString("UNDEFINED-FIELD: root entity contract " + strings.TrimSpace(entityType) + " must not declare _owner; ownership is implied by root location"),
			}}
		}
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
