package contracts

import (
	"fmt"
	"sort"
)

// Validate every declaration in its exact visible scope, including unused ones,
// before any compiled projection can replace an invalid reference with a shape.
func validateWorkflowCatalogReferences(bundle *WorkflowContractBundle) error {
	for _, source := range sortedFlowSources(bundle.FlowSources) {
		if err := validateFlowCatalogReferences(bundle, source); err != nil {
			return err
		}
	}
	return validateEventCatalogReferences(bundle)
}

func validateFlowCatalogReferences(bundle *WorkflowContractBundle, source FlowSource) error {
	types := bundle.RootTypeCatalog()
	entities := bundle.RootEntities
	if source.FlowPath != "." {
		types = bundle.ResolvedTypeCatalogForFlow(source.FlowPath)
		entities = bundle.flowEntities[source.FlowPath]
	}
	names := make([]string, 0, len(types.Scalars)+len(types.Types))
	for name := range types.Scalars {
		names = append(names, name)
	}
	for name := range types.Types {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := validateScopedCatalogReference(types, name, source.Types, "type "+name); err != nil {
			return err
		}
	}
	for _, name := range sortedEntityContractKeys(entities) {
		for _, field := range sortedEntityFieldKeys(entities[name].Fields) {
			if err := validateScopedCatalogReference(types, entities[name].Fields[field].Type, source.Entities, "entity "+name+"."+field); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateEventCatalogReferences(bundle *WorkflowContractBundle) error {
	keys := make([]string, 0, len(bundle.scopedEvents))
	for key := range bundle.scopedEvents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		source := bundle.scopedEventSources[key]
		types := bundle.RootTypeCatalog()
		if source.FlowPath != "" && source.FlowPath != "." {
			types = bundle.ResolvedTypeCatalogForFlow(source.FlowPath)
		}
		fields := bundle.scopedEvents[key].Payload.Properties
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if fields[name].ExactSchema != nil {
				continue
			}
			if err := validateScopedCatalogReference(types, fields[name].Type, source.File, "event "+key+"."+name); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateScopedCatalogReference(types TypeCatalogDocument, ref, file, path string) error {
	if _, err := (CatalogTypeReference{Type: ref, Catalog: types}).Resolve(); err != nil {
		diagnostic, ok := AsLoaderDiagnostic(err)
		if !ok {
			return fmt.Errorf("%s %s: %w", file, path, err)
		}
		out := *diagnostic
		out.Problem = file + ": " + path + ": " + err.Error()
		return out.withLocation(LoaderDiagnosticLocation{File: file, YAMLPath: path})
	}
	return nil
}
