package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func validateCatalogTypeNamespace(bundle *WorkflowContractBundle, source FlowSource, types TypeCatalogDocument) error {
	seen := make(map[string]string, len(builtinWave1ScalarTypes))
	for name := range builtinWave1ScalarTypes {
		seen[name] = "built-in scalar"
	}
	for _, category := range []struct {
		name string
		keys []string
	}{
		{"scalars", sortedContractKeys(types.Scalars)},
		{"enums", sortedContractKeys(types.Enums)},
		{"types", sortedContractKeys(types.Types)},
	} {
		for _, name := range category.keys {
			if previous, ok := seen[name]; ok {
				return catalogTypeNameCollision(bundle, source, name, previous, category.name)
			}
			seen[name] = category.name
		}
	}
	return nil
}

func catalogTypeNameCollision(bundle *WorkflowContractBundle, source FlowSource, name, first, second string) error {
	firstLocation, err := catalogTypeDeclarationLocation(bundle, source, first, name)
	if err != nil {
		return err
	}
	secondLocation, err := catalogTypeDeclarationLocation(bundle, source, second, name)
	if err != nil {
		return err
	}
	diagnostic := NewDeclarationNameCollisionDiagnostic("type catalog", name, firstLocation, secondLocation)
	diagnostic.Problem = fmt.Sprintf("TYPE-NAME-COLLISION: %s and %s use one name. A name identifies one declaration per catalog. Rename one of them.",
		catalogTypeDeclarationDescription(first, name, firstLocation), catalogTypeDeclarationDescription(second, name, secondLocation))
	diagnostic.Location.YAMLPath = name
	return diagnostic
}

func catalogTypeDeclarationLocation(bundle *WorkflowContractBundle, source FlowSource, category, name string) (yamlsource.Location, error) {
	if category == "built-in scalar" {
		return yamlsource.Location{}, nil
	}
	for _, file := range []string{source.Types, bundle.FlowSources["."].Types} {
		document, ok := bundle.SourceArtifact.YAML(file)
		if !ok {
			continue
		}
		declaration, err := document.Root().Lookup(name)
		if err != nil {
			return yamlsource.Location{}, err
		}
		if declaration.Presence == yamlsource.PresenceMissing {
			continue
		}
		switch category {
		case "enums":
			if declaration.Presence != yamlsource.PresenceSequence && declaration.Presence != yamlsource.PresenceEmptySequence {
				continue
			}
		case "types":
			if declaration.Presence != yamlsource.PresenceMapping && declaration.Presence != yamlsource.PresenceEmptyMapping {
				continue
			}
		}
		if len(declaration.Occurrences) > 0 {
			return declaration.Occurrences[0].KeyLocation, nil
		}
	}
	return yamlsource.Location{}, fmt.Errorf("type catalog declaration %s has no admitted source location", name)
}

func catalogTypeDeclarationDescription(category, name string, location yamlsource.Location) string {
	switch category {
	case "built-in scalar":
		return fmt.Sprintf("built-in scalar %q", name)
	case "scalars":
		category = "scalar alias"
	case "enums":
		category = "enum"
	case "types":
		category = "named type"
	}
	return fmt.Sprintf("%s %q at %s", category, name, location)
}
