package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/yamlsource"
)

type optionalDeclarationRole uint8

const (
	optionalDeclarationAgents optionalDeclarationRole = iota + 1
	optionalDeclarationEntities
	optionalDeclarationEvents
	optionalDeclarationNodes
	optionalDeclarationPolicy
	optionalDeclarationTools
	optionalDeclarationTypes
)

func (r optionalDeclarationRole) fileName() string {
	switch r {
	case optionalDeclarationAgents:
		return "agents.yaml"
	case optionalDeclarationEntities:
		return "entities.yaml"
	case optionalDeclarationEvents:
		return "events.yaml"
	case optionalDeclarationNodes:
		return "nodes.yaml"
	case optionalDeclarationPolicy:
		return "policy.yaml"
	case optionalDeclarationTools:
		return "tools.yaml"
	case optionalDeclarationTypes:
		return "types.yaml"
	default:
		return "optional workflow declaration file"
	}
}

type optionalDeclarationAdmission struct {
	path     string
	role     optionalDeclarationRole
	source   yamlsource.Snapshot
	document yamlsource.Document
}

func admitOptionalDeclarationFile(path string, role optionalDeclarationRole) (optionalDeclarationAdmission, bool, error) {
	if strings.TrimSpace(path) == "" {
		return optionalDeclarationAdmission{}, false, nil
	}
	source, err := yamlsource.LoadFile(path)
	if err != nil {
		if cause, ok := yamlsource.ParseCause(err); ok {
			return optionalDeclarationAdmission{}, false, wrapLoaderDiagnosticFile(cause, path)
		}
		return optionalDeclarationAdmission{}, false, fmt.Errorf("read %s: %w", path, err)
	}
	document := source.Document(path)
	if err := validateOptionalDeclarationDocument(document, role); err != nil {
		return optionalDeclarationAdmission{}, false, wrapLoaderDiagnosticFile(err, path)
	}
	if document.Presence() == yamlsource.PresenceNull {
		return optionalDeclarationAdmission{}, false, wrapLoaderDiagnosticFile(NewOptionalDeclarationFileEmptyDiagnostic(role.fileName()), path)
	}
	return optionalDeclarationAdmission{path: path, role: role, source: source, document: document}, true, nil
}

func (a optionalDeclarationAdmission) RequireLive(declarationCount int) error {
	if declarationCount == 0 {
		return wrapLoaderDiagnosticFile(NewOptionalDeclarationFileEmptyDiagnostic(a.role.fileName()), a.path)
	}
	return nil
}

func loadOptionalAgentDeclarations(path string) (map[string]AgentRegistryEntry, error) {
	admission, present, err := admitOptionalDeclarationFile(path, optionalDeclarationAgents)
	if err != nil || !present {
		return map[string]AgentRegistryEntry{}, err
	}
	entries := map[string]AgentRegistryEntry{}
	if err := admission.source.Decode(&entries); err != nil {
		return nil, wrapLoaderDiagnosticFile(err, path)
	}
	return entries, admission.RequireLive(len(entries))
}

func loadOptionalEntityDeclarations(path string) (EntityContractsDocument, error) {
	admission, present, err := admitOptionalDeclarationFile(path, optionalDeclarationEntities)
	if err != nil || !present {
		return EntityContractsDocument{}, err
	}
	entries, err := projectEntityContractsDocument(admission.document.Root())
	if err != nil {
		return nil, wrapLoaderDiagnosticFile(err, path)
	}
	return entries, admission.RequireLive(len(entries))
}

func loadOptionalNodeDeclarations(path string) (map[string]SystemNodeContract, error) {
	admission, present, err := admitOptionalDeclarationFile(path, optionalDeclarationNodes)
	if err != nil || !present {
		return map[string]SystemNodeContract{}, err
	}
	entries := map[string]SystemNodeContract{}
	if err := admission.source.Decode(&entries); err != nil {
		return nil, wrapLoaderDiagnosticFile(err, path)
	}
	return entries, admission.RequireLive(len(entries))
}

func loadOptionalPolicyDeclarations(path string) (PolicyDocument, error) {
	admission, present, err := admitOptionalDeclarationFile(path, optionalDeclarationPolicy)
	if err != nil || !present {
		return PolicyDocument{Values: map[string]PolicyValue{}}, err
	}
	document, err := flowmodel.ProjectPolicyDocument(admission.document.Root())
	if err != nil {
		return PolicyDocument{}, wrapLoaderDiagnosticFile(err, path)
	}
	count := len(document.Values) + len(document.Criteria) + len(document.Validation) + len(document.Modules)
	return document, admission.RequireLive(count)
}

func loadOptionalToolDeclarations(path string) (map[string]ToolSchemaEntry, error) {
	admission, present, err := admitOptionalDeclarationFile(path, optionalDeclarationTools)
	if err != nil || !present {
		return map[string]ToolSchemaEntry{}, err
	}
	entries := map[string]ToolSchemaEntry{}
	if err := admission.source.Decode(&entries); err != nil {
		return nil, wrapLoaderDiagnosticFile(err, path)
	}
	return entries, admission.RequireLive(len(entries))
}

func loadOptionalTypeDeclarations(path string) (TypeCatalogDocument, error) {
	admission, present, err := admitOptionalDeclarationFile(path, optionalDeclarationTypes)
	if err != nil || !present {
		return TypeCatalogDocument{}, err
	}
	document, err := projectTypeCatalogDocument(admission.document.Root())
	if err != nil {
		return TypeCatalogDocument{}, wrapLoaderDiagnosticFile(err, path)
	}
	count := len(document.Scalars) + len(document.Enums) + len(document.Types)
	return document, admission.RequireLive(count)
}

func validateOptionalDeclarationDocument(document yamlsource.Document, role optionalDeclarationRole) error {
	root := document.Root()
	switch root.Presence() {
	case yamlsource.PresenceNull, yamlsource.PresenceEmptyMapping:
		return nil
	case yamlsource.PresenceMapping:
	default:
		return NewOptionalDeclarationFileShapeDiagnostic(role.fileName(), root.Presence())
	}
	switch role {
	case optionalDeclarationEvents:
		// The W3 event adapter owns its canonical event identity grammar.
		return nil
	case optionalDeclarationTypes:
		return validateContainerDeclarationNames(root, role.fileName(), map[string]struct{}{
			"scalars": {},
			"enums":   {},
			"types":   {},
		})
	case optionalDeclarationPolicy:
		return validateContainerDeclarationNames(root, role.fileName(), map[string]struct{}{
			"criteria":   {},
			"validation": {},
			"modules":    {},
		})
	default:
		fields, err := uniqueYAMLMappingFields(root, role.fileName()+" declarations")
		if err != nil {
			return err
		}
		return validateExactDeclarationNames(fields, role.fileName()+" declaration")
	}
}

func validateContainerDeclarationNames(root yamlsource.Value, fileName string, containers map[string]struct{}) error {
	fields, err := uniqueYAMLMappingFields(root, fileName+" fields")
	if err != nil {
		return err
	}
	if err := validateExactDeclarationNames(fields, fileName+" field"); err != nil {
		return err
	}
	for _, field := range fields {
		if _, ok := containers[field.Name]; !ok {
			continue
		}
		if field.Value.Presence() != yamlsource.PresenceMapping {
			continue
		}
		declarations, err := uniqueYAMLMappingFields(field.Value, fileName+" "+field.Name+" declarations")
		if err != nil {
			return err
		}
		if err := validateExactDeclarationNames(declarations, fileName+" "+field.Name+" declaration"); err != nil {
			return err
		}
	}
	return nil
}

func validateExactDeclarationNames(fields []yamlsource.MappingField, context string) error {
	admitted := make(map[string]yamlsource.Location, len(fields))
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		if previous, exists := admitted[name]; exists {
			return NewDeclarationNameCollisionDiagnostic(context, name, previous, field.KeyLocation)
		}
		admitted[name] = field.KeyLocation
	}
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		if name == "" || name != field.Name {
			return NewDeclarationNameInvalidDiagnostic(context, field.Name, field.KeyLocation)
		}
	}
	return nil
}
