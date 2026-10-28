package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// R18 gives every flow's state the same internal and persisted identity.
const stateEntityType = "state"

func projectStateDeclarations(root yamlsource.Value, types TypeCatalogDocument) (EntityContractsDocument, error) {
	state, err := projectStateContract(root, types)
	if err != nil {
		return nil, err
	}
	return EntityContractsDocument{stateEntityType: state}, nil
}

// State syntax owns fields, not an authored entity-type or permission coordinate.
func projectStateContract(root yamlsource.Value, types TypeCatalogDocument) (EntityContract, error) {
	fields, err := uniqueYAMLMappingFields(root, "state.yaml")
	if err != nil {
		return EntityContract{}, err
	}
	state := EntityContract{Fields: map[string]EntityFieldDecl{}}
	for _, field := range fields {
		if field.Name == "" || field.Name != strings.TrimSpace(field.Name) {
			return EntityContract{}, fmt.Errorf("state field %q must be nonempty without surrounding whitespace", field.Name)
		}
		declaration, err := projectStateField(field.Value, types)
		if err != nil {
			if diagnostic, ok := AsLoaderDiagnostic(err); ok {
				location := field.Value.IntroductionLocation()
				out := *diagnostic
				out.Problem = fmt.Sprintf("%s: state.%s: %s", location.File, field.Name, err.Error())
				return EntityContract{}, out.withLocation(LoaderDiagnosticLocation{
					File: location.File, YAMLPath: field.Value.SemanticPath(), Line: location.Line, Column: location.Column,
				})
			}
			return EntityContract{}, fmt.Errorf("state.%s at %s: %w", field.Name, field.IntroductionLocation(), nodeValueError(field.Value, err))
		}
		state.Fields[field.Name] = declaration
	}
	return state, nil
}

func projectStateField(value yamlsource.Value, types TypeCatalogDocument) (EntityFieldDecl, error) {
	switch value.Presence() {
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		return projectIndexedStateField(value, types)
	case yamlsource.PresenceSequence:
		return projectStateList(value, types)
	case yamlsource.PresenceScalar, yamlsource.PresenceEmptyScalar:
		scalar, err := value.Scalar()
		if err != nil {
			return EntityFieldDecl{}, err
		}
		if scalar.Tag == "!!str" && scalar.Style == 0 {
			return stateTypeDeclaration(scalar.Value, types)
		}
		return stateLiteral(value)
	default:
		return EntityFieldDecl{}, fmt.Errorf("state field requires a type or a non-null literal; empty lists are not declarations (use [T])")
	}
}

func projectIndexedStateField(value yamlsource.Value, types TypeCatalogDocument) (EntityFieldDecl, error) {
	fields, err := uniqueYAMLMappingFields(value, "indexed state field")
	if err != nil {
		return EntityFieldDecl{}, err
	}
	if err := rejectLongListField(fields, "state field"); err != nil {
		return EntityFieldDecl{}, err
	}
	var typeValue yamlsource.Value
	indexed := false
	for _, field := range fields {
		switch field.Name {
		case "type":
			typeValue = field.Value
		case "indexed":
			if scalar, err := field.Value.Scalar(); err != nil || scalar.Tag != "!!bool" || scalar.Value != "true" {
				return EntityFieldDecl{}, fmt.Errorf("the indexed mapping requires indexed: true")
			}
			indexed = true
		default:
			return EntityFieldDecl{}, NewUndefinedFieldDiagnostic("state field", field.Name, map[string]struct{}{"type": {}, "indexed": {}}, field)
		}
	}
	if !indexed || typeValue.Presence() == yamlsource.PresenceMissing {
		return EntityFieldDecl{}, fmt.Errorf("the only state field mapping is {type: T, indexed: true}")
	}
	if typeValue.Presence() == yamlsource.PresenceMapping || typeValue.Presence() == yamlsource.PresenceEmptyMapping {
		return EntityFieldDecl{}, fmt.Errorf("indexed type must be a type name or [T], not a mapping")
	}
	var typeRef string
	if typeValue.Presence() == yamlsource.PresenceSequence || typeValue.Presence() == yamlsource.PresenceEmptySequence {
		typeRef, err = listDeclarationSequence(typeValue, "state field")
	} else {
		typeRef, err = listDeclarationScalar(typeValue, "state field", true)
	}
	if err != nil {
		return EntityFieldDecl{}, err
	}
	declaration, err := stateTypeDeclaration(typeRef, types)
	if err != nil {
		return EntityFieldDecl{}, err
	}
	declaration.Indexed = true
	return declaration, nil
}

func projectStateList(value yamlsource.Value, types TypeCatalogDocument) (EntityFieldDecl, error) {
	items, err := value.Sequence()
	if err != nil {
		return EntityFieldDecl{}, err
	}
	if len(items) == 1 {
		if items[0].Presence() == yamlsource.PresenceSequence || items[0].Presence() == yamlsource.PresenceEmptySequence {
			typeRef, err := listDeclarationSequence(value, "state field")
			if err != nil {
				return EntityFieldDecl{}, err
			}
			return stateTypeDeclaration(typeRef, types)
		}
		if token, err := items[0].Scalar(); err == nil && token.Tag == "!!str" && token.Style == 0 {
			declaration, err := stateTypeDeclaration("["+token.Value+"]", types)
			if err != nil {
				return EntityFieldDecl{}, err
			}
			return declaration, nil
		}
	}
	var itemType string
	initial := make([]any, 0, len(items))
	for _, item := range items {
		declaration, err := stateLiteral(item)
		if err != nil {
			return EntityFieldDecl{}, err
		}
		if itemType != "" && itemType != declaration.Type {
			return EntityFieldDecl{}, fmt.Errorf("list initial values must have one homogeneous scalar type")
		}
		itemType = declaration.Type
		initial = append(initial, declaration.Initial)
	}
	return EntityFieldDecl{Type: "[" + itemType + "]", Initial: initial}, nil
}

func stateTypeDeclaration(raw string, types TypeCatalogDocument) (EntityFieldDecl, error) {
	typeName, optional, err := admitEventFieldTypeMarker(raw, "state field")
	if err == nil {
		err = validateWave1TypeRef(typeName, "state field")
	}
	if err != nil {
		return EntityFieldDecl{}, err
	}
	if _, err := (CatalogTypeReference{Type: typeName, Catalog: types}).Resolve(); err != nil {
		return EntityFieldDecl{}, fmt.Errorf("%q is not a declared type; for a text initial value write %q: %w", raw, `"`+raw+`"`, err)
	}
	return EntityFieldDecl{Type: typeName, IsOptional: optional}, nil
}

func stateLiteral(value yamlsource.Value) (EntityFieldDecl, error) {
	scalar, err := value.Scalar()
	if err != nil {
		return EntityFieldDecl{}, fmt.Errorf("state initials must be scalar literals: %w", err)
	}
	var typeName string
	switch scalar.Tag {
	case "!!str":
		if scalar.Style == 0 {
			return EntityFieldDecl{}, fmt.Errorf("text initial %q must be quoted", scalar.Value)
		}
		typeName = "text"
	case "!!int":
		typeName = "integer"
	case "!!float":
		typeName = "numeric"
	case "!!bool":
		typeName = "boolean"
	default:
		return EntityFieldDecl{}, fmt.Errorf("state initial must be quoted text, a number, or a boolean")
	}
	var initial any
	if err := value.Project(&initial); err != nil {
		return EntityFieldDecl{}, err
	}
	return EntityFieldDecl{Type: typeName, Initial: initial}, nil
}
