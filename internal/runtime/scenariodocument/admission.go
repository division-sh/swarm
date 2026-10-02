package scenariodocument

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func Admit(raw []byte, label string) (Document, error) {
	root, err := sourceRoot(raw, label)
	if err != nil {
		return Document{}, err
	}
	return admitRoot(root)
}

// Discover classifies resources using the same lexical view as explicit admission.
// Non-scenario fixtures remain resources; selected scenarios still require Admit.
func Discover(raw []byte, label string) (Document, bool, error) {
	root, err := sourceRoot(raw, label)
	if err != nil {
		return Document{}, false, err
	}
	if root.Presence() != yamlsource.PresenceMapping && root.Presence() != yamlsource.PresenceEmptyMapping {
		return Document{}, false, nil
	}
	fields, err := root.Mapping()
	if err != nil {
		return Document{}, false, err
	}
	for _, field := range fields {
		if contains([]string{"version", "steps", "derive", "invalid"}, field.Name) {
			doc, err := admitRoot(root)
			return doc, true, err
		}
	}
	return Document{}, false, nil
}

func AdmitFixture(raw []byte, label string) (semanticvalue.Value, error) {
	root, err := sourceRoot(raw, label)
	if err != nil {
		return semanticvalue.Value{}, err
	}
	value, err := admitValue(root)
	if err != nil {
		return semanticvalue.Value{}, err
	}
	if value.Kind() != semanticvalue.KindObject {
		return semanticvalue.Value{}, fmt.Errorf("%s: payload fixture must be an object", label)
	}
	return value, nil
}

func sourceRoot(raw []byte, label string) (yamlsource.Value, error) {
	snapshot, err := yamlsource.Load(raw)
	if err != nil {
		return yamlsource.Value{}, fmt.Errorf("%s: parse scenario YAML: %w", label, err)
	}
	root := snapshot.Document(label).Root()
	if err := root.ValidateExpansion(); err != nil {
		return yamlsource.Value{}, err
	}
	if err := root.ValidateUniqueMappings(); err != nil {
		return yamlsource.Value{}, err
	}
	return root, nil
}

func admitRoot(root yamlsource.Value) (Document, error) {
	if fields, err := root.Mapping(); err == nil {
		for _, field := range fields {
			if field.Name == "version" {
				return Document{}, fmt.Errorf("%s: remove `version`; the scenario format has one version", field.KeyLocation)
			}
		}
	}
	value, err := admitValue(root)
	if err != nil {
		return Document{}, err
	}
	if _, err := projectDocument(value); err != nil {
		return Document{}, fmt.Errorf("%s at %s: %w", root.SemanticPath(), root.Location(), err)
	}
	return Document{value: value, source: root}, nil
}

func admitValue(value yamlsource.Value) (semanticvalue.Value, error) {
	switch value.Presence() {
	case yamlsource.PresenceNull:
		return semanticvalue.Null(), nil
	case yamlsource.PresenceScalar, yamlsource.PresenceEmptyScalar:
		return admitScalar(value)
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		items, err := value.Sequence()
		if err != nil {
			return semanticvalue.Value{}, err
		}
		out := make([]semanticvalue.Value, 0, len(items))
		for _, item := range items {
			child, err := admitValue(item)
			if err != nil {
				return semanticvalue.Value{}, err
			}
			out = append(out, child)
		}
		return semanticvalue.Array(out), nil
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		fields, err := value.Mapping()
		if err != nil {
			return semanticvalue.Value{}, err
		}
		out := make([]semanticvalue.ObjectEntry, 0, len(fields))
		for _, field := range fields {
			if field.KeyTag != "!!str" {
				return semanticvalue.Value{}, fmt.Errorf("%s: object key %q must be text", field.KeyLocation, field.Name)
			}
			child, err := admitValue(field.Value)
			if err != nil {
				return semanticvalue.Value{}, err
			}
			out = append(out, semanticvalue.ObjectEntry{Name: field.Name, Value: child})
		}
		return semanticvalue.Object(out)
	default:
		return semanticvalue.Value{}, fmt.Errorf("%s at %s is missing", value.SemanticPath(), value.Location())
	}
}

func admitScalar(value yamlsource.Value) (semanticvalue.Value, error) {
	scalar, err := value.Scalar()
	if err != nil {
		return semanticvalue.Value{}, err
	}
	if scalar.Tag == "!!str" {
		return semanticvalue.String(scalar.Value)
	}
	if scalar.Tag != "!!int" && scalar.Tag != "!!float" && scalar.Tag != "!!bool" {
		return semanticvalue.Value{}, fmt.Errorf("%s at %s: scalar tag %q is not semantic JSON", value.SemanticPath(), value.Location(), scalar.Tag)
	}
	out, err := canonicaljson.Decode([]byte(scalar.Value))
	if err != nil {
		return semanticvalue.Value{}, fmt.Errorf("%s at %s: %w", value.SemanticPath(), value.Location(), err)
	}
	return out, nil
}
