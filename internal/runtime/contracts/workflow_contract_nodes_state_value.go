package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeStateSchemaValue(value yamlsource.Value) (NodeStateSchema, error) {
	if value.Presence() == yamlsource.PresenceNull {
		return NodeStateSchema{}, nil
	}
	fields, err := nodeValueFields(value, "state_schema", map[string]struct{}{
		"description": {}, "fields": {},
	}, nil)
	if err != nil {
		return NodeStateSchema{}, err
	}
	var out NodeStateSchema
	if description, present := fields["description"]; present {
		out.Description, err = nodeValueText(description, "state_schema.description")
		if err != nil {
			return NodeStateSchema{}, err
		}
		out.Description = strings.TrimSpace(out.Description)
	}
	if values, present := fields["fields"]; present {
		out.Fields, err = projectNodeStateFieldsValue(values)
		if err != nil {
			return NodeStateSchema{}, err
		}
	}
	return out, nil
}

func projectNodeStateFieldsValue(value yamlsource.Value) ([]NodeStateField, error) {
	switch value.Presence() {
	case yamlsource.PresenceMissing:
		return nil, nil
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		items, err := value.Sequence()
		if err != nil {
			return nil, err
		}
		out := make([]NodeStateField, 0, len(items))
		for _, item := range items {
			field, err := projectNodeStateFieldValue("", item)
			if err != nil {
				return nil, err
			}
			out = append(out, field)
		}
		return out, nil
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		if err := value.ValidateUniqueMappings(); err != nil {
			return nil, err
		}
		fields, err := value.Mapping()
		if err != nil {
			return nil, err
		}
		out := make([]NodeStateField, 0, len(fields))
		for _, entry := range fields {
			name := strings.TrimSpace(entry.Name)
			if name == "" || name != entry.Name {
				return nil, fmt.Errorf("state field name %q at %s is invalid", entry.Name, entry.KeyLocation)
			}
			field, err := projectNodeStateFieldValue(name, entry.Value)
			if err != nil {
				return nil, err
			}
			out = append(out, field)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("node state fields at %s must be a sequence or mapping, got %s", value.Location(), value.Presence())
	}
}

func projectNodeStateFieldValue(name string, value yamlsource.Value) (NodeStateField, error) {
	field := NodeStateField{Name: strings.TrimSpace(name)}
	switch value.Presence() {
	case yamlsource.PresenceScalar, yamlsource.PresenceEmptyScalar:
		if field.Name == "" {
			return NodeStateField{}, fmt.Errorf("state field at %s requires a name", value.Location())
		}
		text, err := nodeValueText(value, "state field type")
		if err != nil {
			return NodeStateField{}, err
		}
		field.Type = text
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		fields, err := nodeValueFields(value, "state field", map[string]struct{}{
			"name": {}, "type": {}, "default": {},
		}, nil)
		if err != nil {
			return NodeStateField{}, err
		}
		if named, present := fields["name"]; present && name == "" {
			field.Name, err = nodeValueText(named, "state field name")
			if err != nil {
				return NodeStateField{}, err
			}
			field.Name = strings.TrimSpace(field.Name)
		} else if present && name != "" {
			return NodeStateField{}, fmt.Errorf("state field %q at %s cannot reauthor its map-key name", name, named.Location())
		}
		if typed, present := fields["type"]; present {
			field.Type, err = nodeValueText(typed, "state field type")
			if err != nil {
				return NodeStateField{}, err
			}
		}
		if initial, present := fields["default"]; present {
			if err := initial.Project(&field.Default); err != nil {
				return NodeStateField{}, fmt.Errorf("state field default at %s: %w", initial.Location(), err)
			}
		}
	default:
		return NodeStateField{}, fmt.Errorf("state field at %s must be a scalar or mapping, got %s", value.Location(), value.Presence())
	}
	if field.Name == "" {
		return NodeStateField{}, fmt.Errorf("state field at %s requires a name", value.Location())
	}
	normalized, err := NormalizeNodeStateFieldType(field.Type)
	if err != nil {
		return NodeStateField{}, fmt.Errorf("node state field %s at %s: %w", field.Name, value.Location(), err)
	}
	field.Type = normalized
	return field, nil
}

func projectNodeGateStateValue(value yamlsource.Value) (NodeGateStateSchema, error) {
	if value.Presence() == yamlsource.PresenceNull {
		return NodeGateStateSchema{}, nil
	}
	if value.Presence() == yamlsource.PresenceSequence || value.Presence() == yamlsource.PresenceEmptySequence {
		gates, err := projectNodeGateFieldsValue(value)
		return NodeGateStateSchema{Gates: gates}, err
	}
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return NodeGateStateSchema{}, fmt.Errorf("gate_state at %s must be a sequence or mapping, got %s", value.Location(), value.Presence())
	}
	fields, err := value.Mapping()
	if err != nil {
		return NodeGateStateSchema{}, err
	}
	wrapped := false
	for _, field := range fields {
		if field.Name == "description" || field.Name == "gates" || field.Name == "storage" {
			wrapped = true
			break
		}
	}
	if !wrapped {
		gates, err := projectNodeGateFieldsValue(value)
		return NodeGateStateSchema{Gates: gates}, err
	}
	parts, err := nodeValueFields(value, "gate_state", map[string]struct{}{
		"description": {}, "gates": {}, "storage": {},
	}, nil)
	if err != nil {
		return NodeGateStateSchema{}, err
	}
	var out NodeGateStateSchema
	if description, present := parts["description"]; present {
		out.Description, err = nodeValueText(description, "gate_state.description")
		if err != nil {
			return NodeGateStateSchema{}, err
		}
		out.Description = strings.TrimSpace(out.Description)
	}
	if storage, present := parts["storage"]; present {
		out.Storage, err = nodeValueText(storage, "gate_state.storage")
		if err != nil {
			return NodeGateStateSchema{}, err
		}
		out.Storage = strings.TrimSpace(out.Storage)
	}
	if gates, present := parts["gates"]; present {
		out.Gates, err = projectNodeGateFieldsValue(gates)
		if err != nil {
			return NodeGateStateSchema{}, err
		}
	}
	return out, nil
}

func projectNodeGateFieldsValue(value yamlsource.Value) ([]NodeGateField, error) {
	switch value.Presence() {
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		items, err := value.Sequence()
		if err != nil {
			return nil, err
		}
		out := make([]NodeGateField, 0, len(items))
		for _, item := range items {
			gate, err := projectNodeGateFieldValue("", item)
			if err != nil {
				return nil, err
			}
			out = append(out, gate)
		}
		return out, nil
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		if err := value.ValidateUniqueMappings(); err != nil {
			return nil, err
		}
		fields, err := value.Mapping()
		if err != nil {
			return nil, err
		}
		out := make([]NodeGateField, 0, len(fields))
		for _, field := range fields {
			gate, err := projectNodeGateFieldValue(field.Name, field.Value)
			if err != nil {
				return nil, err
			}
			out = append(out, gate)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("gate fields at %s must be a sequence or mapping, got %s", value.Location(), value.Presence())
	}
}

func projectNodeGateFieldValue(name string, value yamlsource.Value) (NodeGateField, error) {
	gate := NodeGateField{Name: strings.TrimSpace(name)}
	switch value.Presence() {
	case yamlsource.PresenceScalar, yamlsource.PresenceEmptyScalar:
		text, err := nodeValueText(value, "gate description")
		if err != nil {
			return NodeGateField{}, err
		}
		if gate.Name == "" {
			gate.Name = strings.TrimSpace(text)
		} else {
			gate.Description = strings.TrimSpace(text)
		}
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		fields, err := nodeValueFields(value, "gate field", map[string]struct{}{
			"name": {}, "description": {},
		}, nil)
		if err != nil {
			return NodeGateField{}, err
		}
		if authored, present := fields["name"]; present {
			gate.Name, err = nodeValueText(authored, "gate name")
			if err != nil {
				return NodeGateField{}, err
			}
			gate.Name = strings.TrimSpace(gate.Name)
		}
		if description, present := fields["description"]; present {
			gate.Description, err = nodeValueText(description, "gate description")
			if err != nil {
				return NodeGateField{}, err
			}
			gate.Description = strings.TrimSpace(gate.Description)
		}
	default:
		return NodeGateField{}, fmt.Errorf("gate field at %s must be scalar or mapping, got %s", value.Location(), value.Presence())
	}
	if gate.Name == "" {
		return NodeGateField{}, fmt.Errorf("gate field at %s requires a name", value.Location())
	}
	return gate, nil
}
