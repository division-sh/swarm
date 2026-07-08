package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func hasYAMLMappingKey(node *yaml.Node, key string) bool {
	if node == nil || node.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == key {
			return true
		}
	}
	return false
}

func hasAnyYAMLMappingKey(node *yaml.Node, keys ...string) bool {
	for _, key := range keys {
		if hasYAMLMappingKey(node, key) {
			return true
		}
	}
	return false
}

func (d *FlowSchemaDocument) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*d = FlowSchemaDocument{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("flow schema document must be a mapping")
	}
	if err := validateFlowSchemaDocumentFields(node); err != nil {
		return err
	}
	type alias FlowSchemaDocument
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*d = FlowSchemaDocument(aux)
	d.InitialStateDeclared = hasYAMLMappingKey(node, "initial_state")
	d.StatesDeclared = hasYAMLMappingKey(node, "states")
	d.TerminalStatesDeclared = hasYAMLMappingKey(node, "terminal_states")
	d.RequiredAgentsDeclared = hasYAMLMappingKey(node, "required_agents")
	return nil
}

func validateFlowSchemaDocumentFields(node *yaml.Node) error {
	allowed := map[string]struct{}{
		"name":                {},
		"mode":                {},
		"entity":              {},
		"instance":            {},
		"initial_state":       {},
		"terminal_states":     {},
		"states":              {},
		"stages":              {},
		"pins":                {},
		"tool_surface":        {},
		"required_agents":     {},
		"instance_variables":  {},
		"auto_emit_on_create": {},
	}
	retired := map[string]string{
		"namespace_prefix": "schema namespace_prefix is retired; flow namespace is derived from the package tree",
		"namespace_rule":   "schema namespace_rule is retired; namespace override semantics require a separate spec owner",
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if reason, ok := retired[key]; ok {
			return fmt.Errorf("RETIRED: schema field %q is retired; %s", key, reason)
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: schema field %q not in platform spec", key)
		}
	}
	return nil
}

func (n *SystemNodeContract) UnmarshalYAML(node *yaml.Node) error {
	if n == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*n = SystemNodeContract{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("system node contract must be a mapping")
	}
	if err := validateSystemNodeContractFields(node); err != nil {
		return err
	}
	type alias SystemNodeContract
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*n = SystemNodeContract(aux)
	return nil
}

func validateSystemNodeContractFields(node *yaml.Node) error {
	allowed := map[string]struct{}{
		"id":             {},
		"description":    {},
		"execution_type": {},
		"subscribes_to":  {},
		"produces":       {},
		"state_table":    {},
		"timers":         {},
		"event_handlers": {},
		"state_schema":   {},
		"gate_state":     {},
	}
	retired := map[string]string{
		"permissions":       "node permissions are not public node YAML authority",
		"implementation":    "executor binding is not public node YAML authority",
		"owned_transitions": "transition ownership is expressed through event owning_node and event_handlers",
		"idempotency_table": "node idempotency table semantics are not public node YAML authority",
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if reason, ok := retired[key]; ok {
			return fmt.Errorf("RETIRED: node field %q is retired; %s", key, reason)
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: node field %q not in platform spec", key)
		}
	}
	return nil
}

func decodeStringListNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return nil, nil
		}
		return []string{strings.TrimSpace(node.Value)}, nil
	case yaml.SequenceNode:
		var values []string
		if err := node.Decode(&values); err != nil {
			return nil, err
		}
		return normalizeStrings(values), nil
	default:
		return nil, fmt.Errorf("unsupported string list yaml node kind %d", node.Kind)
	}
}

func decodeScalarStringNode(node *yaml.Node) (string, error) {
	if node == nil || node.Kind == 0 {
		return "", nil
	}
	if node.Kind != yaml.ScalarNode {
		return "", fmt.Errorf("unsupported scalar string yaml node kind %d", node.Kind)
	}
	if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return "", nil
	}
	return strings.TrimSpace(node.Value), nil
}

func decodeBoolNode(node *yaml.Node) (bool, error) {
	if node == nil || node.Kind == 0 {
		return false, nil
	}
	if node.Kind != yaml.ScalarNode {
		return false, fmt.Errorf("unsupported bool yaml node kind %d", node.Kind)
	}
	if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
		return false, nil
	}
	var value bool
	if err := node.Decode(&value); err == nil {
		return value, nil
	}
	switch strings.ToLower(strings.TrimSpace(node.Value)) {
	case "true", "yes", "on", "conditional":
		return true, nil
	case "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("unsupported bool value %q", node.Value)
	}
}
