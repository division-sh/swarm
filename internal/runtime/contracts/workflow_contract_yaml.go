package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func hasYAMLMappingKey(node *yaml.Node, key string) bool {
	return yamlMappingValue(node, key) != nil
}

func hasAnyYAMLMappingKey(node *yaml.Node, keys ...string) bool {
	for _, key := range keys {
		if hasYAMLMappingKey(node, key) {
			return true
		}
	}
	return false
}

var flowSchemaDocumentFields = map[string]struct{}{
	"name":                {},
	"activation":          {},
	"ingress":             {},
	"connect":             {},
	"imports":             {},
	"instance":            {},
	"stages":              {},
	"loops":               {},
	"pins":                {},
	"required_agents":     {},
	"instance_variables":  {},
	"auto_emit_on_create": {},
}

var systemNodeContractFields = map[string]struct{}{
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
