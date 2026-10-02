package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

var flowSchemaDocumentFields = map[string]struct{}{
	"name":                {},
	"activation":          {},
	"ingress":             {},
	"connect":             {},
	"imports":             {},
	"instance":            {},
	"stages":              {},
	"loops":               {},
	"schedules":           {},
	"pins":                {},
	"required_agents":     {},
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
	return decodeStringListValue(yamlsource.ValueFromNode(node))
}

func decodeStringListValue(value yamlsource.Value) ([]string, error) {
	switch value.Presence() {
	case yamlsource.PresenceMissing, yamlsource.PresenceNull:
		return nil, nil
	case yamlsource.PresenceScalar, yamlsource.PresenceEmptyScalar:
		text, err := optionalScalarString(value, "string list")
		if err != nil || text == "" {
			return nil, err
		}
		return []string{text}, nil
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		return optionalStrictStringSequence(value, "string list")
	default:
		return nil, fmt.Errorf("string list at %s is %s, want scalar or sequence", value.Location(), value.Presence())
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
