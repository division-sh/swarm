package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

var flowSchemaDocumentFields = map[string]struct{}{
	"connect":             {},
	"instance":            {},
	"stages":              {},
	"loops":               {},
	"schedules":           {},
	"inputs":              {},
	"outputs":             {},
	"auto_emit_on_create": {},
}

var systemNodeContractFields = map[string]struct{}{
	"timers": {},
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
