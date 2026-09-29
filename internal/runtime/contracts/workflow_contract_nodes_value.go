package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// nodeValueFields admits one node-family mapping without losing the authored
// location of aliases, merges, or duplicate effective fields.
func nodeValueFields(value yamlsource.Value, owner string, allowed map[string]struct{}, retired map[string]string) (map[string]yamlsource.Value, error) {
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return nil, fmt.Errorf("%s at %s must be a mapping, got %s", value.SemanticPath(), value.Location(), value.Presence())
	}
	if err := value.ValidateUniqueMappings(); err != nil {
		return nil, err
	}
	fields, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]yamlsource.Value, len(fields))
	for _, field := range fields {
		if reason, ok := retired[field.Name]; ok {
			return nil, fmt.Errorf("RETIRED: %s field %q at %s: %s", owner, field.Name, field.IntroductionLocation(), reason)
		}
		if _, ok := allowed[field.Name]; !ok {
			return nil, fmt.Errorf("%s at %s: %w", field.Value.SemanticPath(), field.IntroductionLocation(), NewUndefinedFieldDiagnostic(owner, field.Name, allowed))
		}
		out[field.Name] = field.Value
	}
	return out, nil
}

func nodeValueText(value yamlsource.Value, owner string) (string, error) {
	if value.Presence() != yamlsource.PresenceScalar && value.Presence() != yamlsource.PresenceEmptyScalar {
		return "", fmt.Errorf("%s at %s must be a scalar %s, got %s", value.SemanticPath(), value.Location(), owner, value.Presence())
	}
	scalar, err := value.Scalar()
	if err != nil {
		return "", err
	}
	return scalar.Value, nil
}

func nodeValueRequiredText(value yamlsource.Value, owner string) (string, error) {
	text, err := nodeValueText(value, owner)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("%s at %s requires a non-empty %s", value.SemanticPath(), value.Location(), owner)
	}
	return text, nil
}
