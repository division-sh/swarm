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

func nodeValueBool(value yamlsource.Value, owner string) (bool, error) {
	if value.Presence() != yamlsource.PresenceScalar {
		return false, fmt.Errorf("%s at %s must be a boolean %s, got %s", value.SemanticPath(), value.Location(), owner, value.Presence())
	}
	var out bool
	if err := value.Project(&out); err != nil {
		return false, fmt.Errorf("%s at %s: %w", value.SemanticPath(), value.Location(), err)
	}
	return out, nil
}

func nodeValueStringSequence(value yamlsource.Value, owner string) ([]string, error) {
	if value.Presence() != yamlsource.PresenceSequence && value.Presence() != yamlsource.PresenceEmptySequence {
		return nil, fmt.Errorf("%s at %s must be a sequence of %s, got %s", value.SemanticPath(), value.Location(), owner, value.Presence())
	}
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, err := nodeValueText(item, owner)
		if err != nil {
			return nil, err
		}
		out = append(out, text)
	}
	return out, nil
}

func projectNodeScalarFields(fields map[string]yamlsource.Value, node *SystemNodeContract) error {
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"description", &node.Description},
		{"execution_type", &node.ExecutionType},
		{"state_table", &node.StateTable},
	} {
		value, present := fields[entry.key]
		if !present {
			continue
		}
		text, err := nodeValueText(value, entry.key)
		if err != nil {
			return err
		}
		*entry.target = text
	}
	for _, entry := range []struct {
		key    string
		target *[]string
	}{
		{"subscribes_to", &node.SubscribesTo},
		{"produces", &node.Produces},
	} {
		value, present := fields[entry.key]
		if !present {
			continue
		}
		items, err := nodeValueStringSequence(value, entry.key)
		if err != nil {
			return err
		}
		*entry.target = items
	}
	_, node.ProducesDeclared = fields["produces"]
	return nil
}

var nodeTimerFields = map[string]struct{}{
	"id": {}, "stage": {}, "event": {}, "owner": {}, "action": {},
	"cancellation": {}, "delay": {}, "start_on": {}, "cancel_on": {}, "recurring": {},
}

var retiredNodeTimerFields = map[string]string{
	"delay_seconds": "use delay", "delay_minutes": "use delay",
	"delay_hours": "use delay", "delay_days": "use delay",
}

func projectNodeTimerValue(value yamlsource.Value) (WorkflowTimerContract, error) {
	fields, err := nodeValueFields(value, "timer", nodeTimerFields, retiredNodeTimerFields)
	if err != nil {
		return WorkflowTimerContract{}, err
	}
	var timer WorkflowTimerContract
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"id", &timer.ID}, {"stage", &timer.Stage}, {"event", &timer.Event},
		{"owner", &timer.Owner}, {"action", &timer.Action},
		{"cancellation", &timer.Cancellation}, {"delay", &timer.Delay},
		{"start_on", &timer.StartOn}, {"cancel_on", &timer.CancelOn},
	} {
		value, present := fields[entry.key]
		if !present {
			continue
		}
		text, err := nodeValueText(value, "timer."+entry.key)
		if err != nil {
			return WorkflowTimerContract{}, err
		}
		*entry.target = text
	}
	if value, present := fields["recurring"]; present {
		timer.Recurring, err = nodeValueBool(value, "timer.recurring")
		if err != nil {
			return WorkflowTimerContract{}, err
		}
	}
	return timer, nil
}
