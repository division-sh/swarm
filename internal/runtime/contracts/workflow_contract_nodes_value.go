package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

var guardOnFailFieldOptions = map[string]struct{}{"escalate": {}}

var guardOnFailEscalateFieldOptions = map[string]struct{}{
	"event": {}, "from": {}, "fields": {},
}

var accumulateFieldOptions = map[string]struct{}{
	"into": {}, "from": {}, "description": {}, "key": {},
}

var fanOutFieldOptions = map[string]struct{}{
	"items_from": {}, "as": {}, "identity": {}, "max_items": {}, "emit": {},
}

func nodeValueError(value yamlsource.Value, err error) error {
	return fmt.Errorf("%s at %s (introduced at %s, resolved at %s): %w", value.SemanticPath(), value.Location(), value.IntroductionLocation(), value.ResolvedLocation(), err)
}

// Callers choose required-on-presence fields; omission remains a separate state.
func nodeValueTexts(fields map[string]yamlsource.Value, targets map[string]*string, trim bool, nonempty ...string) error {
	for _, key := range sortedContractKeys(targets) {
		field, present := fields[key]
		if !present {
			continue
		}
		text, err := nodeValueText(field, key)
		for _, required := range nonempty {
			if key == required {
				text, err = nodeValueRequiredText(field, key)
			}
		}
		if err != nil {
			return err
		}
		if trim {
			text = strings.TrimSpace(text)
		}
		*targets[key] = text
	}
	return nil
}

// nodeValueFields admits one node-family mapping without losing the authored
// location of aliases, merges, or duplicate effective fields.
func nodeValueFields(value yamlsource.Value, owner string, allowed map[string]struct{}, retired map[string]string) (map[string]yamlsource.Value, error) {
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return nil, fmt.Errorf("%s at %s must be a mapping, got %s", value.SemanticPath(), value.Location(), value.Presence())
	}
	fields, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]yamlsource.Value, len(fields))
	seen := make(map[string]yamlsource.Location, len(fields))
	for _, field := range fields {
		if previous, exists := seen[field.Name]; exists {
			return nil, fmt.Errorf("duplicate effective YAML key %q at %s and %s for %s", field.Name, previous, field.KeyLocation, field.Value.SemanticPath())
		}
		seen[field.Name] = field.KeyLocation
		if err := retiredHandlerActionFieldError(owner, field.Name); err != nil {
			return nil, fmt.Errorf("%w at %s", err, field.IntroductionLocation())
		}
		if reason, ok := retired[field.Name]; ok {
			return nil, fmt.Errorf("RETIRED: %s field %q at %s: %s", owner, field.Name, field.IntroductionLocation(), reason)
		}
		if _, ok := allowed[field.Name]; !ok {
			diagnostic := NewUndefinedFieldDiagnostic(owner, field.Name, allowed)
			location := field.IntroductionLocation()
			diagnostic.Location = LoaderDiagnosticLocation{File: location.File, YAMLPath: field.Value.SemanticPath(), Line: location.Line, Column: location.Column}
			return nil, fmt.Errorf("%s at %s: %w", field.Value.SemanticPath(), field.IntroductionLocation(), diagnostic)
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
	if err := nodeValueTexts(fields, map[string]*string{
		"description": &node.Description, "execution_type": &node.ExecutionType,
		"state_table": &node.StateTable,
	}, false); err != nil {
		return err
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
	if err := nodeValueTexts(fields, map[string]*string{
		"id": &timer.ID, "stage": &timer.Stage, "event": &timer.Event,
		"owner": &timer.Owner, "action": &timer.Action,
		"cancellation": &timer.Cancellation, "delay": &timer.Delay,
		"start_on": &timer.StartOn, "cancel_on": &timer.CancelOn,
	}, false); err != nil {
		return WorkflowTimerContract{}, err
	}
	if value, present := fields["recurring"]; present {
		timer.Recurring, err = nodeValueBool(value, "timer.recurring")
		if err != nil {
			return WorkflowTimerContract{}, err
		}
	}
	return timer, nil
}

func projectNodeEmitValue(value yamlsource.Value) (EmitSpec, error) {
	switch value.Presence() {
	case yamlsource.PresenceNull:
		return EmitSpec{}, nil
	case yamlsource.PresenceScalar, yamlsource.PresenceEmptyScalar:
		event, err := nodeValueText(value, "emit event")
		if err != nil {
			return EmitSpec{}, err
		}
		return EmitSpec{Event: strings.TrimSpace(event)}, nil
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		fields, err := nodeValueFields(value, "emit", map[string]struct{}{
			"event": {}, "from": {}, "fields": {},
		}, map[string]string{
			"target":    "RETIRED-EMIT-ROUTING: emit.target; use declared connect or accepted output pins",
			"broadcast": "RETIRED-EMIT-ROUTING: emit.broadcast; use declared connect or accepted output pins",
		})
		if err != nil {
			return EmitSpec{}, err
		}
		return projectNodeEmitFields(fields, "emit")
	default:
		return EmitSpec{}, fmt.Errorf("emit at %s must be a scalar or mapping, got %s", value.Location(), value.Presence())
	}
}

func projectNodeEmitFields(fields map[string]yamlsource.Value, owner string) (EmitSpec, error) {
	var out EmitSpec
	var err error
	if err := nodeValueTexts(fields, map[string]*string{"event": &out.Event, "from": &out.From}, true); err != nil {
		return EmitSpec{}, err
	}
	if err := validateEmitFromSource(out.From); err != nil {
		return EmitSpec{}, err
	}
	if payload, present := fields["fields"]; present {
		out.Fields, err = projectNodeExpressionFields(payload, owner+".fields")
		if err != nil {
			return EmitSpec{}, err
		}
	}
	return out, nil
}

func projectNodeOnSuccessValue(value yamlsource.Value) (HandlerOnSuccessSpec, error) {
	if value.Presence() == yamlsource.PresenceNull {
		return HandlerOnSuccessSpec{}, nil
	}
	fields, err := nodeValueFields(value, "on_success", onSuccessFieldOptions, map[string]string{
		"action": "authored actions are retired",
	})
	if err != nil {
		return HandlerOnSuccessSpec{}, err
	}
	var out HandlerOnSuccessSpec
	if emit, present := fields["emit"]; present {
		out.Emit, err = projectNodeEmitValue(emit)
		if err != nil {
			return HandlerOnSuccessSpec{}, err
		}
	}
	if !out.Empty() && out.Emit.EventType() == "" {
		return HandlerOnSuccessSpec{}, fmt.Errorf("INVALID-EMIT: on_success.emit.event is required at %s", value.Location())
	}
	return out, nil
}
