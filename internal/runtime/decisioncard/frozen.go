package decisioncard

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

type FrozenOutcome struct {
	Verdict string
	Label   string
	Input   map[string]runtimecontracts.WorkflowGateInputField
}

func FreezeSnapshot(decision, title string, context map[string]any, outcomes map[string]runtimecontracts.WorkflowGateOutcomePlan) (Snapshot, error) {
	if context == nil {
		context = map[string]any{}
	}
	contextValue, err := canonicaljson.FromGo(context)
	if err != nil {
		return Snapshot{}, fmt.Errorf("admit decision card context: %w", err)
	}
	if contextValue.Kind() != semanticvalue.KindObject {
		return Snapshot{}, fmt.Errorf("decision card context must be an object")
	}
	frozen := make(map[string]FrozenOutcome, len(outcomes))
	for verdict, outcome := range outcomes {
		frozen[verdict] = FrozenOutcome{Verdict: outcome.Verdict, Label: outcome.Label, Input: cloneGateInputs(outcome.Input)}
	}
	return Snapshot{Decision: decision, Title: title, Context: contextValue, Outcomes: frozen}, nil
}

func cloneGateInputs(input map[string]runtimecontracts.WorkflowGateInputField) map[string]runtimecontracts.WorkflowGateInputField {
	out := make(map[string]runtimecontracts.WorkflowGateInputField, len(input))
	for name, field := range input {
		out[name] = field
	}
	return out
}

func (s Snapshot) SemanticValue() (semanticvalue.Value, error) {
	outcomes := make(map[string]semanticvalue.Value, len(s.Outcomes))
	for verdict, outcome := range s.Outcomes {
		value, err := outcome.semanticValue()
		if err != nil {
			return semanticvalue.Value{}, fmt.Errorf("encode decision card outcome %s: %w", verdict, err)
		}
		outcomes[verdict] = value
	}
	outcomesValue, err := semanticvalue.ObjectFromMap(outcomes)
	if err != nil {
		return semanticvalue.Value{}, err
	}
	return semanticObjectWithText(
		map[string]string{"decision": s.Decision, "title": s.Title},
		map[string]semanticvalue.Value{"context": s.Context, "outcomes": outcomesValue},
	)
}

func (o FrozenOutcome) semanticValue() (semanticvalue.Value, error) {
	inputs := make(map[string]semanticvalue.Value, len(o.Input))
	for name, input := range o.Input {
		encoded, err := semanticObjectWithText(
			map[string]string{"type": input.Type, "label": input.Label},
			map[string]semanticvalue.Value{"required": semanticvalue.Bool(input.Required)},
		)
		if err != nil {
			return semanticvalue.Value{}, fmt.Errorf("encode input %s: %w", name, err)
		}
		inputs[name] = encoded
	}
	inputValue, err := semanticvalue.ObjectFromMap(inputs)
	if err != nil {
		return semanticvalue.Value{}, err
	}
	return semanticObjectWithText(
		map[string]string{"Verdict": o.Verdict, "Label": o.Label},
		map[string]semanticvalue.Value{"Input": inputValue},
	)
}

func semanticObjectWithText(textFields map[string]string, valueFields map[string]semanticvalue.Value) (semanticvalue.Value, error) {
	entries := make([]semanticvalue.ObjectEntry, 0, len(textFields)+len(valueFields))
	for name, text := range textFields {
		value, err := semanticvalue.String(text)
		if err != nil {
			return semanticvalue.Value{}, fmt.Errorf("%s: %w", name, err)
		}
		entries = append(entries, semanticvalue.ObjectEntry{Name: name, Value: value})
	}
	for name, value := range valueFields {
		entries = append(entries, semanticvalue.ObjectEntry{Name: name, Value: value})
	}
	return semanticvalue.Object(entries)
}

func snapshotFromSemanticValue(value semanticvalue.Value) (Snapshot, error) {
	root, ok := value.ObjectMap()
	if !ok {
		return Snapshot{}, fmt.Errorf("decision card snapshot must be an object")
	}
	if err := requireExactSemanticFields(root, "decision card snapshot", "decision", "title", "context", "outcomes"); err != nil {
		return Snapshot{}, err
	}
	decision, err := requiredSemanticString(root, "decision")
	if err != nil {
		return Snapshot{}, err
	}
	title, err := requiredSemanticString(root, "title")
	if err != nil {
		return Snapshot{}, err
	}
	contextValue, ok := root["context"]
	if !ok || contextValue.Kind() != semanticvalue.KindObject {
		return Snapshot{}, fmt.Errorf("decision card snapshot context must be an object")
	}
	outcomeValue, ok := root["outcomes"]
	if !ok {
		return Snapshot{}, fmt.Errorf("decision card snapshot outcomes are required")
	}
	outcomeMembers, ok := outcomeValue.ObjectMap()
	if !ok {
		return Snapshot{}, fmt.Errorf("decision card snapshot outcomes must be an object")
	}
	outcomes := make(map[string]FrozenOutcome, len(outcomeMembers))
	for verdict, encoded := range outcomeMembers {
		outcome, err := frozenOutcomeFromSemanticValue(encoded)
		if err != nil {
			return Snapshot{}, fmt.Errorf("decode decision card outcome %s: %w", verdict, err)
		}
		outcomes[verdict] = outcome
	}
	snapshot := Snapshot{Decision: decision, Title: title, Context: contextValue, Outcomes: outcomes}
	projected, err := snapshot.SemanticValue()
	if err != nil {
		return Snapshot{}, fmt.Errorf("re-encode decoded decision card snapshot: %w", err)
	}
	if !projected.Equal(value) {
		return Snapshot{}, fmt.Errorf("decision card snapshot typed projection does not preserve its exact semantic value")
	}
	return snapshot, nil
}

func frozenOutcomeFromSemanticValue(value semanticvalue.Value) (FrozenOutcome, error) {
	root, ok := value.ObjectMap()
	if !ok {
		return FrozenOutcome{}, fmt.Errorf("outcome must be an object")
	}
	if err := requireExactSemanticFields(root, "outcome", "Verdict", "Label", "Input"); err != nil {
		return FrozenOutcome{}, err
	}
	verdict, err := requiredSemanticString(root, "Verdict")
	if err != nil {
		return FrozenOutcome{}, err
	}
	label, err := requiredSemanticString(root, "Label")
	if err != nil {
		return FrozenOutcome{}, err
	}
	inputValue, ok := root["Input"]
	if !ok {
		return FrozenOutcome{}, fmt.Errorf("outcome input is required")
	}
	inputMembers, ok := inputValue.ObjectMap()
	if !ok {
		return FrozenOutcome{}, fmt.Errorf("outcome input must be an object")
	}
	inputs := make(map[string]runtimecontracts.WorkflowGateInputField, len(inputMembers))
	for name, encoded := range inputMembers {
		fields, ok := encoded.ObjectMap()
		if !ok {
			return FrozenOutcome{}, fmt.Errorf("input %s must be an object", name)
		}
		if err := requireExactSemanticFields(fields, "input "+name, "type", "label", "required"); err != nil {
			return FrozenOutcome{}, err
		}
		kind, err := requiredSemanticString(fields, "type")
		if err != nil {
			return FrozenOutcome{}, fmt.Errorf("input %s: %w", name, err)
		}
		label, err := requiredSemanticString(fields, "label")
		if err != nil {
			return FrozenOutcome{}, fmt.Errorf("input %s: %w", name, err)
		}
		required, ok := fields["required"].Bool()
		if !ok {
			return FrozenOutcome{}, fmt.Errorf("input %s required must be a boolean", name)
		}
		inputs[name] = runtimecontracts.WorkflowGateInputField{Type: kind, Required: required, Label: label}
	}
	return FrozenOutcome{Verdict: verdict, Label: label, Input: inputs}, nil
}

func requireExactSemanticFields(values map[string]semanticvalue.Value, label string, expected ...string) error {
	allowed := make(map[string]struct{}, len(expected))
	for _, name := range expected {
		allowed[name] = struct{}{}
		if _, ok := values[name]; !ok {
			return fmt.Errorf("%s has non-canonical semantic structure: missing field %q", label, name)
		}
	}
	for name := range values {
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("%s has non-canonical semantic structure: unexpected field %q", label, name)
		}
	}
	return nil
}

func requiredSemanticString(values map[string]semanticvalue.Value, name string) (string, error) {
	value, ok := values[name]
	if !ok {
		return "", fmt.Errorf("%s is required", name)
	}
	text, ok := value.String()
	if !ok {
		return "", fmt.Errorf("%s must be a string", name)
	}
	return text, nil
}

func semanticObjectProjection(value semanticvalue.Value, label string) (map[string]any, error) {
	if value.Kind() != semanticvalue.KindObject {
		return nil, fmt.Errorf("%s must be an object", label)
	}
	out, ok := value.Interface().(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s has an invalid object projection", label)
	}
	return out, nil
}
