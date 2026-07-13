package decisioncard

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

// FrozenExpression is the decision-card-owned form of an authored expression.
// Literal values are admitted once when the card is materialized.
type FrozenExpression struct {
	Kind    runtimecontracts.ExpressionKind
	Literal semanticvalue.Value
	Ref     string
	CEL     string
}

func (e FrozenExpression) HasLiteralValue() bool {
	return e.Kind == runtimecontracts.ExpressionKindLiteral
}

type FrozenEmit struct {
	Event  string
	Fields map[string]FrozenExpression
}

func (e FrozenEmit) Empty() bool {
	return strings.TrimSpace(e.Event) == "" && len(e.Fields) == 0
}

type FrozenOutcome struct {
	Verdict    string
	Label      string
	Input      map[string]runtimecontracts.WorkflowGateInputField
	AdvancesTo string
	Emit       FrozenEmit
	EmitSchema semanticvalue.Value
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
		item := FrozenOutcome{
			Verdict: outcome.Verdict, Label: outcome.Label, Input: cloneGateInputs(outcome.Input),
			AdvancesTo: outcome.AdvancesTo,
			Emit:       FrozenEmit{Event: outcome.Emit.Event, Fields: make(map[string]FrozenExpression, len(outcome.Emit.Fields))},
			EmitSchema: semanticvalue.EmptyObject(),
		}
		for field, expression := range outcome.Emit.Fields {
			frozenExpression := FrozenExpression{Kind: expression.Kind, Ref: expression.Ref, CEL: expression.CEL}
			if expression.HasLiteralValue() {
				frozenExpression.Literal, err = canonicaljson.FromGo(expression.Literal)
				if err != nil {
					return Snapshot{}, fmt.Errorf("admit decision card outcome %s literal %s: %w", verdict, field, err)
				}
			}
			item.Emit.Fields[field] = frozenExpression
		}
		if len(outcome.EmitSchema) > 0 {
			item.EmitSchema, err = canonicaljson.FromGo(outcome.EmitSchema)
			if err != nil {
				return Snapshot{}, fmt.Errorf("admit decision card outcome %s schema: %w", verdict, err)
			}
			if item.EmitSchema.Kind() != semanticvalue.KindObject {
				return Snapshot{}, fmt.Errorf("decision card outcome %s schema must be an object", verdict)
			}
		}
		frozen[verdict] = item
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
	expressions := make(map[string]semanticvalue.Value, len(o.Emit.Fields))
	for name, expression := range o.Emit.Fields {
		fields := map[string]semanticvalue.Value{}
		if expression.HasLiteralValue() {
			fields["Literal"] = expression.Literal
		} else {
			fields["Literal"] = semanticvalue.Null()
		}
		encoded, err := semanticObjectWithText(
			map[string]string{"Kind": string(expression.Kind), "Ref": expression.Ref, "CEL": expression.CEL},
			fields,
		)
		if err != nil {
			return semanticvalue.Value{}, fmt.Errorf("encode emit field %s: %w", name, err)
		}
		expressions[name] = encoded
	}
	expressionValue, err := semanticvalue.ObjectFromMap(expressions)
	if err != nil {
		return semanticvalue.Value{}, err
	}
	emit, err := semanticObjectWithText(
		map[string]string{"Event": o.Emit.Event},
		map[string]semanticvalue.Value{"Fields": expressionValue},
	)
	if err != nil {
		return semanticvalue.Value{}, err
	}
	return semanticObjectWithText(
		map[string]string{"Verdict": o.Verdict, "Label": o.Label, "AdvancesTo": o.AdvancesTo},
		map[string]semanticvalue.Value{"Input": inputValue, "Emit": emit, "EmitSchema": o.EmitSchema},
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
	return Snapshot{Decision: decision, Title: title, Context: contextValue, Outcomes: outcomes}, nil
}

func frozenOutcomeFromSemanticValue(value semanticvalue.Value) (FrozenOutcome, error) {
	root, ok := value.ObjectMap()
	if !ok {
		return FrozenOutcome{}, fmt.Errorf("outcome must be an object")
	}
	verdict, err := requiredSemanticString(root, "Verdict")
	if err != nil {
		return FrozenOutcome{}, err
	}
	label, err := requiredSemanticString(root, "Label")
	if err != nil {
		return FrozenOutcome{}, err
	}
	advancesTo, err := requiredSemanticString(root, "AdvancesTo")
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
	emitValue, ok := root["Emit"]
	if !ok {
		return FrozenOutcome{}, fmt.Errorf("outcome emit is required")
	}
	emitFields, ok := emitValue.ObjectMap()
	if !ok {
		return FrozenOutcome{}, fmt.Errorf("outcome emit must be an object")
	}
	event, err := requiredSemanticString(emitFields, "Event")
	if err != nil {
		return FrozenOutcome{}, err
	}
	expressionContainer, ok := emitFields["Fields"]
	if !ok {
		return FrozenOutcome{}, fmt.Errorf("outcome emit fields are required")
	}
	expressionMembers, ok := expressionContainer.ObjectMap()
	if !ok {
		return FrozenOutcome{}, fmt.Errorf("outcome emit fields must be an object")
	}
	expressions := make(map[string]FrozenExpression, len(expressionMembers))
	for name, encoded := range expressionMembers {
		fields, ok := encoded.ObjectMap()
		if !ok {
			return FrozenOutcome{}, fmt.Errorf("emit field %s must be an object", name)
		}
		kind, err := requiredSemanticString(fields, "Kind")
		if err != nil {
			return FrozenOutcome{}, fmt.Errorf("emit field %s: %w", name, err)
		}
		ref, err := requiredSemanticString(fields, "Ref")
		if err != nil {
			return FrozenOutcome{}, fmt.Errorf("emit field %s: %w", name, err)
		}
		cel, err := requiredSemanticString(fields, "CEL")
		if err != nil {
			return FrozenOutcome{}, fmt.Errorf("emit field %s: %w", name, err)
		}
		expression := FrozenExpression{Kind: runtimecontracts.ExpressionKind(kind), Ref: ref, CEL: cel}
		if expression.HasLiteralValue() {
			literal, ok := fields["Literal"]
			if !ok {
				return FrozenOutcome{}, fmt.Errorf("emit field %s literal is required", name)
			}
			expression.Literal = literal
		}
		expressions[name] = expression
	}
	schema := semanticvalue.EmptyObject()
	if encoded, ok := root["EmitSchema"]; ok {
		if encoded.Kind() != semanticvalue.KindObject {
			return FrozenOutcome{}, fmt.Errorf("outcome emit schema must be an object")
		}
		schema = encoded
	}
	return FrozenOutcome{
		Verdict: verdict, Label: label, Input: inputs, AdvancesTo: advancesTo,
		Emit: FrozenEmit{Event: event, Fields: expressions}, EmitSchema: schema,
	}, nil
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
