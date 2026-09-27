package decisioncard

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

var ErrInvalidInput = errors.New("invalid decision input")

type InputFieldProgress struct {
	Fields         semanticvalue.Value
	AcceptedField  string
	NextFieldIndex int
	NextField      string
	Complete       bool
	Skipped        bool
}

// AdvanceInputField consumes exactly the next declared field. Selection and
// persistence of the draft remain the selected-store owner's responsibility.
func AdvanceInputField(outcome FrozenOutcome, fields semanticvalue.Value, index int, text string) (InputFieldProgress, error) {
	name, err := currentInputField(outcome, fields, index)
	if err != nil {
		return InputFieldProgress{}, err
	}
	value, err := ParseInputFieldText(outcome.Input[name].Type, text)
	if err != nil {
		return InputFieldProgress{}, err
	}
	fields, err = fields.With(name, value)
	if err != nil {
		return InputFieldProgress{}, err
	}
	return completedInputField(outcome, fields, index, name, false), nil
}

// SkipInputField omits only an optional current field. Callers must require an
// explicit authenticated skip action; plain text is always parsed as an answer.
func SkipInputField(outcome FrozenOutcome, fields semanticvalue.Value, index int) (InputFieldProgress, error) {
	name, err := currentInputField(outcome, fields, index)
	if err != nil {
		return InputFieldProgress{}, err
	}
	if outcome.Input[name].Required {
		return InputFieldProgress{}, fmt.Errorf("required decision input field cannot be skipped")
	}
	return completedInputField(outcome, fields, index, name, true), nil
}

func currentInputField(outcome FrozenOutcome, fields semanticvalue.Value, index int) (string, error) {
	if err := validateInputOrder(outcome.Input, outcome.InputOrder); err != nil {
		return "", err
	}
	if index < 0 || index >= len(outcome.InputOrder) {
		return "", fmt.Errorf("decision input draft has no next field")
	}
	if fields.Kind() != semanticvalue.KindObject {
		return "", fmt.Errorf("decision input draft fields are not an object")
	}
	if fields.Len() > index {
		return "", fmt.Errorf("decision input draft progress contradicts its field count")
	}
	for fieldIndex, fieldName := range outcome.InputOrder {
		_, present := fields.Lookup(fieldName)
		if present && fieldIndex >= index || !present && fieldIndex < index && outcome.Input[fieldName].Required {
			return "", fmt.Errorf("decision input draft progress contradicts declared order")
		}
	}
	return outcome.InputOrder[index], nil
}

func completedInputField(outcome FrozenOutcome, fields semanticvalue.Value, index int, name string, skipped bool) InputFieldProgress {
	next := index + 1
	progress := InputFieldProgress{Fields: fields, AcceptedField: name, NextFieldIndex: next, Complete: next == len(outcome.InputOrder), Skipped: skipped}
	if !progress.Complete {
		progress.NextField = outcome.InputOrder[next]
	}
	return progress
}

// ParseInputFieldText lowers one human text answer into the closed gate scalar
// vocabulary. It does not decide a card or advance a draft.
func ParseInputFieldText(kind, text string) (semanticvalue.Value, error) {
	if _, err := contracts.ValidateCanonicalWorkflowGateInputType(kind); err != nil {
		return semanticvalue.Value{}, err
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return semanticvalue.Value{}, fmt.Errorf("%w: decision input for %s is empty", ErrInvalidInput, kind)
	}
	var value semanticvalue.Value
	var err error
	switch kind {
	case "text":
		value, err = semanticvalue.String(text)
	case "timestamp", "uuid":
		value, err = semanticvalue.String(trimmed)
	case "integer", "numeric", "boolean":
		value, err = canonicaljson.Decode([]byte(trimmed))
		if err == nil && kind == "integer" {
			if _, parseErr := strconv.ParseInt(trimmed, 10, 64); parseErr != nil {
				err = parseErr
			}
		}
	default:
		return semanticvalue.Value{}, fmt.Errorf("unsupported decision input type %s", kind)
	}
	if err != nil || !contracts.WorkflowGateInputValueMatches(kind, value.Interface()) {
		return semanticvalue.Value{}, fmt.Errorf("%w: decision input must be %s", ErrInvalidInput, kind)
	}
	return value, nil
}
