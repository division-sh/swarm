package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectSchemaStagesValue(value yamlsource.Value) (FlowStageDeclarations, error) {
	out := FlowStageDeclarations{Declared: true}
	if value.Presence() == yamlsource.PresenceEmptySequence {
		return out, nil
	}
	fields, err := schemaValueDeclarations(value, true)
	if err != nil {
		return out, err
	}
	for _, field := range fields {
		members, err := schemaValueFields(field.Value, "stage", stageDeclarationFieldOptions, nil, false)
		if err != nil {
			return out, err
		}
		stage := FlowStageDeclaration{ID: field.Name}
		if err := schemaValueTexts(members, map[string]*string{"description": &stage.Description}, false); err != nil {
			return out, err
		}
		for _, flag := range []struct {
			key    string
			target *bool
		}{{"initial", &stage.Initial}, {"terminal", &stage.Terminal}} {
			if member, present := members[flag.key]; present {
				*flag.target, err = schemaValueBool(member, flag.key)
				if err != nil {
					return out, err
				}
			}
		}
		if timers, present := members["timers"]; present {
			items, err := schemaValueSequence(timers, false)
			if err != nil {
				return out, err
			}
			for _, item := range items {
				row, err := schemaValueFields(item, "stage timer", stageTimerFieldOptions, nil, true)
				if err != nil {
					return out, err
				}
				var timer FlowStageTimerDeclaration
				if err := schemaValueRequiredTexts(item, row, map[string]*string{"after": &timer.After}); err != nil {
					return out, err
				}
				if err := schemaValueTexts(row, map[string]*string{"id": &timer.ID, "emit": &timer.Emit, "advances_to": &timer.AdvancesTo}, true); err != nil {
					return out, err
				}
				stage.Timers = append(stage.Timers, timer)
			}
		}
		if gate, present := members["gate"]; present {
			stage.Gate, err = projectSchemaGateValue(gate)
			if err != nil {
				return out, err
			}
		}
		if err := stage.normalizeTimerIDs(); err != nil {
			return out, nodeValueError(field.Value, err)
		}
		out.Entries = append(out.Entries, stage)
	}
	if err := validateStageTimerIDNamespace(out.Entries); err != nil {
		return out, nodeValueError(value, err)
	}
	return out, nil
}

func projectSchemaGateValue(value yamlsource.Value) (*FlowStageGateDeclaration, error) {
	fields, err := schemaValueFields(value, "stage gate", stageGateFieldOptions, nil, true)
	if err != nil {
		return nil, err
	}
	out := &FlowStageGateDeclaration{}
	if err := schemaValueRequiredTexts(value, fields, map[string]*string{"decision": &out.Decision}); err != nil {
		return nil, err
	}
	if err := schemaValueTexts(fields, map[string]*string{"title": &out.Title}, false); err != nil {
		return nil, err
	}
	if context, present := fields["context"]; present {
		if _, err := schemaValueDeclarations(context, false); err != nil {
			return nil, err
		}
		out.Context, err = projectNodeExpressionFields(context, "gate context")
		if err != nil {
			return nil, err
		}
	}
	outcomes, present := fields["outcomes"]
	if !present {
		return nil, nodeValueError(value, fmt.Errorf("gate outcomes are required"))
	}
	rows, err := schemaValueDeclarations(outcomes, true)
	if err != nil {
		return nil, err
	}
	out.Outcomes = map[string]FlowStageGateOutcomeDeclaration{}
	for _, row := range rows {
		members, err := schemaValueFields(row.Value, "stage gate outcome", stageGateOutcomeFieldOptions, nil, true)
		if err != nil {
			return nil, err
		}
		var outcome FlowStageGateOutcomeDeclaration
		if err := schemaValueRequiredTexts(row.Value, members, map[string]*string{"advances_to": &outcome.AdvancesTo}); err != nil {
			return nil, err
		}
		if err := schemaValueTexts(members, map[string]*string{"label": &outcome.Label}, false); err != nil {
			return nil, err
		}
		if emit, present := members["emit"]; present {
			outcome.Emit, err = projectSchemaEmitValue(emit)
			if err != nil {
				return nil, err
			}
		}
		if input, present := members["input"]; present {
			outcome.Input, outcome.InputOrder, err = projectSchemaGateInputValue(input)
			if err != nil {
				return nil, err
			}
		}
		out.Outcomes[row.Name] = outcome
	}
	return out, nil
}

func projectSchemaGateInputValue(value yamlsource.Value) (map[string]WorkflowGateInputField, []string, error) {
	rows, err := schemaValueDeclarations(value, false)
	if err != nil {
		return nil, nil, err
	}
	out := map[string]WorkflowGateInputField{}
	order := make([]string, 0, len(rows))
	for _, row := range rows {
		fields, err := schemaValueFields(row.Value, "stage gate input field", stageGateInputFieldOptions, nil, true)
		if err != nil {
			return nil, nil, err
		}
		var field WorkflowGateInputField
		if err := schemaValueRequiredTexts(row.Value, fields, map[string]*string{"type": &field.Type}); err != nil {
			return nil, nil, err
		}
		field.Type, err = NormalizeWorkflowGateInputType(field.Type)
		if err != nil {
			return nil, nil, nodeValueError(row.Value, err)
		}
		if err := schemaValueTexts(fields, map[string]*string{"label": &field.Label}, false); err != nil {
			return nil, nil, err
		}
		if required, present := fields["required"]; present {
			field.Required, err = schemaValueBool(required, "gate input required")
			if err != nil {
				return nil, nil, err
			}
		}
		out[row.Name] = field
		order = append(order, row.Name)
	}
	return out, order, nil
}

func projectSchemaEmitValue(value yamlsource.Value) (EmitSpec, error) {
	if value.Presence() == yamlsource.PresenceScalar {
		if _, err := schemaValueText(value, true); err != nil {
			return EmitSpec{}, err
		}
	} else {
		fields, err := schemaValueFields(value, "emit", map[string]struct{}{"event": {}, "from": {}, "fields": {}}, nil, true)
		if err != nil {
			return EmitSpec{}, err
		}
		var event, from string
		if err := schemaValueRequiredTexts(value, fields, map[string]*string{"event": &event}); err != nil {
			return EmitSpec{}, err
		}
		if err := schemaValueTexts(fields, map[string]*string{"from": &from}, true); err != nil {
			return EmitSpec{}, err
		}
	}
	return projectNodeEmitValue(value)
}

func projectSchemaLoopsValue(value yamlsource.Value) (FlowLoopDeclarations, error) {
	rows, err := schemaValueDeclarations(value, false)
	out := FlowLoopDeclarations{Declared: true}
	if err != nil {
		return out, err
	}
	revisions := map[string]string{}
	for _, row := range rows {
		fields, err := schemaValueFields(row.Value, "loop", loopDeclarationFieldOptions, nil, true)
		if err != nil {
			return out, err
		}
		loop := FlowLoopDeclaration{ID: row.Name}
		if err := schemaValueRequiredTexts(row.Value, fields, map[string]*string{"revision_field": &loop.RevisionField}); err != nil {
			return out, err
		}
		limit, present := fields["max_attempts"]
		if !present {
			return out, nodeValueError(row.Value, fmt.Errorf("max_attempts is required"))
		}
		loop.MaxAttempts, err = projectSchemaLoopLimitValue(limit)
		if err != nil {
			return out, err
		}
		escape, present := fields["escape"]
		if !present {
			return out, nodeValueError(row.Value, fmt.Errorf("escape is required"))
		}
		members, err := schemaValueFields(escape, "loop escape", loopEscapeFieldOptions, nil, true)
		if err != nil {
			return out, err
		}
		if err := schemaValueRequiredTexts(escape, members, map[string]*string{"advances_to": &loop.Escape.AdvancesTo}); err != nil {
			return out, err
		}
		if emit, present := members["emit"]; present {
			loop.Escape.Emit, err = projectSchemaEmitValue(emit)
			if err != nil {
				return out, err
			}
		}
		if previous, exists := revisions[loop.RevisionField]; exists {
			return out, nodeValueError(row.Value, fmt.Errorf("loops %q and %q use the same revision_field", previous, row.Name))
		}
		revisions[loop.RevisionField] = row.Name
		out.Entries = append(out.Entries, loop)
	}
	return out, nil
}

func projectSchemaLoopLimitValue(value yamlsource.Value) (LoopAttemptLimit, error) {
	scalar, err := value.Scalar()
	if err != nil {
		return LoopAttemptLimit{}, err
	}
	if scalar.Tag == "!!int" {
		var count int
		if err := value.Project(&count); err != nil || count <= 0 {
			return LoopAttemptLimit{}, nodeValueError(value, fmt.Errorf("max_attempts must be a positive integer"))
		}
		return LoopAttemptLimit{Literal: count}, nil
	}
	raw, err := schemaValueText(value, true)
	if err != nil {
		return LoopAttemptLimit{}, err
	}
	if !strings.HasPrefix(raw, "{{") || !strings.HasSuffix(raw, "}}") {
		return LoopAttemptLimit{}, nodeValueError(value, fmt.Errorf("max_attempts must be a positive integer or {{policy_key}}"))
	}
	key := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(raw, "{{"), "}}"))
	key = strings.TrimPrefix(key, "policy.")
	if key == "" || strings.ContainsAny(key, "{}[]() \t\r\n") {
		return LoopAttemptLimit{}, nodeValueError(value, fmt.Errorf("max_attempts policy reference is invalid"))
	}
	return LoopAttemptLimit{PolicyRef: key}, nil
}
