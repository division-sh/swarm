package scenariodocument

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

// PrepareCLI closes all authored expression sites before session acquisition.
// Compiled derive payloads are already Materialized and pass through unchanged.
func PrepareCLI(out CLI, evaluator *Evaluator, fixture func(string) (semanticvalue.Value, error)) (CLI, error) {
	for i := range out.Setup.Entities {
		entity := &out.Setup.Entities[i]
		if err := prepareTextValues(evaluator, &entity.Flow, &entity.CurrentState); err != nil {
			return CLI{}, fmt.Errorf("setup.entities[%d]: %w", i, err)
		}
		if err := prepareObjects(evaluator, &entity.Fields, &entity.Gates); err != nil {
			return CLI{}, fmt.Errorf("setup.entities[%d]: %w", i, err)
		}
	}
	for i := range out.Steps {
		if err := prepareStep(&out.Steps[i], evaluator, fixture); err != nil {
			return CLI{}, fmt.Errorf("steps[%d]: %w", i, err)
		}
	}
	for i := range out.Expect.Entities {
		entity := &out.Expect.Entities[i]
		if err := prepareTextValues(evaluator, &entity.CurrentState); err != nil {
			return CLI{}, fmt.Errorf("expect.entities[%d]: %w", i, err)
		}
		if err := prepareObjects(evaluator, &entity.Fields, &entity.Gates); err != nil {
			return CLI{}, fmt.Errorf("expect.entities[%d]: %w", i, err)
		}
	}
	if err := validateAliases(out, evaluator); err != nil {
		return CLI{}, err
	}
	return out, nil
}

func validateAliases(out CLI, evaluator *Evaluator) error {
	aliases := map[string]string{}
	for _, entity := range out.Setup.Entities {
		aliases[entity.Alias] = entity.EntityType
	}
	for _, expectation := range out.Expect.Entities {
		if expectation.Ref == "" {
			continue
		}
		entityType, found := aliases[expectation.Ref]
		if !found {
			return fmt.Errorf("expect.entities.ref %q is not a setup alias", expectation.Ref)
		}
		if expectation.EntityType != "" && expectation.EntityType != entityType {
			return fmt.Errorf("expect.entities.type disagrees with setup alias %q", expectation.Ref)
		}
	}
	for _, step := range out.Steps {
		if step.Target == nil {
			continue
		}
		value, err := evaluator.Evaluate(step.Target)
		if err != nil {
			return err
		}
		if _, found := aliases[strings.TrimSpace(value.(string))]; !found {
			return fmt.Errorf("publish target %q is not a setup alias", value)
		}
	}
	return nil
}

func prepareStep(step *Step, evaluator *Evaluator, fixture func(string) (semanticvalue.Value, error)) error {
	if err := prepareTextValues(evaluator, &step.Emitter, &step.SourceEventID, &step.IdempotencyKey, &step.Target, &step.TargetFlowInstance, &step.TargetEntityID, &step.Verdict, &step.Until); err != nil {
		return err
	}
	if err := prepareObjects(evaluator, &step.Fields); err != nil {
		return err
	}
	for _, key := range sortedKeys(step.Match) {
		value := step.Match[key]
		if err := prepareTextValues(evaluator, &value); err != nil {
			return fmt.Errorf("match.%s: %w", key, err)
		}
		step.Match[key] = value
	}
	if step.Action == "publish" && !step.GeneratePayload {
		data, err := PreparePayload(step.Payload, evaluator, fixture)
		if err != nil {
			return err
		}
		step.Payload = data
	}
	return nil
}

func prepareTextValues(evaluator *Evaluator, fields ...*any) error {
	for _, field := range fields {
		if *field == nil {
			continue
		}
		value, err := evaluator.Evaluate(*field)
		if err != nil {
			return err
		}
		text, ok := value.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return fmt.Errorf("scenario expression must resolve to non-empty text, got %T", value)
		}
		data, err := Materialize(text)
		if err != nil {
			return err
		}
		*field = data
	}
	return nil
}

func prepareObjects(evaluator *Evaluator, fields ...*any) error {
	for _, field := range fields {
		if *field == nil {
			continue
		}
		value, err := evaluator.Evaluate(*field)
		if err != nil {
			return err
		}
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("scenario expression must resolve to an object, got %T", value)
		}
		data, err := Materialize(value)
		if err != nil {
			return err
		}
		*field = data
	}
	return nil
}
