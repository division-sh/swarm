package scenariodocument

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

// PreparePayload interprets controls only on authored mappings, never on a
// compound expression result or a compiled payload handed back as data.
func PreparePayload(spec any, evaluator *Evaluator, fixture func(string) (semanticvalue.Value, error)) (Materialized, error) {
	if data, ok := spec.(Materialized); ok {
		return data, nil
	}
	if spec == nil {
		return Materialized{}, fmt.Errorf("payload is required")
	}
	authored, controls := spec.(map[string]any)
	if !controls {
		value, err := evaluator.Evaluate(spec)
		if err != nil {
			return Materialized{}, err
		}
		return materializeObject(value)
	}
	var payload map[string]any
	if from, ok := authored["from"]; ok {
		var err error
		payload, err = prepareFixturePayload(from, evaluator, fixture)
		if err != nil {
			return Materialized{}, err
		}
	} else {
		inline := make(map[string]any, len(authored))
		for key, value := range authored {
			if key != "set" {
				inline[key] = value
			}
		}
		value, err := evaluator.Evaluate(inline)
		if err != nil {
			return Materialized{}, err
		}
		payload = value.(map[string]any)
	}
	if raw, ok := authored["set"]; ok {
		set, ok := raw.(map[string]any)
		if !ok {
			return Materialized{}, fmt.Errorf("payload.set must be a mapping")
		}
		if err := ValidateSetPaths(set); err != nil {
			return Materialized{}, err
		}
		for _, key := range sortedKeys(set) {
			value, err := evaluator.Evaluate(set[key])
			if err != nil {
				return Materialized{}, fmt.Errorf("payload.set.%s: %w", key, err)
			}
			if err := SetPath(payload, key, value); err != nil {
				return Materialized{}, err
			}
		}
	}
	return materializeObject(payload)
}

func prepareFixturePayload(from any, evaluator *Evaluator, fixture func(string) (semanticvalue.Value, error)) (map[string]any, error) {
	value, err := evaluator.Evaluate(from)
	if err != nil {
		return nil, fmt.Errorf("payload.from: %w", err)
	}
	label, ok := value.(string)
	if !ok || strings.TrimSpace(label) == "" {
		return nil, fmt.Errorf("payload.from must be non-empty text")
	}
	admitted, err := fixture(label)
	if err != nil {
		return nil, err
	}
	value, err = workflowexpr.ProjectSemanticValue(admitted)
	if err != nil {
		return nil, err
	}
	value, err = evaluator.Evaluate(value)
	if err != nil {
		return nil, fmt.Errorf("payload fixture %s: %w", label, err)
	}
	payload, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("payload fixture must be an object")
	}
	return payload, nil
}

func materializeObject(value any) (Materialized, error) {
	if _, ok := value.(map[string]any); !ok {
		return Materialized{}, fmt.Errorf("payload must be an object")
	}
	return Materialize(value)
}

func normalizedSetPath(raw string) (string, error) {
	parts := strings.Split(strings.TrimPrefix(strings.TrimSpace(raw), "payload."), ".")
	for i, part := range parts {
		parts[i] = strings.TrimSpace(part)
		if parts[i] == "" {
			return "", fmt.Errorf("payload.set path %q contains an empty segment", raw)
		}
	}
	return strings.Join(parts, "."), nil
}

func ValidateSetPaths(set map[string]any) error {
	type admittedPath struct{ path, original string }
	seen := make([]admittedPath, 0, len(set))
	for _, key := range sortedKeys(set) {
		path, err := normalizedSetPath(key)
		if err != nil {
			return err
		}
		for _, prior := range seen {
			if path == prior.path || strings.HasPrefix(path, prior.path+".") || strings.HasPrefix(prior.path, path+".") {
				return fmt.Errorf("payload.set paths %q and %q overlap", prior.original, key)
			}
		}
		seen = append(seen, admittedPath{path: path, original: key})
	}
	return nil
}

func SetPath(root map[string]any, raw string, value any) error {
	path, err := normalizedSetPath(raw)
	if err != nil {
		return err
	}
	parts := strings.Split(path, ".")
	cursor := root
	for i, part := range parts {
		if i == len(parts)-1 {
			if value == nil {
				delete(cursor, part)
			} else {
				cursor[part] = value
			}
			return nil
		}
		next, _ := cursor[part].(map[string]any)
		if next == nil {
			next = map[string]any{}
			cursor[part] = next
		}
		cursor = next
	}
	return nil
}
