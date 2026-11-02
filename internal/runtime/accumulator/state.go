package accumulator

import (
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

type State struct {
	Items     []map[string]any
	loadError error
}

func (s *State) Err() error {
	if s == nil {
		return nil
	}
	return s.loadError
}

// Admit compares the complete item before mutating the ordinary state list.
// Durable delivery idempotency belongs to the surrounding claim transaction.
func (s *State) Admit(spec *runtimecontracts.AccumulateSpec, payload map[string]any) (bool, error) {
	if err := s.Err(); err != nil {
		return false, err
	}
	if spec == nil || spec.Target == "" || spec.Key == "" {
		return false, invalidState("target and key are required")
	}
	key, err := itemKey(payload, spec.Key)
	if err != nil {
		return false, err
	}
	item, err := canonicaljson.Bytes(payload)
	if err != nil {
		return false, err
	}
	seen := map[string]bool{}
	duplicate := false
	for _, previous := range s.Items {
		previousKey, err := itemKey(previous, spec.Key)
		if err != nil || seen[previousKey] {
			return false, invalidState("list contains an invalid or repeated key")
		}
		seen[previousKey] = true
		if previousKey != key {
			continue
		}
		before, err := canonicaljson.Bytes(previous)
		if err != nil {
			return false, err
		}
		if string(before) != string(item) {
			return false, failures.New(failures.ClassConflictingDuplicate, "accumulator_payload_conflict", "accumulator", "admit", nil)
		}
		duplicate = true
	}
	if !duplicate {
		s.Items = append(s.Items, cloneObject(payload))
	}
	return duplicate, nil
}

func itemKey(item map[string]any, name string) (string, error) {
	value, present := item[name]
	if !present || value == nil {
		return "", invalidKey(name)
	}
	switch value.(type) {
	case string, bool, json.Number, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
	default:
		return "", invalidKey(name)
	}
	key, err := canonicaljson.Bytes(value)
	if err != nil {
		return "", invalidKey(name)
	}
	return string(key), nil
}

func Load(raw any) *State {
	s := &State{}
	if raw == nil {
		return s
	}
	var values []any
	switch list := raw.(type) {
	case []map[string]any:
		for _, item := range list {
			values = append(values, item)
		}
	case []any:
		values = list
	default:
		s.loadError = invalidState("target is not a list")
		return s
	}
	for _, value := range values {
		item, ok := value.(map[string]any)
		if !ok {
			s.loadError = invalidState("list item is not an object")
			return s
		}
		s.Items = append(s.Items, cloneObject(item))
	}
	return s
}

func invalidState(message string) error {
	return failures.Wrap(failures.ClassSchemaInvalid, "accumulator_state_invalid", "accumulator", "admit", nil, fmt.Errorf("accumulate: %s", message))
}

func invalidKey(name string) error {
	return failures.New(failures.ClassSchemaInvalid, "accumulator_key_invalid", "accumulator", "admit", map[string]any{"key": name})
}

func cloneObject(object map[string]any) map[string]any {
	out := make(map[string]any, len(object))
	for key, value := range object {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneObject(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneValue(item)
		}
		return out
	case []map[string]any:
		out := make([]map[string]any, len(typed))
		for i, item := range typed {
			out[i] = cloneObject(item)
		}
		return out
	default:
		return value
	}
}
