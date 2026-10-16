package accumulator

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

// State is a detached candidate for the declared state list, not a receipt ledger.
type State struct {
	Items     []map[string]any `json:"items"`
	loadError error
}

func (s *State) Err() error {
	if s == nil {
		return nil
	}
	return s.loadError
}

// Admit compares typed keys and complete items before changing the candidate.
func (s *State) Admit(spec *runtimecontracts.AccumulateSpec, payload map[string]any) (duplicate bool, err error) {
	if s.loadError != nil {
		return false, s.loadError
	}
	if spec == nil {
		return false, failures.Wrap(failures.ClassSchemaInvalid, "accumulator_key_invalid", "accumulator", "admit", nil, fmt.Errorf("accumulate requires key: <item field>; use op: append to collect every arrival"))
	}
	if _, err := spec.ItemKeyPath(); err != nil {
		return false, failures.Wrap(failures.ClassSchemaInvalid, "accumulator_key_invalid", "accumulator", "admit", nil, err)
	}
	detached, item, err := admitItem(payload)
	if err != nil {
		return false, failures.Wrap(failures.ClassSchemaInvalid, "accumulator_payload_invalid", "accumulator", "admit", nil, err)
	}
	key, err := itemKey(detached, spec.Key)
	if err != nil {
		return false, err
	}
	existing := make(map[scalarKey]semanticvalue.Value, len(s.Items))
	for _, stored := range s.Items {
		_, value, err := admitItem(stored)
		if err != nil {
			return false, invalidState("item payload")
		}
		storedKey, err := itemKey(stored, spec.Key)
		if err != nil {
			return false, invalidState("required scalar key")
		}
		if _, duplicate := existing[storedKey]; duplicate {
			return false, invalidState("duplicate item key")
		}
		existing[storedKey] = value
	}
	if previous, found := existing[key]; found {
		if !previous.Equal(item) {
			return false, failures.New(failures.ClassConflictingDuplicate, "accumulator_payload_conflict", "accumulator", "admit", nil)
		}
		return true, nil
	}
	items := make([]map[string]any, len(s.Items)+1)
	copy(items, s.Items)
	items[len(s.Items)] = detached
	s.Items = items
	return false, nil
}

// Preview uses the same list semantics on a caller-owned detached candidate.
func (s *State) Preview(spec *runtimecontracts.AccumulateSpec, payload map[string]any) (bool, error) {
	return s.Admit(spec, payload)
}

// The kind remains part of identity: false, "false", zero and "0" differ.
type scalarKey struct {
	kind    semanticvalue.Kind
	text    string
	number  float64
	boolean bool
}

func itemKey(item map[string]any, name string) (scalarKey, error) {
	raw, present := item[name]
	value, err := canonicaljson.FromGo(raw)
	if !present || err != nil {
		return scalarKey{}, invalidItemKey(name)
	}
	key := scalarKey{kind: value.Kind()}
	switch value.Kind() {
	case semanticvalue.KindString:
		key.text, _ = value.String()
	case semanticvalue.KindNumber:
		key.number, _ = value.Number()
	case semanticvalue.KindBool:
		key.boolean, _ = value.Bool()
	default:
		return scalarKey{}, invalidItemKey(name)
	}
	return key, nil
}

func invalidItemKey(name string) error {
	return failures.New(failures.ClassSchemaInvalid, "accumulator_key_invalid", "accumulator", "admit", map[string]any{"key": name})
}

func admitItem(item map[string]any) (map[string]any, semanticvalue.Value, error) {
	if item == nil {
		return nil, semanticvalue.Value{}, fmt.Errorf("a collected item must be an object")
	}
	detached, err := canonicaljson.CloneRuntimeValue(item)
	if err != nil {
		return nil, semanticvalue.Value{}, err
	}
	value, err := canonicaljson.FromGo(detached)
	if err != nil {
		return nil, semanticvalue.Value{}, err
	}
	return detached.(map[string]any), value, nil
}

// Load accepts the state list itself. Retired receipt/bucket objects are not data.
func Load(raw any) *State {
	s := &State{}
	var items []any
	switch list := raw.(type) {
	case []map[string]any:
		if list == nil {
			s.loadError = invalidState("null list")
			return s
		}
		items = make([]any, len(list))
		for index, item := range list {
			items[index] = item
		}
	case []any:
		if list == nil {
			s.loadError = invalidState("null list")
			return s
		}
		items = list
	default:
		s.loadError = invalidState("list value")
		return s
	}
	s.Items = make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			s.loadError = invalidState("item object")
			return s
		}
		detached, _, err := admitItem(object)
		if err != nil {
			s.loadError = invalidState("item payload")
			return s
		}
		s.Items = append(s.Items, detached)
	}
	return s
}

func invalidState(field string) error {
	return failures.Wrap(failures.ClassSchemaInvalid, "accumulator_state_invalid", "accumulator", "load", nil, fmt.Errorf("collected state list has invalid %s", field))
}
