package accumulator

import (
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func requireClass(t *testing.T, err error, class failures.Class) {
	t.Helper()
	var failure *failures.Error
	if !errors.As(err, &failure) || failure.Failure.Class != class {
		t.Fatalf("error = %v, want %s", err, class)
	}
}

func TestA2M10KeyedAccumulationIdempotent(t *testing.T) {
	state := &State{}
	spec := &runtimecontracts.AccumulateSpec{Target: "items", Key: "id"}
	first := map[string]any{"id": "item", "nested": map[string]any{"b": int64(2), "a": "value"}}
	if duplicate, err := state.Admit(spec, first); err != nil || duplicate {
		t.Fatalf("first = %v, %v", duplicate, err)
	}
	before := cloneObject(state.Items[0])
	if duplicate, err := state.Admit(spec, map[string]any{"nested": map[string]any{"a": "value", "b": int64(2)}, "id": "item"}); err != nil || !duplicate {
		t.Fatalf("repeat = %v, %v", duplicate, err)
	}
	if len(state.Items) != 1 || !reflect.DeepEqual(state.Items[0], before) {
		t.Fatalf("duplicate changed state: %#v", state)
	}
	first["nested"].(map[string]any)["a"] = "changed"
	if !reflect.DeepEqual(state.Items[0], before) {
		t.Fatal("admitted item aliases input")
	}
}

func TestA2M11KeyedAccumulationConflictIsAtomic(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Target: "items", Key: "id"}
	first := map[string]any{"id": "item", "event_id": "business-event", "event_type": "business-type", "source": "business-source", "received_at": "business-time", "payload": map[string]any{"nested": "original"}}
	for _, field := range []string{"event_id", "event_type", "source", "received_at", "payload"} {
		t.Run(field, func(t *testing.T) {
			state := &State{}
			if _, err := state.Admit(spec, first); err != nil {
				t.Fatal(err)
			}
			changed := cloneObject(first)
			changed[field] = "changed"
			_, err := state.Admit(spec, changed)
			requireClass(t, err, failures.ClassConflictingDuplicate)
			if len(state.Items) != 1 || !reflect.DeepEqual(state.Items[0], first) {
				t.Fatalf("conflict mutated state: %#v", state)
			}
		})
	}
}

func TestA2M12ConfiguredKeyRefusalIsAtomic(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Target: "items", Key: "id"}
	for _, key := range []any{nil, []any{"id"}, map[string]any{"id": "value"}} {
		state := &State{}
		_, err := state.Admit(spec, map[string]any{"id": key})
		requireClass(t, err, failures.ClassSchemaInvalid)
		if len(state.Items) != 0 {
			t.Fatalf("invalid key mutated state: %#v", state)
		}
	}
	state := &State{}
	_, err := state.Admit(spec, map[string]any{"other": "missing"})
	requireClass(t, err, failures.ClassSchemaInvalid)
	for _, key := range []any{"", " ", " a ", "a", int64(0), int64(1), float64(1.5), true, false} {
		item := map[string]any{"id": key}
		if duplicate, err := state.Admit(spec, item); err != nil || duplicate {
			t.Fatalf("scalar key %#v = %v/%v", key, duplicate, err)
		}
	}
	if len(state.Items) != 9 {
		t.Fatalf("scalar identity/order changed: %#v", state.Items)
	}
}

func TestA2M13CanonicalBusinessPayloadEvidenceSurvivesHydration(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Target: "items", Key: "id"}
	first := map[string]any{"id": "item", "payload": map[string]any{"nested": []any{int64(1), float64(0.5), nil}}, "source": "authored", "optional": nil}
	state := &State{}
	if _, err := state.Admit(spec, first); err != nil {
		t.Fatal(err)
	}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(state.Items)
	if err != nil {
		t.Fatal(err)
	}
	var stored []any
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &stored); err != nil {
		t.Fatal(err)
	}
	restored := Load(stored)
	if err := restored.Err(); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := restored.Admit(spec, first); err != nil || !duplicate {
		t.Fatalf("hydrated duplicate = %v, %v", duplicate, err)
	}
	for _, mutate := range []func(map[string]any){
		func(p map[string]any) { delete(p, "optional") },
		func(p map[string]any) { p["optional"] = "null" },
		func(p map[string]any) { p["payload"].(map[string]any)["nested"] = []any{"1", float64(0.5), nil} },
	} {
		changed := cloneObject(first)
		mutate(changed)
		_, err := restored.Admit(spec, changed)
		requireClass(t, err, failures.ClassConflictingDuplicate)
	}
}

func TestA2AccumulatorRefusesDuplicatedBusinessKeyEvidence(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Target: "items", Key: "id"}
	item := map[string]any{"id": "original", "value": int64(7)}
	loaded := Load([]map[string]any{item, item})
	before, err := canonicaljson.MarshalPreservingNumberKinds(loaded.Items)
	if err != nil {
		t.Fatal(err)
	}
	_, err = loaded.Admit(spec, map[string]any{"id": "new", "value": int64(8)})
	requireClass(t, err, failures.ClassSchemaInvalid)
	after, err := canonicaljson.MarshalPreservingNumberKinds(loaded.Items)
	if err != nil || string(before) != string(after) {
		t.Fatalf("corrupt list refusal mutated state: %s / %s (%v)", before, after, err)
	}
}
