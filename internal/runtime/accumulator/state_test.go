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
	spec := &runtimecontracts.AccumulateSpec{Key: "payload.id"}
	first := map[string]any{"id": "item", "nested": map[string]any{"b": int64(2), "a": "value"}}
	if duplicate, err := state.Admit(spec, first, "delivery-1"); err != nil || duplicate {
		t.Fatalf("first = %v, %v", duplicate, err)
	}
	before := cloneObject(state.Items[0])
	if duplicate, err := state.Admit(spec, map[string]any{"nested": map[string]any{"a": "value", "b": int64(2)}, "id": "item"}, "delivery-2"); err != nil || !duplicate {
		t.Fatalf("repeat = %v, %v", duplicate, err)
	}
	if len(state.Items) != 1 || len(state.Received) != 1 || !reflect.DeepEqual(state.Items[0], before) {
		t.Fatalf("duplicate changed state: %#v", state)
	}
}

func TestA2M11KeyedAccumulationConflictIsAtomic(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Key: "payload.id"}
	first := map[string]any{"id": "item", "event_id": "business-event", "event_type": "business-type", "source": "business-source", "received_at": "business-time", "payload": map[string]any{"nested": "original"}}
	for _, field := range []string{"event_id", "event_type", "source", "received_at", "payload"} {
		t.Run(field, func(t *testing.T) {
			state := &State{}
			if _, err := state.Admit(spec, first, "delivery-1"); err != nil {
				t.Fatal(err)
			}
			before := state.Received["item"]
			changed := cloneObject(first)
			changed[field] = "changed"
			_, err := state.Admit(spec, changed, "delivery-2")
			requireClass(t, err, failures.ClassConflictingDuplicate)
			if state.Received["item"] != before || len(state.Items) != 1 || !reflect.DeepEqual(state.Items[0], first) {
				t.Fatalf("conflict mutated state: %#v", state)
			}
		})
	}
}

func TestA2M12ConfiguredKeyRefusalIsAtomic(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Key: "payload.record.id"}
	for _, tc := range []struct {
		name string
		key  any
	}{
		{"null", nil}, {"empty", ""}, {"integer", int64(1)}, {"number", float64(1)}, {"boolean", true}, {"list", []any{"id"}}, {"record", map[string]any{"id": "value"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &State{}
			payload := map[string]any{"record": map[string]any{"id": tc.key}, "event_id": "not-a-fallback", "source": "not-a-fallback"}
			_, err := state.Admit(spec, payload, "durable-delivery")
			requireClass(t, err, failures.ClassSchemaInvalid)
			if len(state.Items)+len(state.Received)+len(state.Deliveries) != 0 {
				t.Fatalf("invalid key mutated state: %#v", state)
			}
		})
	}
	for _, payload := range []map[string]any{{}, {"record": map[string]any{}}, {"record": nil}, {"record": "not-an-object"}} {
		state := &State{}
		_, err := state.Admit(spec, payload, "durable-delivery")
		requireClass(t, err, failures.ClassSchemaInvalid)
	}
	state := &State{}
	for _, key := range []string{" exact ", "exact", " "} {
		if _, err := state.Admit(spec, map[string]any{"record": map[string]any{"id": key}}, ""); err != nil {
			t.Fatal(err)
		}
		if _, ok := state.Received[key]; !ok {
			t.Fatalf("key %q was normalized", key)
		}
	}
}

func TestA2M13CanonicalBusinessPayloadEvidenceSurvivesHydration(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Key: "payload.payload.id"}
	first := map[string]any{"payload": map[string]any{"id": "item", "nested": []any{int64(1), float64(0.5), nil}}, "source": "authored", "optional": nil}
	state := &State{}
	if _, err := state.Admit(spec, first, ""); err != nil {
		t.Fatal(err)
	}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(state)
	if err != nil {
		t.Fatal(err)
	}
	var stored map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &stored); err != nil {
		t.Fatal(err)
	}
	restored := Load(stored)
	if err := restored.Err(); err != nil {
		t.Fatalf("%v: %v; stored=%s", err, errors.Unwrap(err), raw)
	}
	if duplicate, err := restored.Admit(spec, first, "another-delivery"); err != nil || !duplicate {
		t.Fatalf("hydrated duplicate = %v, %v", duplicate, err)
	}
	for _, mutate := range []func(map[string]any){
		func(p map[string]any) { delete(p, "optional") },
		func(p map[string]any) { p["optional"] = "null" },
		func(p map[string]any) { p["payload"].(map[string]any)["nested"] = []any{"1", float64(0.5), nil} },
	} {
		changed := cloneObject(first)
		mutate(changed)
		_, err := restored.Admit(spec, changed, "")
		requireClass(t, err, failures.ClassConflictingDuplicate)
	}
	if len(restored.Items) != 1 || restored.Items[0]["source"] != "authored" {
		t.Fatalf("business payload lost: %#v", restored)
	}
}

func TestA2M14UnkeyedPreservesDeliveryMultiplicityAndOrder(t *testing.T) {
	state := &State{}
	first := map[string]any{"event_id": "same", "item_id": "same", "source": "same"}
	for _, delivery := range []string{"third-publication-first-commit", "first-publication-second-commit"} {
		if duplicate, err := state.Admit(nil, first, delivery); err != nil || duplicate {
			t.Fatalf("distinct delivery = %v, %v", duplicate, err)
		}
	}
	if _, err := state.Admit(nil, map[string]any{"marker": "third-commit"}, "third-commit"); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := state.Admit(nil, first, "third-publication-first-commit"); err != nil || !duplicate {
		t.Fatalf("retry = %v, %v", duplicate, err)
	}
	if len(state.Received) != 0 || len(state.Deliveries) != 3 || len(state.Items) != 3 || state.Items[2]["marker"] != "third-commit" {
		t.Fatalf("multiplicity/order = %#v", state)
	}
	before := cloneObject(state.Items[0])
	first["source"] = "changed"
	if !reflect.DeepEqual(state.Items[0], before) {
		t.Fatal("admitted business item aliases input")
	}
	_, err := state.Admit(nil, first, "third-publication-first-commit")
	requireClass(t, err, failures.ClassConflictingDuplicate)
	_, err = state.Admit(nil, first, "")
	requireClass(t, err, failures.ClassLifecycleConflict)
}

func TestA2AccumulatorLoadRefusesBooleanReceipts(t *testing.T) {
	state := Load(map[string]any{"received": map[string]any{"id": true}, "items": []any{map[string]any{"id": "id"}}})
	requireClass(t, state.Err(), failures.ClassSchemaInvalid)
	_, err := state.Admit(&runtimecontracts.AccumulateSpec{Key: "payload.id"}, map[string]any{"id": "id"}, "")
	requireClass(t, err, failures.ClassSchemaInvalid)
}

func TestA2AccumulatorHydrationRejectsCorruptPayloadEvidence(t *testing.T) {
	state := &State{}
	if _, err := state.Admit(nil, map[string]any{"marker": "original"}, "delivery"); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []map[string]any{
		{"deliveries": map[string]any{"delivery": state.Deliveries["delivery"]}, "items": []any{map[string]any{"marker": "corrupt"}}},
		{"deliveries": map[string]any{}, "items": []any{map[string]any{"marker": "unreceipted"}}},
		{"deliveries": map[string]any{"delivery": state.Deliveries["delivery"]}, "items": []any{}},
		{"deliveries": map[string]any{"delivery": state.Deliveries["delivery"]}, "items": []any{"not-an-object"}},
	} {
		loaded := Load(raw)
		requireClass(t, loaded.Err(), failures.ClassSchemaInvalid)
		_, err := loaded.Admit(nil, map[string]any{"marker": "original"}, "delivery")
		requireClass(t, err, failures.ClassSchemaInvalid)
	}
}

func TestA2AccumulatorRefusesReboundBusinessKeyReceipt(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Key: "payload.id"}
	state := &State{}
	item := map[string]any{"id": "original"}
	if _, err := state.Admit(spec, item, ""); err != nil {
		t.Fatal(err)
	}
	loaded := Load(map[string]any{"received": map[string]any{"rebound": state.Received["original"]}, "items": []map[string]any{item}})
	_, err := loaded.Admit(spec, item, "")
	requireClass(t, err, failures.ClassSchemaInvalid)
	if len(loaded.Items) != 1 || len(loaded.Received) != 1 {
		t.Fatal("corrupt receipt refusal mutated state")
	}
}

func TestA2UnkeyedEmptyBusinessPayloadIsAnItem(t *testing.T) {
	state := &State{}
	if _, err := state.Admit(nil, map[string]any{}, "delivery"); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := state.Admit(nil, map[string]any{}, "delivery"); err != nil || !duplicate {
		t.Fatalf("empty payload retry = %v, %v", duplicate, err)
	}
	_, err := state.Admit(nil, nil, "another-delivery")
	requireClass(t, err, failures.ClassSchemaInvalid)
	if len(state.Items) != 1 || len(state.Deliveries) != 1 {
		t.Fatal("null carrier was coerced to a business object")
	}
}
