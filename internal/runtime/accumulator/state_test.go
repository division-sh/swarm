package accumulator

import (
	"errors"
	"reflect"
	"strings"
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
	spec := &runtimecontracts.AccumulateSpec{Key: "id"}
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
}

func TestA2M11KeyedAccumulationConflictIsAtomic(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Key: "id"}
	first := map[string]any{"id": "item", "event_id": "business-event", "event_type": "business-type", "source": "business-source", "received_at": "business-time", "payload": map[string]any{"nested": "original"}}
	for _, field := range []string{"event_id", "event_type", "source", "received_at", "payload"} {
		t.Run(field, func(t *testing.T) {
			state := &State{}
			if _, err := state.Admit(spec, first); err != nil {
				t.Fatal(err)
			}
			before := cloneObject(state.Items[0])
			changed := cloneObject(first)
			changed[field] = "changed"
			_, err := state.Admit(spec, changed)
			requireClass(t, err, failures.ClassConflictingDuplicate)
			if len(state.Items) != 1 || !reflect.DeepEqual(state.Items[0], before) {
				t.Fatalf("conflict mutated state: %#v", state)
			}
		})
	}
}

func TestA2M12ConfiguredKeyRefusalIsAtomic(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Key: "id"}
	for _, payload := range []map[string]any{
		nil, {}, {"id": nil}, {"id": []any{"id"}}, {"id": map[string]any{"value": "id"}},
	} {
		state := &State{}
		_, err := state.Admit(spec, payload)
		requireClass(t, err, failures.ClassSchemaInvalid)
		if len(state.Items) != 0 {
			t.Fatalf("invalid key mutated state: %#v", state)
		}
	}
	state := &State{}
	for index, key := range []string{" exact ", "exact", " ", ""} {
		if _, err := state.Admit(spec, map[string]any{"id": key}); err != nil {
			t.Fatal(err)
		}
		if state.Items[index]["id"] != key {
			t.Fatalf("key %q was normalized", key)
		}
	}
}

func TestA2M13CanonicalBusinessPayloadEvidenceSurvivesHydration(t *testing.T) {
	spec := &runtimecontracts.AccumulateSpec{Key: "id"}
	first := map[string]any{"id": "item", "payload": map[string]any{"id": "nested", "nested": []any{int64(1), float64(0.5), nil}}, "source": "authored", "optional": nil}
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
		t.Fatalf("%v: %v; stored=%s", err, errors.Unwrap(err), raw)
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
	if len(restored.Items) != 1 || restored.Items[0]["source"] != "authored" {
		t.Fatalf("business payload lost: %#v", restored)
	}
}

func TestA2M14UnkeyedCollectionRequiresOrdinaryAppend(t *testing.T) {
	for _, payload := range []map[string]any{{}, {"event_id": "same", "item_id": "same", "source": "same"}} {
		state := &State{}
		_, err := state.Admit(nil, payload)
		requireClass(t, err, failures.ClassSchemaInvalid)
		cause := errors.Unwrap(err)
		if cause == nil || !strings.Contains(cause.Error(), "op: append") || len(state.Items) != 0 {
			t.Fatalf("unkeyed refusal lost teaching or changed the list: %#v %v", state, err)
		}
	}
	// Publication/redelivery multiplicity belongs to the ordinary append and
	// claim-settlement proof, not a second accumulator receipt implementation.
}

func TestA2AccumulatorLoadRefusesReceiptObjects(t *testing.T) {
	for _, raw := range []map[string]any{
		{"received": map[string]any{"id": true}, "items": []any{map[string]any{"id": "id"}}},
		{"deliveries": map[string]any{}, "items": []any{}},
		{"items": []any{map[string]any{"id": "id"}}},
	} {
		state := Load(raw)
		requireClass(t, state.Err(), failures.ClassSchemaInvalid)
		_, err := state.Admit(&runtimecontracts.AccumulateSpec{Key: "id"}, map[string]any{"id": "id"})
		requireClass(t, err, failures.ClassSchemaInvalid)
	}
}

func TestA2AccumulatorHydrationRejectsMalformedList(t *testing.T) {
	for _, raw := range []any{
		nil, []any(nil), []map[string]any(nil), "not-a-list",
		[]any{"not-an-object"}, []any{nil}, []any{map[string]any(nil)},
		[]any{map[string]any{"id": make(chan int)}},
	} {
		state := Load(raw)
		requireClass(t, state.Err(), failures.ClassSchemaInvalid)
		_, err := state.Admit(&runtimecontracts.AccumulateSpec{Key: "id"}, map[string]any{"id": "new"})
		requireClass(t, err, failures.ClassSchemaInvalid)
	}
}

func TestA2AccumulatorRefusesDuplicatedBusinessKeyEvidence(t *testing.T) {
	item := map[string]any{"id": "original", "value": int64(7)}
	loaded := Load([]map[string]any{item, item})
	if err := loaded.Err(); err != nil {
		t.Fatalf("probe must reach declaration-aware key validation: %v", err)
	}
	before, err := canonicaljson.MarshalPreservingNumberKinds(loaded.Items)
	if err != nil {
		t.Fatal(err)
	}
	_, err = loaded.Admit(&runtimecontracts.AccumulateSpec{Key: "id"}, map[string]any{"id": "new", "value": int64(8)})
	requireClass(t, err, failures.ClassSchemaInvalid)
	after, err := canonicaljson.MarshalPreservingNumberKinds(loaded.Items)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("invalid keyed evidence mutated state: before=%s after=%s err=%v", before, after, err)
	}
}

func TestA2AccumulatorPreviewAndAdmissionUseIdenticalDetachedLists(t *testing.T) {
	persisted := []map[string]any{{"id": "first", "payload": map[string]any{"value": "original"}}}
	admitted, preview := Load(persisted), Load(persisted)
	spec := &runtimecontracts.AccumulateSpec{Key: "id"}
	item := map[string]any{"id": "second", "payload": map[string]any{"value": "next"}}
	duplicate, err := admitted.Admit(spec, item)
	previewDuplicate, previewErr := preview.Preview(spec, item)
	if err != nil || previewErr != nil || duplicate || previewDuplicate || !reflect.DeepEqual(admitted.Items, preview.Items) {
		t.Fatalf("admission=%#v/%v preview=%#v/%v", admitted, err, preview, previewErr)
	}
	item["payload"].(map[string]any)["value"] = "caller mutation"
	preview.Items[0]["payload"].(map[string]any)["value"] = "preview mutation"
	if persisted[0]["payload"].(map[string]any)["value"] != "original" ||
		admitted.Items[0]["payload"].(map[string]any)["value"] != "original" ||
		admitted.Items[1]["payload"].(map[string]any)["value"] != "next" {
		t.Fatal("candidate or caller mutation leaked into another list")
	}
}

func cloneObject(object map[string]any) map[string]any {
	result := make(map[string]any, len(object))
	for key, value := range object {
		result[key] = cloneValue(value)
	}
	return result
}

func cloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneObject(value)
	case []any:
		result := make([]any, len(value))
		for index, item := range value {
			result[index] = cloneValue(item)
		}
		return result
	default:
		return value
	}
}
