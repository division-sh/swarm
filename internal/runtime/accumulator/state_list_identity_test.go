package accumulator

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func Test2293Q1StateListCarriesItsOwnItemIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  any
	}{
		{"empty_text", ""}, {"whitespace_text", " "}, {"text", "reviewer"},
		{"zero_integer", int64(0)}, {"integer", int64(-3)},
		{"zero_number", float64(0)}, {"fraction", float64(0.25)},
		{"false", false}, {"true", true},
		{"uuid", "a33acde0-47a0-4e08-beb2-d4635814f1ee"},
		{"timestamp", "2026-10-16T00:00:00Z"}, {"enum", "low"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := map[string]any{"reviewer_id": tc.key, "message": "original", "details": map[string]any{"value": "retained"}}
			state := &State{Items: []map[string]any{cloneObject(first)}}
			spec := &runtimecontracts.AccumulateSpec{Key: "reviewer_id"}
			before, err := canonicaljson.Bytes(state)
			if err != nil {
				t.Fatal(err)
			}
			duplicate, err := state.Admit(spec, cloneObject(first), "new-publication")
			if err != nil || !duplicate {
				t.Fatalf("list-only restored identity not recognized: duplicate=%v error=%v", duplicate, err)
			}
			changed := cloneObject(first)
			changed["details"].(map[string]any)["value"] = "changed"
			_, err = state.Admit(spec, changed, "different-publication")
			requireClass(t, err, failures.ClassConflictingDuplicate)
			after, err := canonicaljson.Bytes(state)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("duplicate or conflict mutated the retained list: before=%s after=%s error=%v", before, after, err)
			}
			if bytes.Contains(after, []byte(`"received":`)) || bytes.Contains(after, []byte(`"deliveries":`)) {
				t.Fatalf("identity was reconstructed into a second receipt ledger: %s", after)
			}
			if !reflect.DeepEqual(state.Items[0], first) {
				t.Fatalf("typed item changed: got=%#v want=%#v", state.Items[0], first)
			}
		})
	}
}

func Test2293Q1StateListKeyIdentityDoesNotStringify(t *testing.T) {
	state := &State{}
	spec := &runtimecontracts.AccumulateSpec{Key: "reviewer_id"}
	for index, key := range []any{false, "false", int64(0), "0", ""} {
		item := map[string]any{"reviewer_id": key, "sequence": int64(index)}
		if duplicate, err := state.Admit(spec, item, ""); err != nil || duplicate {
			t.Fatalf("distinct typed key %T(%v) conflated: duplicate=%v error=%v", key, key, duplicate, err)
		}
		item["sequence"] = "changed by caller"
	}
	if len(state.Items) != 5 {
		t.Fatalf("arrival count=%d, want 5", len(state.Items))
	}
	for index, item := range state.Items {
		if item["sequence"] != int64(index) {
			t.Fatalf("arrival order/input detachment changed at %d: %#v", index, item)
		}
	}
}
