package entityruntime

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestJSONAndNumericEntityMaterializationAndReload(t *testing.T) {
	contract := Contract{Entity: contracts.EntityContract{Fields: map[string]contracts.EntityFieldDecl{
		"json": {Type: "json"}, "numeric": {Type: "numeric"},
	}}}
	for _, value := range []any{int64(8), float64(8), float64(8.25), "not-a-number", true,
		[]any{int64(8), float64(8), nil, map[string]any{"value": "s"}},
		map[string]any{"nested": []any{nil, int64(8), float64(8)}},
	} {
		for _, number := range []any{int64(8), float64(8), float64(8.25)} {
			want := map[string]any{"json": value, "numeric": number}
			got, err := Materialize(contract, want)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("materialize %T/%T: %#v %v", value, number, got, err)
			}
			for cycle := 0; cycle < 3; cycle++ {
				raw, err := canonicaljson.MarshalPreservingNumberKinds(got)
				if err != nil {
					t.Fatal(err)
				}
				var reloaded map[string]any
				if err := canonicaljson.DecodePreservingNumberLexemes(raw, &reloaded); err != nil {
					t.Fatal(err)
				}
				got, err = Materialize(contract, reloaded)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("cycle %d %s: %#v %v; want %#v", cycle, raw, got, err, want)
				}
			}
		}
	}
	for _, number := range []any{int64(8), float64(8)} {
		contract.Entity.Fields["numeric"] = contracts.EntityFieldDecl{Type: "numeric", Initial: number}
		got, err := Materialize(contract, nil)
		if err != nil || !reflect.DeepEqual(got, map[string]any{"numeric": number, "json": map[string]any{}}) {
			t.Fatalf("initial/default: %#v %v", got, err)
		}
	}
}

func TestJSONEntityIsolationAndHostileValues(t *testing.T) {
	contract := Contract{Entity: contracts.EntityContract{Fields: map[string]contracts.EntityFieldDecl{"value": {Type: "json"}}}}
	source := map[string]any{"nested": []any{map[string]any{"value": int64(8)}, float64(8)}}
	got, err := NormalizeFieldValue(contract, "value", source)
	if err != nil {
		t.Fatal(err)
	}
	source["nested"].([]any)[0].(map[string]any)["value"] = int64(99)
	if got.(map[string]any)["nested"].([]any)[0].(map[string]any)["value"] != int64(8) {
		t.Fatal("caller alias retained")
	}
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	for _, invalid := range []any{nil, map[string]any(nil), []any(nil), math.NaN(), math.Inf(1), int64(9007199254740992),
		json.Number("9007199254740992"), string([]byte{0xff}), map[string]any{string([]byte{0xff}): true},
		[]any{map[string]any{"bad": func() {}}}, struct{ A int }{1}, cyclic,
	} {
		if value, err := NormalizeFieldValue(contract, "value", invalid); err == nil || value != nil {
			t.Fatalf("admitted hostile %T", invalid)
		}
	}
}

func TestJSONEntityOperationCapabilities(t *testing.T) {
	contract := Contract{Entity: contracts.EntityContract{Fields: map[string]contracts.EntityFieldDecl{
		"value": {Type: "json"}, "object": {Type: "object"}, "list": {Type: "list<json>"}, "map": {Type: "map[text]json"},
	}}}
	field, err := ResolveFieldPath(contract, "value")
	if err != nil || field.LeafKind != "json" {
		t.Fatalf("JSON kind: %#v %v", field, err)
	}
	if _, err := ResolveLeafField(contract, "value"); err == nil {
		t.Fatal("opaque JSON accepted as scalar key")
	}
	if _, err := ResolveFieldPath(contract, "value.child"); err == nil {
		t.Fatal("opaque JSON acquired structural fields")
	}
	if _, err := NormalizeFieldValue(contract, "object", []any{int64(8)}); err == nil {
		t.Fatal("object accepts non-object")
	}
	for _, op := range []string{ContainedOperationSet, ContainedOperationMerge, ContainedOperationAppend, ContainedOperationUpdate} {
		path, key, index := "entity.map", true, false
		if op == ContainedOperationAppend || op == ContainedOperationUpdate {
			path, key, index = "entity.list", false, op == ContainedOperationUpdate
		}
		target, err := ResolveContainedOperationTarget(contract, path, op, key, index)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range []any{int64(8), float64(8), true, "s", []any{nil, "s"}, map[string]any{"null": nil}} {
			got, err := NormalizeContainedOperationValue(contract, target, op, value)
			_, object := value.(map[string]any)
			if op == ContainedOperationMerge && !object {
				if err == nil {
					t.Fatal("merge accepted non-object")
				}
				continue
			}
			if err != nil || !reflect.DeepEqual(got, value) {
				t.Fatalf("%s %T: %#v %v", op, value, got, err)
			}
		}
	}
}
