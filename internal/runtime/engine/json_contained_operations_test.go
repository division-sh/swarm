package engine

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func TestJSONContainedMutationRequiresActualObjectAndRetainsStateOnError(t *testing.T) {
	contract := entityruntime.Contract{Entity: contracts.EntityContract{Fields: map[string]contracts.EntityFieldDecl{
		"values": {Type: "map[text]json"}, "items": {Type: "list<json>"},
	}}}
	for _, existing := range []any{"scalar", []any{int64(8)}, map[string]any{"old": float64(8)}} {
		metadata := map[string]any{"values": map[string]any{"key": existing}, "items": []any{int64(8)}}
		before, err := canonicaljson.CloneRuntimeValue(metadata)
		if err != nil {
			t.Fatal(err)
		}
		target, err := entityruntime.ResolveContainedOperationTarget(contract, "entity.values", "merge", true, false)
		if err != nil {
			t.Fatal(err)
		}
		patch, err := entityruntime.NormalizeContainedOperationValue(contract, target, "merge", map[string]any{"new": []any{nil, int64(8)}})
		if err != nil {
			t.Fatal(err)
		}
		result, err := entityruntime.ApplyMutations(contract, metadata, []entityruntime.Mutation{{Operation: "merge", Target: "entity.values", Key: "key", HasKey: true, Value: patch}})
		_, object := existing.(map[string]any)
		if !object {
			if err == nil || !reflect.DeepEqual(metadata, before) {
				t.Fatalf("invalid destination mutated: %#v %v", metadata, err)
			}
		} else if err != nil || !reflect.DeepEqual(result["values"].(map[string]any)["key"], map[string]any{"old": float64(8), "new": []any{nil, int64(8)}}) {
			t.Fatalf("object merge: %#v %v", result, err)
		}
	}
	for _, op := range []string{"set", "append", "update"} {
		path, hasKey, hasIndex := "entity.values", true, false
		if op != "set" {
			path, hasKey, hasIndex = "entity.items", false, op == "update"
		}
		target, err := entityruntime.ResolveContainedOperationTarget(contract, path, op, hasKey, hasIndex)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range []any{true, "scalar", int64(8), float64(8), []any{nil, int64(8)}} {
			metadata := map[string]any{"values": map[string]any{}, "items": []any{false}}
			normalized, err := entityruntime.NormalizeContainedOperationValue(contract, target, op, value)
			if err != nil {
				t.Fatal(err)
			}
			result, err := entityruntime.ApplyMutations(contract, metadata, []entityruntime.Mutation{{Operation: op, Target: path, Key: "key", HasKey: hasKey, Index: 0, HasIndex: hasIndex, Value: normalized}})
			if err != nil {
				t.Fatal(err)
			}
			got := result["values"].(map[string]any)["key"]
			if op == "append" {
				got = result["items"].([]any)[1]
			} else if op == "update" {
				got = result["items"].([]any)[0]
			}
			if !reflect.DeepEqual(got, value) {
				t.Fatalf("%s changed value: %#v != %#v", op, got, value)
			}
		}
	}
}
