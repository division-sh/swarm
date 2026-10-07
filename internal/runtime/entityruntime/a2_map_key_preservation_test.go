package entityruntime

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
)

func a2ExactTextMapKeys() []string {
	return []string{"a", " a ", "a ", "z", "z ", " ", "\t", "\ta\t"}
}

func a2ExactTextMapValues() map[string]any {
	values := map[string]any{}
	for index, key := range a2ExactTextMapKeys() {
		values[key] = []any{int64(index), int64(index), int64(index + 100)}
	}
	return values
}

func TestA2TextMapFieldPreservesExactNonemptyKeys(t *testing.T) {
	for _, keyType := range []string{"text", "string", "BusinessKey"} {
		t.Run(keyType, func(t *testing.T) {
			contract := Contract{
				Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
					"items": {Type: "map[" + keyType + "][integer]"},
				}},
				Types: rc.TypeCatalogDocument{Scalars: map[string]rc.ScalarTypeDecl{
					"BusinessKey": {Base: "text"},
				}},
			}
			source := a2ExactTextMapValues()
			before := cloneMap(source)
			got, err := NormalizeFieldValue(contract, "items", source)
			if err != nil || !reflect.DeepEqual(got, source) {
				t.Fatalf("field changed exact business keys/values: got=%#v err=%v", got, err)
			}
			if !reflect.DeepEqual(source, before) {
				t.Fatal("field normalization mutated source")
			}
			source[""] = []any{int64(99)}
			before = cloneMap(source)
			if got, err := NormalizeFieldValue(contract, "items", source); err == nil || got != nil || !strings.Contains(err.Error(), "map key cannot be empty") {
				t.Fatalf("empty text key admitted: got=%#v err=%v", got, err)
			}
			if !reflect.DeepEqual(source, before) {
				t.Fatal("empty-key refusal mutated source")
			}
		})
	}
}

func TestA2ContainedTextMapKeysPreserveExactIdentity(t *testing.T) {
	contract := Contract{Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
		"items": {Type: "map[text][integer]"},
	}}}
	for _, operation := range []string{ContainedOperationSet, ContainedOperationAppend, ContainedOperationUpdate, ContainedOperationDelete} {
		t.Run(operation, func(t *testing.T) {
			for _, key := range a2ExactTextMapKeys() {
				t.Run(fmt.Sprintf("key=%q", key), func(t *testing.T) {
					source := map[string]any{"items": a2ExactTextMapValues()}
					items := source["items"].(map[string]any)
					if operation == ContainedOperationSet {
						delete(items, key)
					}
					before := cloneMap(source)
					want := cloneMap(source)
					wantItems := want["items"].(map[string]any)
					mutation := Mutation{Operation: operation, Target: "items", HasKey: true, Key: key}
					switch operation {
					case ContainedOperationSet:
						mutation.Value = []any{int64(9), int64(9)}
						wantItems[key] = mutation.Value
					case ContainedOperationAppend:
						mutation.Value = int64(9)
						wantItems[key] = append(wantItems[key].([]any), mutation.Value)
					case ContainedOperationUpdate:
						mutation.HasIndex, mutation.Index, mutation.Value = true, int64(1), int64(9)
						wantItems[key].([]any)[1] = mutation.Value
					case ContainedOperationDelete:
						delete(wantItems, key)
					}
					got, err := ApplyMutations(contract, source, []Mutation{mutation})
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("contained %s changed another exact key or value: got=%#v want=%#v err=%v", operation, got, want, err)
					}
					if !reflect.DeepEqual(source, before) {
						t.Fatal("contained operation mutated source")
					}
				})
			}
		})
	}
	for _, key := range []any{"", nil, int64(7), []byte("a")} {
		t.Run(fmt.Sprintf("refuse=%T/%v", key, key), func(t *testing.T) {
			source := map[string]any{"items": a2ExactTextMapValues()}
			before := cloneMap(source)
			plan, err := NewMutationPlan(contract, source)
			if err != nil {
				t.Fatal(err)
			}
			if err := plan.Append(Mutation{Operation: ContainedOperationSet, Target: "items", HasKey: true, Key: key, Value: []any{int64(9)}}); err == nil {
				t.Fatalf("admitted absent/empty/nontext key %#v", key)
			}
			if got, err := plan.Validate(); err == nil || got != nil {
				t.Fatalf("refused key exposed committed candidate: got=%#v err=%v", got, err)
			}
			if !reflect.DeepEqual(plan.Draft(), before) || !reflect.DeepEqual(source, before) {
				t.Fatal("key refusal changed candidate or source")
			}
		})
	}
}

func TestA2TextMapKeysNestedInDeclaredRecord(t *testing.T) {
	contract := Contract{
		Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
			"records": {Type: "map[text]Record"},
		}},
		Types: rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{
			"Record": {Fields: map[string]rc.TypeFieldSpec{"items": {Type: "map[text][integer]"}}},
		}},
	}
	source := map[string]any{"records": map[string]any{
		" ":  map[string]any{"items": a2ExactTextMapValues()},
		"\t": map[string]any{"items": a2ExactTextMapValues()},
	}}
	before := cloneMap(source)
	for _, operation := range []string{ContainedOperationSet, ContainedOperationMerge} {
		t.Run(operation, func(t *testing.T) {
			value := map[string]any{"items": a2ExactTextMapValues()}
			got, err := ApplyMutations(contract, source, []Mutation{
				{Operation: operation, Target: "records", HasKey: true, Key: "\t", Value: value},
			})
			if err != nil || !reflect.DeepEqual(got, source) {
				t.Fatalf("nested map keys changed through declared record %s: got=%#v err=%v", operation, got, err)
			}
			if !reflect.DeepEqual(source, before) {
				t.Fatal("nested record operation mutated source")
			}
		})
	}
}

func TestA2NontextMapKeysRetainTypeOwnerNormalization(t *testing.T) {
	contract := Contract{Types: rc.TypeCatalogDocument{Enums: map[string]rc.EnumTypeDecl{
		"Status": {Values: []string{"open", "closed"}},
	}}}
	for _, control := range []struct {
		keyType string
		input   string
		want    string
	}{
		{"uuid", " \tD9428888-122B-11E1-B85C-61CD3CBB3210\t ", "D9428888-122B-11E1-B85C-61CD3CBB3210"},
		{"Status", " \topen\t ", "open"},
	} {
		t.Run(control.keyType, func(t *testing.T) {
			key, err := NormalizeContainedOperationKey(contract, control.keyType, control.input)
			if err != nil || key != control.want {
				t.Fatalf("changed type-owner key normalization: got=%q err=%v", key, err)
			}
			fieldContract := contract
			fieldContract.Entity = rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
				"items": {Type: "map[" + control.keyType + "]integer"},
			}}
			got, err := NormalizeFieldValue(fieldContract, "items", map[string]any{control.input: int64(1)})
			want := map[string]any{control.want: int64(1)}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("changed whole-field nontext key normalization: got=%#v err=%v", got, err)
			}
		})
	}
}

func TestA2WholeMapRejectsDuplicateNormalizedKeysAtomically(t *testing.T) {
	for _, tc := range []struct {
		keyType string
		key     string
	}{
		{"Status", "Ready"},
		{"uuid", "d9428888-122b-11e1-b85c-61cd3cbb3210"},
	} {
		contract := Contract{
			Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{"items": {Type: "map[" + tc.keyType + "]integer"}}},
			Types:  rc.TypeCatalogDocument{Enums: map[string]rc.EnumTypeDecl{"Status": {Values: []string{"Ready", "Done"}, Default: "Ready"}}},
		}
		for _, second := range []int64{1, 2} {
			t.Run(fmt.Sprintf("%s/second=%d", tc.keyType, second), func(t *testing.T) {
				input := map[string]any{tc.key: int64(1), " " + tc.key + " ": second}
				before := cloneMap(input)
				// Neither equal values nor map iteration order selects a winner.
				for attempt := 0; attempt < 16; attempt++ {
					if got, err := NormalizeFieldValue(contract, "items", input); err == nil || got != nil || !strings.Contains(err.Error(), "duplicate normalized map key") {
						t.Fatalf("duplicate map admitted: got=%#v err=%v", got, err)
					}
					source := map[string]any{"items": map[string]any{tc.key: int64(9)}}
					sourceBefore := cloneMap(source)
					plan, err := NewMutationPlan(contract, source)
					if err != nil {
						t.Fatal(err)
					}
					if err := plan.Append(Mutation{Target: "entity.items", Value: input}); err == nil || !strings.Contains(err.Error(), "duplicate normalized map key") {
						t.Fatalf("whole set selected a collision winner: %v", err)
					}
					if got, err := plan.Validate(); err == nil || got != nil {
						t.Fatalf("refusal exposed candidate: %#v, %v", got, err)
					}
					if !reflect.DeepEqual(plan.Draft(), sourceBefore) || !reflect.DeepEqual(source, sourceBefore) || !reflect.DeepEqual(input, before) {
						t.Fatal("duplicate-key refusal changed candidate, source, or operand")
					}
				}
			})
		}
	}
}
