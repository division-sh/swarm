package tools

import (
	"reflect"
	"strings"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/llm"
)

func TestEntityOperationCatalogSchemaAliasAndMapKeyParity(t *testing.T) {
	minLength, maxLength := 2, 4
	minRank, maxRank := float64(1), float64(3)
	textRefinement := rc.SchemaRefinements{Pattern: "^[a-z]+$", Length: rc.SchemaLengthRefinement{Min: &minLength, Max: &maxLength}}
	catalog := rc.TypeCatalogDocument{
		Scalars: map[string]rc.ScalarTypeDecl{"Label": {Base: "text"}, "Rank": {Base: "integer"}, "Identifier": {Base: "uuid"}},
		Enums:   map[string]rc.EnumTypeDecl{"Status": {Values: []string{"open", "closed"}, Default: "open"}},
		Types: map[string]rc.NamedTypeDecl{"Entry": {Fields: map[string]rc.TypeFieldSpec{
			"name": {Type: "Label", Refinements: textRefinement},
			"rank": {Type: "Rank", Refinements: rc.SchemaRefinements{Range: rc.SchemaRangeRefinement{Min: &minRank, Max: &maxRank}}},
			"note": {Type: "Label", IsOptional: true},
		}}},
	}
	entry := map[string]any{"name": "good", "rank": int64(2)}
	badPattern := map[string]any{"name": "BAD", "rank": int64(2)}
	badLength := map[string]any{"name": "a", "rank": int64(2)}
	badRange := map[string]any{"name": "good", "rank": int64(4)}
	badRecord := map[string]any{"name": "good", "rank": int64(2), "unknown": "forbidden"}
	for _, tc := range []struct {
		name, typeRef string
		refinements   rc.SchemaRefinements
		initial       any
		input         map[string]any
		valid         bool
	}{
		{"json/scalar", "json", rc.SchemaRefinements{}, "before", map[string]any{"value": "after"}, true},
		{"json/record", "json", rc.SchemaRefinements{}, map[string]any{"old": "kept"}, map[string]any{"value": map[string]any{"arbitrary": map[string]any{"nested": true}}}, true},
		{"array/dynamic-items", "array", rc.SchemaRefinements{}, []any{"before"}, map[string]any{"value": []any{"after", true, map[string]any{"arbitrary": "value"}, nil}}, true},
		{"alias/default", "Label", textRefinement, "good", map[string]any{"value": "nice"}, true},
		{"alias/explicit", "Label", textRefinement, "good", map[string]any{"op": "set", "value": "nice"}, true},
		{"alias/pattern", "Label", textRefinement, "good", map[string]any{"value": "BAD"}, false},
		{"alias/length", "Label", textRefinement, "good", map[string]any{"value": "a"}, false},
		{"list/alias-fields", "[Entry]", rc.SchemaRefinements{}, []any{entry}, map[string]any{"op": "append", "value": entry}, true},
		{"list/alias-pattern", "[Entry]", rc.SchemaRefinements{}, []any{entry}, map[string]any{"op": "append", "value": badPattern}, false},
		{"list/alias-range", "[Entry]", rc.SchemaRefinements{}, []any{entry}, map[string]any{"op": "update", "index": int64(0), "value": badRange}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contract := entityruntime.Contract{EntityType: "case", Types: catalog, Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{"field": {Type: tc.typeRef, Refinements: tc.refinements}}}}
			assertEntityCatalogSchemaRuntimeParity(t, contract, tc.initial, tc.input, tc.valid)
		})
	}
	for _, keyType := range []string{"Label", "Status", "Identifier"} {
		goodKey := map[string]string{"Label": " business key ", "Status": "open", "Identifier": "d9428888-122b-11e1-b85c-61cd3cbb3210"}[keyType]
		badKey := map[string]string{"Label": "", "Status": "unknown", "Identifier": "not-a-uuid"}[keyType]
		contract := entityruntime.Contract{EntityType: "case", Types: catalog, Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{"field": {Type: "map[" + keyType + "]Entry"}}}}
		for _, tc := range []struct {
			name  string
			key   string
			value any
			valid bool
		}{
			{"valid", goodKey, entry, true},
			{"invalid-key", badKey, entry, false},
			{"empty-key", "", entry, false},
			{"alias-pattern", goodKey, badPattern, false},
			{"alias-length", goodKey, badLength, false},
			{"alias-range", goodKey, badRange, false},
			{"record-closure", goodKey, badRecord, false},
		} {
			for _, mode := range []string{"default", "whole-set", "key-set"} {
				t.Run(keyType+"/"+tc.name+"/"+mode, func(t *testing.T) {
					input := map[string]any{"value": map[string]any{tc.key: tc.value}}
					if mode != "default" {
						input["op"] = "set"
					}
					if mode == "key-set" {
						input["key"], input["value"] = tc.key, tc.value
					}
					assertEntityCatalogSchemaRuntimeParity(t, contract, map[string]any{goodKey: entry}, input, tc.valid)
				})
			}
		}
	}
}

func TestEntityToolLiteralMutationRejectsWholeMapKeyCollisions(t *testing.T) {
	for _, keyType := range []string{"Status", "uuid"} {
		key := "Ready"
		if keyType == "uuid" {
			key = "d9428888-122b-11e1-b85c-61cd3cbb3210"
		}
		contract := entityruntime.Contract{
			Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{"field": {Type: "map[" + keyType + "]text"}}},
			Types:  rc.TypeCatalogDocument{Enums: map[string]rc.EnumTypeDecl{"Status": {Values: []string{"Ready", "Done"}, Default: "Ready"}}},
		}
		for _, second := range []string{"first", "different"} {
			for _, explicit := range []bool{false, true} {
				input := map[string]any{"value": map[string]any{key: "first", " " + key + " ": second}}
				before := map[string]any{"value": map[string]any{key: "first", " " + key + " ": second}}
				if explicit {
					input["op"], before["op"] = "set", "set"
				}
				if _, err := entityToolLiteralMutation(contract, "field", input); err == nil || !strings.Contains(err.Error(), "duplicate normalized map key") {
					t.Fatalf("keyType=%s explicit=%v second=%s: %v", keyType, explicit, second, err)
				}
				if !reflect.DeepEqual(input, before) {
					t.Fatal("whole-map refusal changed literal operand")
				}
			}
		}
	}
}

func assertEntityCatalogSchemaRuntimeParity(t *testing.T, contract entityruntime.Contract, initial any, input map[string]any, valid bool) {
	t.Helper()
	entry := roleScopedEntityToolSchemaEntry(contract, roleScopedEntityToolSpec{Kind: roleScopedEntityToolSaveField, Field: "field"})
	schema := entry.InputSchema
	execution, err := admitBuiltinExecutionTool("save_case_field", entry)
	if err != nil {
		t.Fatal(err)
	}
	candidates := []map[string]any{schema, execution.InputSchema()}
	if _, branches := schema["oneOf"]; branches {
		candidates = append(candidates, closeGeneratedJSONSchema(schema))
	}
	for _, candidate := range candidates {
		if err := llm.ValidateProviderToolSchema("save_case_field", candidate); err != nil {
			t.Fatal(err)
		}
		if err := ValidatePayloadAgainstSchema(candidate, input); (err == nil) != valid {
			t.Fatalf("schema valid=%v: %v; input=%#v", valid, err, input)
		}
		admitted, err := rc.AdmitToolInputSchemaMap(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if err := admitted.Validate(input); (err == nil) != valid {
			t.Fatalf("admitted schema valid=%v: %v", valid, err)
		}
		if _, branches := candidate["oneOf"]; branches {
			if problems := validateGeneratedJSONSchema("catalog mutation", candidate); len(problems) != 0 {
				t.Fatal(problems)
			}
		}
		for _, extra := range []string{"index", "unknown"} {
			if _, present := input[extra]; present {
				continue
			}
			withExtra := make(map[string]any, len(input)+1)
			for key, value := range input {
				withExtra[key] = value
			}
			withExtra[extra] = int64(0)
			if err := admitted.Validate(withExtra); err == nil {
				t.Fatalf("unexpected argument %q admitted", extra)
			}
		}
	}
	if _, err := entityToolLiteralMutation(contract, "field", input); (err == nil) != valid {
		t.Fatalf("tool operand valid=%v: %v", valid, err)
	}
	mutation := entityruntime.Mutation{Target: "entity.field", Value: input["value"]}
	if key, present := input["key"]; present {
		mutation.Key, mutation.HasKey = key, true
	}
	if index, present := input["index"]; present {
		mutation.Index, mutation.HasIndex = index, true
	}
	if operation, present := input["op"].(string); present && (operation != "set" || mutation.HasKey) {
		mutation.Operation = operation
	}
	source := map[string]any{"field": initial}
	plan, err := entityruntime.NewMutationPlan(contract, source)
	if err != nil {
		t.Fatal(err)
	}
	before := plan.Draft()
	err = plan.Append(mutation)
	if err == nil {
		_, err = plan.Validate()
	}
	if (err == nil) != valid {
		t.Fatalf("MutationPlan valid=%v: %v", valid, err)
	}
	if !valid && !reflect.DeepEqual(plan.Draft(), before) {
		t.Fatal("invalid operation changed candidate")
	}
	if !reflect.DeepEqual(source, before) {
		t.Fatal("operation mutated caller state")
	}
	if valid && mutation.Operation == "" {
		candidate, err := plan.Validate()
		if err != nil || !reflect.DeepEqual(candidate["field"], input["value"]) {
			t.Fatalf("whole set lost literal keys/value: %#v, %v", candidate, err)
		}
	}
}
