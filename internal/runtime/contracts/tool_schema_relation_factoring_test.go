package contracts

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func relationFactoringSchema(t *testing.T, text string) ToolInputSchema {
	t.Helper()
	var projection map[string]any
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decoder.Decode(&projection); err != nil {
		t.Fatal(err)
	}
	schema, err := AdmitToolInputSchemaMap(projection)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestToolSchemaRelationFactoringCharacterization(t *testing.T) {
	for _, tc := range []struct{ name, source, target, want string }{
		{"zero_source", "", `{"type":"string"}`, "proof has no source or target schema"},
		{"zero_target", `{"type":"string"}`, "", "proof has no source or target schema"},
		{"oneof_identical", `{"type":"any","oneOf":[{"type":"string"},{"type":"integer"}]}`, `{"type":"any","oneOf":[{"type":"string"},{"type":"integer"}]}`, ""},
		{"oneof_free_any", `{"type":"any","oneOf":[{"type":"string"}]}`, `{"type":"any"}`, ""},
		{"oneof_unproved", `{"type":"string"}`, `{"type":"any","oneOf":[{"type":"string"}]}`, "proof oneOf schemas are not provably assignable unless identical"},
		{"oneof_constrained_any", `{"type":"any","oneOf":[{"type":"string"}]}`, `{"type":"any","enum":["x"]}`, "proof oneOf schemas are not provably assignable unless identical"},
		{"finite_enum_source", `{"type":"any","enum":[1,2]}`, `{"type":"number","minimum":1,"maximum":2}`, ""},
		{"finite_text", `{"type":"string","enum":["ab"]}`, `{"type":"string","minLength":2}`, ""},
		{"finite_null", `{"type":"null","enum":[null]}`, `{"type":"null"}`, ""},
		{"enum_subset", `{"type":"integer","enum":[2,1]}`, `{"type":"number","enum":[1,2,3]}`, ""},
		{"enum_missing_first", `{"type":"integer","enum":[2,1]}`, `{"type":"number","enum":[3]}`, "proof source enum value 2 is absent from target enum"},
		{"target_enum_unbounded_source", `{"type":"string"}`, `{"type":"string","enum":["x"]}`, "proof target enum is narrower than an unbounded source"},
		{"target_constraint", `{"type":"integer","enum":[1]}`, `{"type":"number","minimum":2}`, "proof source enum value is outside target schema: $ must be >= 2"},
		{"free_any_target", `{"type":"object","additionalProperties":false}`, `{"type":"any"}`, ""},
		{"integer_to_number", `{"type":"integer"}`, `{"type":"number"}`, ""},
		{"reverse_refusal", `{"type":"number"}`, `{"type":"integer"}`, "proof has incompatible types number and integer"},
		{"incompatible_types", `{"type":"string"}`, `{"type":"boolean"}`, "proof has incompatible types string and boolean"},
		{"boolean", `{"type":"boolean"}`, `{"type":"boolean"}`, ""},
		{"null", `{"type":"null"}`, `{"type":"null"}`, ""},
		{"string_min", `{"type":"string","minLength":2}`, `{"type":"string","minLength":3}`, "proof string length source minimum is broader than target minimum 3"},
		{"string_min_absent", `{"type":"string"}`, `{"type":"string","minLength":1}`, "proof string length source minimum is broader than target minimum 1"},
		{"string_max", `{"type":"string","maxLength":3}`, `{"type":"string","maxLength":2}`, "proof string length source maximum is broader than target maximum 2"},
		{"string_tighter", `{"type":"string","minLength":2,"maxLength":3}`, `{"type":"string","minLength":1,"maxLength":4}`, ""},
		{"pattern", `{"type":"string","pattern":"^a"}`, `{"type":"string","pattern":"^b"}`, `proof source pattern "^a" is not provably assignable to target pattern "^b"`},
		{"pattern_equal", `{"type":"string","pattern":"^a"}`, `{"type":"string","pattern":"^a"}`, ""},
		{"format", `{"type":"string"}`, `{"type":"string","format":"uuid"}`, `proof source format "" is not provably assignable to target format "uuid"`},
		{"numeric_min", `{"type":"integer","minimum":1}`, `{"type":"number","minimum":2}`, "proof numeric range source minimum is broader than target minimum 2"},
		{"numeric_max", `{"type":"number","maximum":3}`, `{"type":"number","maximum":2}`, "proof numeric range source maximum is broader than target maximum 2"},
		{"numeric_tighter", `{"type":"integer","minimum":2,"maximum":3}`, `{"type":"number","minimum":1,"maximum":4}`, ""},
		{"array_min", `{"type":"array","items":{"type":"integer"}}`, `{"type":"array","items":{"type":"integer"},"minItems":1}`, "proof array length source minimum is broader than target minimum 1"},
		{"array_max", `{"type":"array","items":{"type":"integer"},"maxItems":3}`, `{"type":"array","items":{"type":"integer"},"maxItems":2}`, "proof array length source maximum is broader than target maximum 2"},
		{"nested_items", `{"type":"array","items":{"type":"string"}}`, `{"type":"array","items":{"type":"string","minLength":1}}`, "proof[] string length source minimum is broader than target minimum 1"},
		{"array_widening", `{"type":"array","items":{"type":"integer"}}`, `{"type":"array","items":{"type":"number"}}`, ""},
		{"property_names_relation", `{"type":"object","propertyNames":{"type":"string"}}`, `{"type":"object","propertyNames":{"type":"string","minLength":1}}`, "proof propertyNames string length source minimum is broader than target minimum 1"},
		{"property_names_closed_good", `{"type":"object","additionalProperties":false,"properties":{"a":{"type":"string"}}}`, `{"type":"object","propertyNames":{"type":"string","enum":["a"]}}`, ""},
		{"property_names_closed_bad", `{"type":"object","additionalProperties":false,"properties":{"b":{"type":"string"}}}`, `{"type":"object","propertyNames":{"type":"string","enum":["a"]}}`, `proof source property name "b" is outside target propertyNames: $ is not one of the declared enum values`},
		{"property_names_open", `{"type":"object"}`, `{"type":"object","propertyNames":{"type":"string","minLength":1}}`, "proof source admits unconstrained property names while target constrains them"},
		{"missing_required_first", `{"type":"object"}`, `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}},"required":["b","a"]}`, `proof target requires property "b" that source does not require`},
		{"required_equal", `{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}`, `{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}`, ""},
		{"declared_nested", `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}}}`, `{"type":"object","properties":{"a":{"type":"string","minLength":2},"b":{"type":"string"}}}`, "proof.a string length source minimum is broader than target minimum 2"},
		{"extra_forbidden", `{"type":"object","properties":{"a":{"type":"string"}}}`, `{"type":"object","additionalProperties":false}`, `proof source property "a" is forbidden by target`},
		{"extra_schema", `{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"}}}`, `{"type":"object","additionalProperties":{"type":"number"}}`, ""},
		{"additional_free_any", `{"type":"object"}`, `{"type":"object","properties":{"a":{"type":"any"}}}`, ""},
		{"additional_source_schema", `{"type":"object","additionalProperties":{"type":"integer"}}`, `{"type":"object","properties":{"a":{"type":"number"}}}`, ""},
		{"source_closed", `{"type":"object","additionalProperties":false}`, `{"type":"object","additionalProperties":false}`, ""},
		{"target_closed", `{"type":"object"}`, `{"type":"object","additionalProperties":false}`, "proof source admits additional properties while target is closed"},
		{"additional_nested", `{"type":"object","additionalProperties":{"type":"string"}}`, `{"type":"object","additionalProperties":{"type":"string","minLength":2}}`, "proof.* string length source minimum is broader than target minimum 2"},
		{"additional_unconstrained", `{"type":"object"}`, `{"type":"object","additionalProperties":{"type":"string"}}`, "proof source admits unconstrained additional properties while target constrains them"},
		{"first_error_order", `{"type":"string","minLength":1,"maxLength":20,"pattern":"^a"}`, `{"type":"string","minLength":2,"maxLength":10,"pattern":"^b"}`, "proof string length source minimum is broader than target minimum 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var source, target ToolInputSchema
			if tc.source != "" {
				source = relationFactoringSchema(t, tc.source)
			}
			if tc.target != "" {
				target = relationFactoringSchema(t, tc.target)
			}
			beforeSource, beforeTarget := source.Projection(), target.Projection()
			for range 2 {
				err := source.ValidateAssignableTo("proof", target)
				got := ""
				if err != nil {
					got = err.Error()
				}
				if got != tc.want {
					t.Fatalf("error=%q, want %q", got, tc.want)
				}
			}
			if !reflect.DeepEqual(beforeSource, source.Projection()) || !reflect.DeepEqual(beforeTarget, target.Projection()) {
				t.Fatal("relation changed immutable projections")
			}
		})
	}
	t.Run("private_defensive_carriers", func(t *testing.T) {
		// Definition admission forbids these states; this is internal defense only.
		source := relationFactoringSchema(t, `{"type":"string","enum":["a"]}`)
		value := *source.value
		value.kind = ToolSchemaInteger
		source.value = &value
		if err := source.ValidateAssignableTo("proof", MustToolInputSchema(ToolSchemaString)); err == nil {
			t.Fatal("invalid source enum acquired proof")
		}
		array := MustToolInputSchema(ToolSchemaArray, ToolSchemaItems(MustToolInputSchema(ToolSchemaString)))
		value = *array.value
		value.items = ToolInputSchema{}
		value.hasItems = false
		array.value = &value
		if err := array.ValidateAssignableTo("proof", MustToolInputSchema(ToolSchemaArray, ToolSchemaItems(MustToolInputSchema(ToolSchemaString)))); err == nil || err.Error() != "proof source array items are unconstrained while target items are constrained" {
			t.Fatalf("missing private items: %v", err)
		}
		value.kind = ToolSchemaKind("unsupported")
		source.value = &value
		if err := source.ValidateAssignableTo("proof", source); err == nil || err.Error() != `proof has unsupported schema type "unsupported"` {
			t.Fatalf("unsupported private kind: %v", err)
		}
	})
}

func TestToolSchemaRelationFactoringConsumers(t *testing.T) {
	input := MustToolInputSchema(ToolSchemaObject)
	narrow := relationFactoringSchema(t, `{"type":"string","minLength":3}`)
	accepted := relationFactoringSchema(t, `{"type":"string","minLength":2}`)
	broad := MustToolInputSchema(ToolSchemaString)
	for _, entrance := range []string{"constructor", "with_schemas", "with_compiled_result"} {
		t.Run(entrance, func(t *testing.T) {
			source := additionalRelationObject("payload", narrow)
			projection := CompiledResultProjection{Fields: map[string]CompiledResultField{"projected": {From: "result.payload"}}, OutputSchema: additionalRelationObject("projected", accepted)}
			options := []ToolSchemaEntryOption{WithToolHandler(ToolHandlerHTTP), WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.invalid"}), WithToolSchemas(input, source)}
			original := MustToolSchemaEntry(append(options, WithToolCompiledResult(projection))...)
			before, _ := original.CompiledResult()
			compiled, _ := original.CompiledResultExecution()
			out, err := compiled.Project(map[string]any{"payload": "abc"})
			if err != nil || !reflect.DeepEqual(out, map[string]any{"projected": "abc"}) {
				t.Fatalf("valid projection: %v %v", out, err)
			}
			badSource := additionalRelationObject("payload", broad)
			var got ToolSchemaEntry
			switch entrance {
			case "constructor":
				got, err = NewToolSchemaEntry(WithToolHandler(ToolHandlerHTTP), WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.invalid"}), WithToolSchemas(input, badSource), WithToolCompiledResult(projection))
			case "with_schemas":
				got, err = original.WithSchemas(input, badSource)
			case "with_compiled_result":
				unprojected := MustToolSchemaEntry(WithToolSchemas(input, badSource))
				got, err = unprojected.WithCompiledResult(projection)
			}
			want := "compiled result result.payload -> projected string length source minimum is broader than target minimum 2"
			if err == nil || err.Error() != want || !got.IsZero() {
				t.Fatalf("refusal zero=%t error=%v, want %q", got.IsZero(), err, want)
			}
			after, _ := original.CompiledResult()
			if !reflect.DeepEqual(before, after) || !original.OutputSchema().Equal(source) {
				t.Fatal("consumer revalidation mutated the original entry")
			}
		})
	}
	t.Run("catalog_field_refinements", func(t *testing.T) {
		field := EntityFieldDecl{Type: "text", Refinements: SchemaRefinements{Pattern: "^a"}}
		patterned := relationFactoringSchema(t, `{"type":"string","pattern":"^a"}`)
		if err := ValidateCatalogFieldSupply("proof", patterned.Projection(), field, TypeCatalogDocument{}); err != nil {
			t.Fatal(err)
		}
		if err := ValidateCatalogFieldSupply("proof", broad.Projection(), field, TypeCatalogDocument{}); err == nil || err.Error() != `proof source pattern "" is not provably assignable to target pattern "^a"` {
			t.Fatalf("catalog refinement: %v", err)
		}
	})
	t.Run("catalog_nested_record_refusal", func(t *testing.T) {
		catalog := TypeCatalogDocument{Types: map[string]NamedTypeDecl{"Pair": {Fields: map[string]TypeFieldSpec{
			"mode": {Type: "text", Refinements: SchemaRefinements{EqualTo: "peer"}},
			"peer": {Type: "text", Refinements: SchemaRefinements{Pattern: "^other"}},
		}}}}
		source := detachedEqualityObject(t, "string", "mode", "approved", true)
		if err := source.Validate(map[string]any{"mode": "approved", "peer": "approved"}); err != nil {
			t.Fatal(err)
		}
		err := ValidateCatalogFieldSupply("proof", source.Projection(), EntityFieldDecl{Type: "Pair"}, catalog)
		if err == nil || !strings.Contains(err.Error(), "proof.peer source enum value is outside target schema") {
			t.Fatalf("contextual member identity bypassed incompatible catalog peer: %v", err)
		}
	})
	t.Run("map_key_nested", func(t *testing.T) {
		for _, path := range []string{"items", "additionalProperties", "property"} {
			source := map[string]any{"type": "object", "propertyNames": broad.Projection()}
			target := map[string]any{"type": "object", "propertyNames": accepted.Projection()}
			subject := "proof." + path
			if path == "property" {
				source = map[string]any{"type": "object", "properties": map[string]any{"child": source}}
				target = map[string]any{"type": "object", "properties": map[string]any{"child": target}}
				subject = "proof.child"
			} else {
				source, target = map[string]any{path: source}, map[string]any{path: target}
			}
			want := subject + " map key string length source minimum is broader than target minimum 2"
			if err := validateCatalogMapKeySupply("proof", source, target); err == nil || err.Error() != want {
				t.Fatalf("%s: %v, want %s", path, err, want)
			}
		}
	})
}
