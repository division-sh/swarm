package contracts

import (
	"reflect"
	"strings"
	"testing"
)

var additionalRelationConstraints = []string{
	"enum", "oneof", "equality", "enum_oneof", "enum_equality", "oneof_equality", "enum_oneof_equality",
}

func additionalRelationTarget(t *testing.T, constraint string) ToolInputSchema {
	t.Helper()
	mode := map[string]any{"type": "any"}
	properties := map[string]any{"mode": mode}
	if strings.Contains(constraint, "enum") {
		mode["enum"] = []any{"approved"}
	}
	if strings.Contains(constraint, "oneof") {
		mode["oneOf"] = []any{map[string]any{"type": "string"}, map[string]any{"type": "integer"}}
	}
	if strings.Contains(constraint, "equality") {
		mode["x-swarm-equalTo"] = "peer"
		properties["peer"] = map[string]any{"type": "any"}
	}
	target, err := AdmitToolInputSchemaMap(map[string]any{"type": "object", "properties": properties})
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func additionalRelationSource(policy string) ToolInputSchema {
	switch policy {
	case "true":
		return MustToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesAllowed(true))
	case "schema_any":
		return MustToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesSchema(MustToolInputSchema(ToolSchemaAny)))
	default:
		return MustToolInputSchema(ToolSchemaObject)
	}
}

func additionalRelationRefusal(subject, constraint string) string {
	if strings.Contains(constraint, "oneof") {
		return subject + " oneOf schemas are not provably assignable unless identical"
	}
	if strings.Contains(constraint, "equality") {
		return subject + ` source equality target "" is not provably assignable to target equality "peer"`
	}
	return subject + " target enum is narrower than an unbounded source"
}

func additionalRelationObject(name string, value ToolInputSchema) ToolInputSchema {
	return MustToolInputSchema(ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{name: value}),
		ToolSchemaRequired(name), ToolSchemaAdditionalPropertiesAllowed(false))
}

func additionalRelationPlacement(placement string, source, target ToolInputSchema, value any) (ToolInputSchema, ToolInputSchema, any, string) {
	switch placement {
	case "object":
		return additionalRelationObject("payload", source), additionalRelationObject("payload", target), map[string]any{"payload": value}, "audit.payload.mode"
	case "array":
		return MustToolInputSchema(ToolSchemaArray, ToolSchemaItems(source)), MustToolInputSchema(ToolSchemaArray, ToolSchemaItems(target)), []any{value}, "audit[].mode"
	case "additional":
		return MustToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesSchema(source)), MustToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesSchema(target)), map[string]any{"payload": value}, "audit.*.mode"
	default:
		return source, target, value, "audit.mode"
	}
}

func TestSchemaRelation2630AdditionalValues(t *testing.T) {
	for _, constraint := range additionalRelationConstraints {
		for _, policy := range []string{"omitted", "true", "schema_any"} {
			for _, placement := range []string{"root", "object", "array", "additional"} {
				t.Run(constraint+"/"+policy+"/"+placement, func(t *testing.T) {
					source, target, value, subject := additionalRelationPlacement(placement, additionalRelationSource(policy), additionalRelationTarget(t, constraint), map[string]any{"mode": true, "peer": "approved"})
					if err := source.Validate(value); err != nil {
						t.Fatalf("source must accept example value: %v", err)
					}
					if err := target.Validate(value); err == nil {
						t.Fatal("target must refuse example value")
					}
					beforeSource, beforeTarget := source.Projection(), target.Projection()
					for range 2 {
						err := source.ValidateAssignableTo("audit", target)
						if want := additionalRelationRefusal(subject, constraint); err == nil || err.Error() != want {
							t.Fatalf("relation error = %v, want %q", err, want)
						}
					}
					if !reflect.DeepEqual(beforeSource, source.Projection()) || !reflect.DeepEqual(beforeTarget, target.Projection()) {
						t.Fatal("relation changed admitted schema projection")
					}
				})
			}
		}
	}
}

func TestSchemaRelation2630MatchingUnionEqualityControl(t *testing.T) {
	for _, constraint := range []string{"oneof_equality", "enum_oneof_equality"} {
		t.Run(constraint, func(t *testing.T) {
			schema := additionalRelationTarget(t, constraint)
			copy := additionalRelationTarget(t, constraint)
			if err := schema.Validate(map[string]any{"mode": "approved", "peer": "approved"}); err != nil {
				t.Fatal(err)
			}
			property, _ := schema.Property("mode")
			_, projectionErr := property.Project()
			if projectionErr == nil || property.Equal(property) {
				t.Fatal("public root projection/equality must retain standalone admission")
			}
			if !schema.Equal(copy) || schema.value == copy.value {
				t.Fatal("control must use separately admitted identical schemas")
			}
			before := schema.Projection()
			t.Run("same", func(t *testing.T) {
				if err := schema.ValidateAssignableTo("matching", schema); err != nil {
					t.Fatalf("identical admitted object refused: %v", err)
				}
			})
			t.Run("separate_copy", func(t *testing.T) {
				if err := schema.ValidateAssignableTo("matching", copy); err != nil {
					t.Fatalf("separately admitted identical object refused: %v", err)
				}
			})
			if !reflect.DeepEqual(before, schema.Projection()) || !schema.Equal(copy) {
				t.Fatal("identity comparison changed admitted projections")
			}
			t.Run("nonidentical_parent", func(t *testing.T) {
				source, err := schema.WithProperty("other", MustToolInputSchema(ToolSchemaString, ToolSchemaMinLength(2)))
				if err != nil {
					t.Fatal(err)
				}
				target, err := copy.WithProperty("other", MustToolInputSchema(ToolSchemaString, ToolSchemaMinLength(1)))
				if err != nil {
					t.Fatal(err)
				}
				if source.Equal(target) {
					t.Fatal("whole-object identity must not prove this case")
				}
				if err := source.ValidateAssignableTo("matching", target); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("incompatible_peer", func(t *testing.T) {
				target, err := copy.WithProperty("peer", MustToolInputSchema(ToolSchemaAny, ToolSchemaEnum("other")))
				if err != nil {
					t.Fatal(err)
				}
				value := map[string]any{"mode": "approved", "peer": "approved"}
				if err := schema.Validate(value); err != nil {
					t.Fatal(err)
				}
				if err := target.Validate(value); err == nil {
					t.Fatal("target must refuse example at peer constraint")
				}
				want := "matching.peer target enum is narrower than an unbounded source"
				if err := schema.ValidateAssignableTo("matching", target); err == nil || err.Error() != want {
					t.Fatalf("peer proof error=%v, want %q", err, want)
				}
			})
		})
	}
	for _, change := range []string{"enum", "equality", "union"} {
		t.Run("changed_"+change, func(t *testing.T) {
			mode := map[string]any{"type": "any", "oneOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "integer"}}, "x-swarm-equalTo": "peer"}
			properties := map[string]any{"mode": mode, "peer": map[string]any{"type": "any"}}
			source := additionalRelationTarget(t, "oneof_equality")
			switch change {
			case "enum":
				source = additionalRelationTarget(t, "enum_oneof_equality")
				mode["enum"] = []any{"other"}
			case "equality":
				mode["x-swarm-equalTo"] = "other"
				properties["other"] = map[string]any{"type": "any"}
			case "union":
				mode["oneOf"] = []any{map[string]any{"type": "string"}, map[string]any{"type": "boolean"}}
			}
			target := mustAdmitToolSchemaMap(t, map[string]any{"type": "object", "properties": properties})
			want := "different.mode oneOf schemas are not provably assignable unless identical"
			if err := source.ValidateAssignableTo("different", target); err == nil || err.Error() != want {
				t.Fatalf("nonidentical union error=%v, want %q", err, want)
			}
		})
	}
	t.Run("standalone", func(t *testing.T) {
		if _, err := AdmitToolInputSchemaMap(map[string]any{"type": "any", "oneOf": []any{map[string]any{"type": "string"}}, "x-swarm-equalTo": "peer"}); err == nil {
			t.Fatal("standalone equality root acquired definition admission")
		}
		property, _ := additionalRelationTarget(t, "oneof_equality").Property("mode")
		if err := property.ValidateDefinition(); err == nil {
			t.Fatal("standalone property unexpectedly validates as root")
		}
		if err := property.ValidateAssignableTo("standalone", property); err == nil {
			t.Fatal("standalone root acquired object-property identity")
		}
	})
}

func TestSchemaRelation2630AdditionalControls(t *testing.T) {
	for _, constraint := range additionalRelationConstraints {
		t.Run("declared_and_closed/"+constraint, func(t *testing.T) {
			target := additionalRelationTarget(t, constraint)
			if err := target.Validate(map[string]any{"mode": "approved", "peer": "approved"}); err != nil {
				t.Fatalf("valid target value: %v", err)
			}
			if err := target.ValidateAssignableTo("matching", target); err != nil {
				t.Fatal(err)
			}
			closed := MustToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesAllowed(false))
			if err := closed.ValidateAssignableTo("optional absent", target); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, policy := range []string{"omitted", "true", "schema_any"} {
		t.Run("free_any/"+policy, func(t *testing.T) {
			target := MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{
				"mode": MustToolInputSchema(ToolSchemaAny, ToolSchemaDescription("description is not a value constraint")),
			}))
			before := target.Projection()
			if err := additionalRelationSource(policy).ValidateAssignableTo("free", target); err != nil {
				t.Fatal(err)
			}
			for _, value := range []any{nil, "approved", true, 3, map[string]any{"x": "y"}, []any{"x"}} {
				if err := target.Validate(map[string]any{"mode": value}); err != nil {
					t.Fatalf("free any refused %v: %v", value, err)
				}
			}
			if !reflect.DeepEqual(before, target.Projection()) {
				t.Fatal("description or projection changed")
			}
		})
	}
	t.Run("narrow_source_and_integer_widening", func(t *testing.T) {
		source := MustToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesSchema(MustToolInputSchema(ToolSchemaAny, ToolSchemaEnum("approved"))))
		if err := source.ValidateAssignableTo("narrow", additionalRelationTarget(t, "enum")); err != nil {
			t.Fatal(err)
		}
		integer := MustToolInputSchema(ToolSchemaInteger)
		number := MustToolInputSchema(ToolSchemaNumber)
		if err := integer.ValidateAssignableTo("widen", number); err != nil {
			t.Fatal(err)
		}
		if err := number.ValidateAssignableTo("reverse", integer); err == nil {
			t.Fatal("number to integer must remain refused")
		}
	})
	t.Run("conservative_union", func(t *testing.T) {
		target, _ := additionalRelationTarget(t, "oneof").Property("mode")
		source := MustToolInputSchema(ToolSchemaString)
		if err := source.ValidateAssignableTo("different union", target); err == nil {
			t.Fatal("nonidentical union must remain unproved")
		}
		if err := target.ValidateAssignableTo("union to free", MustToolInputSchema(ToolSchemaAny)); err != nil {
			t.Fatal(err)
		}
	})
	for name, option := range map[string]ToolInputSchemaOption{
		"pattern": ToolSchemaPattern("x"), "length": ToolSchemaMinLength(1), "format": ToolSchemaFormat("uuid"),
		"minimum": ToolSchemaMinimum(0), "items": ToolSchemaItems(MustToolInputSchema(ToolSchemaString)), "required": ToolSchemaRequired("mode"),
	} {
		t.Run("inapplicable/"+name, func(t *testing.T) {
			if _, err := NewToolInputSchema(ToolSchemaAny, option); err == nil {
				t.Fatal("inapplicable constraint must fail definition admission")
			}
		})
	}
}

func TestSchemaRelation2630AdditionalErrorOrder(t *testing.T) {
	free := additionalRelationSource("omitted")
	for _, tc := range []struct {
		name           string
		source, target ToolInputSchema
		want           string
	}{
		{"property_names", free, mustAdmitToolSchemaMap(t, map[string]any{"type": "object", "propertyNames": map[string]any{"type": "string", "enum": []any{"mode"}}, "properties": map[string]any{"mode": map[string]any{"type": "any", "enum": []any{"approved"}}}}), "audit source admits unconstrained property names while target constrains them"},
		{"required", free, MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"mode": MustToolInputSchema(ToolSchemaAny, ToolSchemaEnum("approved"))}), ToolSchemaRequired("mode")), `audit target requires property "mode" that source does not require`},
		{"declared", MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"first": MustToolInputSchema(ToolSchemaInteger)})), MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"first": MustToolInputSchema(ToolSchemaString), "mode": MustToolInputSchema(ToolSchemaAny, ToolSchemaEnum("approved"))})), "audit.first has incompatible types integer and string"},
		{"non_any", free, MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"mode": MustToolInputSchema(ToolSchemaString)})), `audit source admits unconstrained additional property "mode" while target constrains it`},
		{"closed_target", free, MustToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesAllowed(false)), "audit source admits additional properties while target is closed"},
		{"sorted_names", free, MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"z": MustToolInputSchema(ToolSchemaAny, ToolSchemaEnum("approved")), "a": MustToolInputSchema(ToolSchemaAny, ToolSchemaEnum("approved"))})), "audit.a target enum is narrower than an unbounded source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.source.ValidateAssignableTo("audit", tc.target); err == nil || err.Error() != tc.want {
				t.Fatalf("relation error = %v, want %q", err, tc.want)
			}
		})
	}
}

func mustAdmitToolSchemaMap(t *testing.T, raw map[string]any) ToolInputSchema {
	t.Helper()
	schema, err := AdmitToolInputSchemaMap(raw)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestSchemaRelation2630CompiledResultEntrances(t *testing.T) {
	input := MustToolInputSchema(ToolSchemaObject)
	for _, constraint := range additionalRelationConstraints {
		for _, policy := range []string{"omitted", "true", "schema_any"} {
			for _, entrance := range []string{"constructor", "with_schemas", "with_compiled_result"} {
				t.Run(constraint+"/"+policy+"/"+entrance, func(t *testing.T) {
					target := additionalRelationTarget(t, constraint)
					badOutput := additionalRelationObject("payload", additionalRelationSource(policy))
					goodOutput := additionalRelationObject("payload", target)
					projection := CompiledResultProjection{Fields: map[string]CompiledResultField{"projected": {From: "result.payload"}}, OutputSchema: additionalRelationObject("projected", target)}
					options := func(output ToolInputSchema) []ToolSchemaEntryOption {
						return []ToolSchemaEntryOption{WithToolHandler(ToolHandlerHTTP), WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.invalid"}), WithToolSchemas(input, output)}
					}
					good := MustToolSchemaEntry(append(options(goodOutput), WithToolCompiledResult(projection))...)
					before, _ := good.CompiledResult()
					badValue := map[string]any{"mode": true, "peer": "approved"}
					if err := badOutput.Validate(map[string]any{"payload": badValue}); err != nil {
						t.Fatal(err)
					}
					if err := projection.OutputSchema.Validate(map[string]any{"projected": badValue}); err == nil {
						t.Fatal("target must refuse source example")
					}
					var got ToolSchemaEntry
					var err error
					switch entrance {
					case "constructor":
						got, err = NewToolSchemaEntry(append(options(badOutput), WithToolCompiledResult(projection))...)
					case "with_schemas":
						got, err = good.WithSchemas(input, badOutput)
					case "with_compiled_result":
						unprojected := MustToolSchemaEntry(options(badOutput)...)
						got, err = unprojected.WithCompiledResult(projection)
						if _, present := unprojected.CompiledResult(); present {
							t.Fatal("original entry acquired result projection")
						}
					}
					want := additionalRelationRefusal("compiled result result.payload -> projected.mode", constraint)
					if err == nil || !strings.HasSuffix(err.Error(), want) || !got.IsZero() {
						t.Fatalf("entry zero=%v error=%v, want zero and %q", got.IsZero(), err, want)
					}
					after, _ := good.CompiledResult()
					if !reflect.DeepEqual(before, after) || !good.OutputSchema().Equal(goodOutput) {
						t.Fatal("refused revalidation changed original admitted entry")
					}
					compiled, ok := good.CompiledResultExecution()
					if !ok {
						t.Fatal("valid entry lost compiled projection")
					}
					valid := map[string]any{"mode": "approved", "peer": "approved"}
					result, err := compiled.Project(map[string]any{"payload": valid})
					if err != nil || !reflect.DeepEqual(result, map[string]any{"projected": valid}) {
						t.Fatalf("valid projection=%v error=%v", result, err)
					}
				})
			}
		}
	}
}
