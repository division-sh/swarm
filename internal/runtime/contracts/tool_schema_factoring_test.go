package contracts

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

func assertFactoringSchemaError(t *testing.T, err error, want string) {
	t.Helper()
	got := ""
	if err != nil {
		got = err.Error()
	}
	if got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func TestFactoringSchemaDefinition(t *testing.T) {
	text := MustToolInputSchema(ToolSchemaString)
	integer := MustToolInputSchema(ToolSchemaInteger)
	for _, tc := range []struct {
		name  string
		value toolInputSchemaValue
		want  string
	}{
		{"shape-order", toolInputSchemaValue{kind: "unknown", oneOfDeclared: true}, `$ requires an explicit supported JSON type, got "unknown"`},
		{"oneof-definition/empty", toolInputSchemaValue{kind: ToolSchemaAny, oneOfDeclared: true}, "$ oneOf must contain at least one schema"},
		{"oneof-definition/child-before-applicability", toolInputSchemaValue{kind: ToolSchemaString, oneOf: []ToolInputSchema{{}}, hasItems: true}, "$.oneOf[0] schema is missing"},
		{"shape-order/object-before-array", toolInputSchemaValue{kind: ToolSchemaBoolean, hasPropertyNames: true, hasItems: true}, "$ type boolean cannot declare object constraints"},
		{"shape-order/array-before-string", toolInputSchemaValue{kind: ToolSchemaBoolean, hasItems: true, hasMinLength: true}, "$ type boolean cannot declare array constraints"},
		{"string/applicability", toolInputSchemaValue{kind: ToolSchemaBoolean, pattern: "[", hasMinimum: true}, "$ type boolean cannot declare string constraints"},
		{"equality-order/scope-before-numeric", toolInputSchemaValue{kind: ToolSchemaBoolean, equalTo: "other", hasMinimum: true}, "$ x-swarm-equalTo is only valid on a declared object property"},
		{"numeric/applicability", toolInputSchemaValue{kind: ToolSchemaBoolean, hasMinimum: true}, "$ type boolean cannot declare numeric constraints"},
		{"numeric/minimum-infinity", toolInputSchemaValue{kind: ToolSchemaNumber, hasMinimum: true, minimum: math.Inf(1)}, "$ minimum must be a finite non-negative-zero JSON number"},
		{"numeric/minimum-negative-zero", toolInputSchemaValue{kind: ToolSchemaNumber, hasMinimum: true, minimum: math.Copysign(0, -1)}, "$ minimum must be a finite non-negative-zero JSON number"},
		{"numeric/maximum-nan", toolInputSchemaValue{kind: ToolSchemaNumber, hasMaximum: true, maximum: math.NaN()}, "$ maximum must be a finite non-negative-zero JSON number"},
		{"numeric/order", toolInputSchemaValue{kind: ToolSchemaInteger, hasMinimum: true, minimum: 2, hasMaximum: true, maximum: 1}, "$ minimum must be <= maximum"},
		{"bounds/length-minimum", toolInputSchemaValue{kind: ToolSchemaString, hasMinLength: true, minLength: -1, pattern: "["}, "$ minLength must be non-negative"},
		{"bounds/length-maximum", toolInputSchemaValue{kind: ToolSchemaString, hasMaxLength: true, maxLength: -1}, "$ maxLength must be non-negative"},
		{"bounds/length-order", toolInputSchemaValue{kind: ToolSchemaString, hasMinLength: true, minLength: 2, hasMaxLength: true, maxLength: 1}, "$ minLength must be <= maxLength"},
		{"bounds/items-minimum", toolInputSchemaValue{kind: ToolSchemaArray, hasMinItems: true, minItems: -1}, "$ minItems must be non-negative"},
		{"bounds/items-maximum", toolInputSchemaValue{kind: ToolSchemaArray, hasMaxItems: true, maxItems: -1}, "$ maxItems must be non-negative"},
		{"bounds/items-order", toolInputSchemaValue{kind: ToolSchemaArray, hasMinItems: true, minItems: 2, hasMaxItems: true, maxItems: 1}, "$ minItems must be <= maxItems"},
		{"string/utf8", toolInputSchemaValue{kind: ToolSchemaString, pattern: string([]byte{0xff})}, "$ pattern is not valid UTF-8"},
		{"string/regex", toolInputSchemaValue{kind: ToolSchemaString, pattern: "["}, "$ pattern is invalid: error parsing regexp: missing closing ]: `[`"},
		{"array/missing-items", toolInputSchemaValue{kind: ToolSchemaArray}, "$ array requires items"},
		{"array/zero-items", toolInputSchemaValue{kind: ToolSchemaArray, hasItems: true}, "$.items schema is missing"},
		{"property-names-definition/before-additional", toolInputSchemaValue{kind: ToolSchemaObject, hasPropertyNames: true, additionalAllowedDeclared: true, hasAdditionalSchema: true}, "$.propertyNames schema is missing"},
		{"object-order/additional-union", toolInputSchemaValue{kind: ToolSchemaObject, additionalAllowedDeclared: true, hasAdditionalSchema: true}, "$ additionalProperties must declare a boolean or schema, not both"},
		{"object-order/noncanonical-property", toolInputSchemaValue{kind: ToolSchemaObject, properties: map[string]ToolInputSchema{" x": text}, required: []string{"missing"}}, `$ property name " x" is not canonical`},
		{"object-order/property-before-required", toolInputSchemaValue{kind: ToolSchemaObject, properties: map[string]ToolInputSchema{"x": {}}, required: []string{"missing"}}, "$.properties[x] schema is missing"},
		{"equality-order/target-before-required", toolInputSchemaValue{kind: ToolSchemaObject, properties: map[string]ToolInputSchema{"x": {value: &toolInputSchemaValue{kind: ToolSchemaString, equalTo: "missing"}}}, required: []string{"also-missing"}}, `$ property "x" x-swarm-equalTo target "missing" is not declared`},
		{"object-order/noncanonical-required", toolInputSchemaValue{kind: ToolSchemaObject, required: []string{" x"}}, `$ required property name " x" is not canonical`},
		{"object-order/duplicate-required", toolInputSchemaValue{kind: ToolSchemaObject, properties: map[string]ToolInputSchema{"x": text}, required: []string{"x", "x"}}, `$ required property "x" is duplicated`},
		{"object-order/undeclared-required", toolInputSchemaValue{kind: ToolSchemaObject, required: []string{"x"}, hasAdditionalSchema: true}, `$ required property "x" is not declared`},
		{"object-order/additional-child", toolInputSchemaValue{kind: ToolSchemaObject, hasAdditionalSchema: true}, "$.additionalProperties schema is missing"},
		{"enum/empty", toolInputSchemaValue{kind: ToolSchemaString, enumDeclared: true}, "$ enum must contain at least one value"},
		{"property-names-definition/non-string-vocabulary", toolInputSchemaValue{kind: ToolSchemaObject, hasPropertyNames: true, propertyNames: integer}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertFactoringSchemaError(t, (ToolInputSchema{value: &tc.value}).ValidateDefinition(), tc.want)
		})
	}
	t.Run("entrance", func(t *testing.T) {
		assertFactoringSchemaError(t, (ToolInputSchema{}).ValidateDefinition(), "$ schema is missing")
		_, err := NewToolInputSchema(ToolSchemaString, nil)
		assertFactoringSchemaError(t, err, "tool schema option is nil")
		_, err = NewToolInputSchema(ToolSchemaString, ToolSchemaFormat("invalid"))
		assertFactoringSchemaError(t, err, `unsupported schema format "invalid"`)
	})
	t.Run("graph", func(t *testing.T) {
		for _, arm := range []string{"items", "oneOf", "propertyNames", "additionalProperties"} {
			t.Run(arm, func(t *testing.T) {
				cycle := ToolInputSchema{value: &toolInputSchemaValue{kind: ToolSchemaObject}}
				path := "$." + arm
				switch arm {
				case "items":
					cycle.value.kind, cycle.value.hasItems, cycle.value.items = ToolSchemaArray, true, cycle
				case "oneOf":
					cycle.value.oneOf = []ToolInputSchema{cycle}
					path += "[0]"
				case "propertyNames":
					cycle.value.hasPropertyNames, cycle.value.propertyNames = true, cycle
				case "additionalProperties":
					cycle.value.hasAdditionalSchema, cycle.value.additionalSchema = true, cycle
				}
				active := map[*toolInputSchemaValue]struct{}{}
				assertFactoringSchemaError(t, validateAdmittedToolInputSchemaActive("$", cycle, 0, false, active), path+" contains a schema cycle")
				if len(active) != 0 {
					t.Fatal("failed recursive admission retained active membership")
				}
			})
		}
		dag := MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"x": text, "y": text}))
		assertFactoringSchemaError(t, validateAdmittedToolInputSchema("$", dag, MaxToolInputSchemaDepth-1), "")
		assertFactoringSchemaError(t, validateAdmittedToolInputSchema("$", text, MaxToolInputSchemaDepth+1), "$ exceeds maximum schema depth 64")
	})
	t.Run("oneof-graph-enum", func(t *testing.T) {
		_, err := NewToolInputSchema(ToolSchemaString, ToolSchemaEnum("same", "same"))
		assertFactoringSchemaError(t, err, "$ enum[1] duplicates another semantic value")
		_, err = NewToolInputSchema(ToolSchemaInteger, ToolSchemaEnum("wrong"))
		assertFactoringSchemaError(t, err, "$.enum[0] must be integer")
		_, err = NewToolInputSchema(ToolSchemaAny, ToolSchemaOneOf(text, integer), ToolSchemaEnum(true))
		assertFactoringSchemaError(t, err, "$.enum[0] must match exactly one oneOf branch (matched 0)")
	})
}

func TestFactoringSchemaValue(t *testing.T) {
	text := MustToolInputSchema(ToolSchemaString)
	integer := MustToolInputSchema(ToolSchemaInteger)
	one := MustToolInputSchema(ToolSchemaAny, ToolSchemaOneOf(text, integer))
	names := MustToolInputSchema(ToolSchemaObject, ToolSchemaPropertyNames(MustToolInputSchema(ToolSchemaString, ToolSchemaEnum("ok"))), ToolSchemaAdditionalPropertiesSchema(integer))
	equality, err := AdmitToolInputSchemaMap(map[string]any{"type": "object", "properties": map[string]any{"a": map[string]any{"type": "any", "x-swarm-equalTo": "b"}, "b": map[string]any{}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		schema ToolInputSchema
		input  any
		want   string
	}{
		{"enum-kind", MustToolInputSchema(ToolSchemaString, ToolSchemaEnum("yes")), true, "$ is not one of the declared enum values"},
		{"string-order/type", text, 7, "$ must be string"},
		{"string-order/format-before-pattern", MustToolInputSchema(ToolSchemaString, ToolSchemaFormat("uuid"), ToolSchemaPattern("^x$")), "bad", "$ must be uuid"},
		{"string-order/time", MustToolInputSchema(ToolSchemaString, ToolSchemaFormat("date-time")), "bad", "$ must be RFC3339 date-time"},
		{"string-order/pattern-before-length", MustToolInputSchema(ToolSchemaString, ToolSchemaPattern("^x$"), ToolSchemaMinLength(3)), "z", `$ does not match pattern "^x$"`},
		{"string-order/runes", MustToolInputSchema(ToolSchemaString, ToolSchemaMinLength(2)), "\u00e9", "$ length must be >= 2"},
		{"string-order/maximum", MustToolInputSchema(ToolSchemaString, ToolSchemaMaxLength(1)), "xx", "$ length must be <= 1"},
		{"scalar/number", MustToolInputSchema(ToolSchemaNumber), "1", "$ must be number"},
		{"scalar/integer", integer, 1.5, "$ must be integer"},
		{"scalar/minimum", MustToolInputSchema(ToolSchemaNumber, ToolSchemaMinimum(2)), 1, "$ must be >= 2"},
		{"scalar/maximum", MustToolInputSchema(ToolSchemaNumber, ToolSchemaMaximum(2)), 3, "$ must be <= 2"},
		{"scalar/boolean", MustToolInputSchema(ToolSchemaBoolean), "true", "$ must be boolean"},
		{"scalar/null", MustToolInputSchema(ToolSchemaNull), false, "$ must be null"},
		{"scalar/any", MustToolInputSchema(ToolSchemaAny), map[string]any{"a": []any{true, nil, 1}}, ""},
		{"array-order/type", MustToolInputSchema(ToolSchemaArray, ToolSchemaItems(integer)), true, "$ must be array"},
		{"array-order/cardinality-before-item", MustToolInputSchema(ToolSchemaArray, ToolSchemaItems(integer), ToolSchemaMinItems(2)), []any{"bad"}, "$ length must be >= 2"},
		{"array-order/maximum", MustToolInputSchema(ToolSchemaArray, ToolSchemaItems(integer), ToolSchemaMaxItems(1)), []any{1, 2}, "$ length must be <= 1"},
		{"array-order/item", MustToolInputSchema(ToolSchemaArray, ToolSchemaItems(integer)), []any{1, "bad"}, "$[1] must be integer"},
		{"object-order/type", names, false, "$ must be object"},
		{"object-order/required-before-member", MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"x": integer}), ToolSchemaRequired("x"), ToolSchemaAdditionalPropertiesAllowed(false)), map[string]any{"extra": true}, "$.x is required"},
		{"object-order/closed", MustToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesAllowed(false)), map[string]any{"extra": true}, "$.extra is not allowed"},
		{"object-order/open", MustToolInputSchema(ToolSchemaObject), map[string]any{"extra": true}, ""},
		{"property-names-value-order/key-before-value", names, map[string]any{"bad": "also bad"}, "$[key=bad] is not one of the declared enum values"},
		{"property-names-value-order/empty-key", names, map[string]any{"": 1}, "$[key=] is not one of the declared enum values"},
		{"property-names-composition/member", names, map[string]any{"ok": "bad"}, "$.ok must be integer"},
		{"property-names-composition/valid", names, map[string]any{"ok": 1}, ""},
		{"equality/optional-source", equality, map[string]any{"b": 1}, ""},
		{"equality/missing-target", equality, map[string]any{"a": 1}, "$.a must equal $.b, but target is missing"},
		{"equality/semantic-not-stringified", equality, map[string]any{"a": 1, "b": "1"}, "$.a must equal $.b"},
		{"equality/semantic-object", equality, map[string]any{"a": map[string]any{"x": 1}, "b": map[string]any{"x": int64(1)}}, ""},
		{"oneof-count-order/zero", one, true, "$ must match exactly one oneOf branch (matched 0)"},
		{"oneof-count-order/single", one, "x", ""},
		{"oneof-count-order/multiple-before-type", MustToolInputSchema(ToolSchemaInteger, ToolSchemaOneOf(text, text)), "x", "$ must match exactly one oneOf branch (matched 2)"},
		{"oneof-recursion/branch-enum", MustToolInputSchema(ToolSchemaAny, ToolSchemaOneOf(MustToolInputSchema(ToolSchemaString, ToolSchemaEnum("x")), integer)), "y", "$ must match exactly one oneOf branch (matched 0)"},
	} {
		t.Run(tc.name, func(t *testing.T) { assertFactoringSchemaError(t, tc.schema.Validate(tc.input), tc.want) })
	}
}

func TestFactoringSchemaProjectionIsolation(t *testing.T) {
	text := MustToolInputSchema(ToolSchemaString, ToolSchemaEnum("first", "second"))
	schema := MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"x": text}), ToolSchemaRequired("x"),
		ToolSchemaPropertyNames(MustToolInputSchema(ToolSchemaString, ToolSchemaPattern("^x$"))),
		ToolSchemaOneOf(MustToolInputSchema(ToolSchemaObject)), ToolSchemaAdditionalPropertiesAllowed(false))
	before, err := schema.CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	projection := schema.Projection()
	if got := projection["properties"].(map[string]any)["x"].(map[string]any)["enum"]; !reflect.DeepEqual(got, []any{"first", "second"}) {
		t.Fatalf("authored enum order = %#v", got)
	}
	roundtrip, err := AdmitToolInputSchemaMap(projection)
	if err != nil || !schema.Equal(roundtrip) {
		t.Fatalf("projection roundtrip: %v", err)
	}
	projection["properties"].(map[string]any)["x"].(map[string]any)["enum"] = []any{"changed"}
	projection["propertyNames"].(map[string]any)["pattern"] = "changed"
	projection["oneOf"].([]any)[0].(map[string]any)["type"] = "string"
	properties := schema.Properties()
	properties["x"] = MustToolInputSchema(ToolSchemaNull)
	required := schema.RequiredProperties()
	required[0] = "changed"
	branches, _ := schema.OneOfSchemas()
	branches[0] = text
	enum, _ := text.EnumValues()
	enum[0], _ = semanticvalue.String("changed")
	after, err := schema.CanonicalHash()
	if err != nil || before != after {
		t.Fatalf("readback mutated canonical identity: %s -> %s (%v)", before, after, err)
	}
	assertFactoringSchemaError(t, schema.Validate(map[string]any{"x": "first"}), "")
	assertFactoringSchemaError(t, schema.Validate(map[string]any{"x": "changed"}), "$.x is not one of the declared enum values")
	t.Run("reentry-after-error", func(t *testing.T) {
		active := map[*toolInputSchemaValue]struct{}{}
		bad := ToolInputSchema{value: &toolInputSchemaValue{kind: ToolSchemaArray, hasItems: true}}
		assertFactoringSchemaError(t, validateAdmittedToolInputSchemaActive("$", bad, 0, false, active), "$.items schema is missing")
		bad.value.items = text
		assertFactoringSchemaError(t, validateAdmittedToolInputSchemaActive("$", bad, 0, false, active), "")
		if len(active) != 0 {
			t.Fatal(fmt.Sprint("active map leaked: ", active))
		}
	})
}
