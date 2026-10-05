package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func TestToolInputSchemaPropertyNamesAdmissionProjectionAndValidation(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("type: object\npropertyNames: {type: string, enum: [open, closed], minLength: 1}\nadditionalProperties: {type: integer}\n"))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := AdmitToolInputSchemaValue(snapshot.Document("map.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []map[string]any{{"open": int64(1)}, {"closed": int64(2)}, {}} {
		if err := schema.Validate(input); err != nil {
			t.Fatal(err)
		}
	}
	for _, input := range []map[string]any{{"unknown": int64(1)}, {"": int64(2)}, {"open": "wrong value"}} {
		if err := schema.Validate(input); err == nil {
			t.Fatalf("invalid map admitted: %#v", input)
		}
	}
	projected := schema.Projection()
	roundtrip, err := AdmitToolInputSchemaMap(projected)
	if err != nil || !schema.Equal(roundtrip) {
		t.Fatalf("map projection: %v", err)
	}
	projected["propertyNames"].(map[string]any)["enum"] = []any{"unknown"}
	if err := schema.Validate(map[string]any{"unknown": int64(1)}); err == nil {
		t.Fatal("projection mutated immutable name constraint")
	}
	body, err := yaml.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = yamlsource.Load(body)
	if err != nil {
		t.Fatal(err)
	}
	roundtrip, err = AdmitToolInputSchemaValue(snapshot.Document("roundtrip.yaml").Root())
	if err != nil || !schema.Equal(roundtrip) {
		t.Fatalf("YAML name projection: %s; %v", body, err)
	}
	unbounded := MustToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesSchema(MustToolInputSchema(ToolSchemaInteger)))
	if err := unbounded.ValidateAssignableTo("map names", schema); err == nil {
		t.Fatal("assignability ignored propertyNames")
	}
	if err := schema.ValidateAssignableTo("bounded names", unbounded); err != nil {
		t.Fatal(err)
	}
	closed := MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"unknown": MustToolInputSchema(ToolSchemaInteger)}), ToolSchemaAdditionalPropertiesAllowed(false))
	if err := closed.ValidateAssignableTo("closed bad name", schema); err == nil {
		t.Fatal("closed declared key bypassed name constraint")
	}
	if err := schema.ValidateAssignableTo("identical", roundtrip); err != nil {
		t.Fatal(err)
	}
}

func TestToolInputSchemaPropertyNamesRejectsMalformedAndCycles(t *testing.T) {
	for _, raw := range []any{nil, true, "text", map[string]any{"type": "string", "unknown": true}} {
		if _, err := AdmitToolInputSchemaMap(map[string]any{"type": "object", "propertyNames": raw}); err == nil {
			t.Fatalf("malformed propertyNames admitted: %#v", raw)
		}
	}
	if _, err := NewToolInputSchema(ToolSchemaString, ToolSchemaPropertyNames(MustToolInputSchema(ToolSchemaString))); err == nil {
		t.Fatal("nonobject name constraints admitted")
	}
	cyclic := ToolInputSchema{value: &toolInputSchemaValue{kind: ToolSchemaObject, hasPropertyNames: true}}
	cyclic.value.propertyNames = cyclic
	if err := cyclic.ValidateDefinition(); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("name schema cycle admitted: %v", err)
	}
}
