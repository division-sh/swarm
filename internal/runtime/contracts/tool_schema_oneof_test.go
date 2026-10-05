package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func oneOfOperationTestSchema(t *testing.T) ToolInputSchema {
	t.Helper()
	snapshot, err := yamlsource.Load([]byte(`type: object
additionalProperties: false
properties:
  op: {type: string, enum: [set, append]}
  value: {}
required: [value]
oneOf:
  - type: object
    additionalProperties: false
    properties:
      op: {type: string, enum: [set]}
      value: {type: array, items: {type: string}}
    required: [value]
  - type: object
    additionalProperties: false
    properties:
      op: {type: string, enum: [append]}
      value: {type: string}
    required: [op, value]
`))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := AdmitToolInputSchemaValue(snapshot.Document("tools.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestToolInputSchemaOneOfAdmissionProjectionAndWrongBranch(t *testing.T) {
	schema := oneOfOperationTestSchema(t)
	projected := schema.Projection()
	roundtrip, err := AdmitToolInputSchemaMap(projected)
	if err != nil || !schema.Equal(roundtrip) {
		t.Fatalf("oneOf roundtrip: %v", err)
	}
	branches, declared := schema.OneOfSchemas()
	if !declared || len(branches) != 2 {
		t.Fatal("oneOf branches were lost")
	}
	branches[0] = MustToolInputSchema(ToolSchemaAny)
	projected["oneOf"].([]any)[0].(map[string]any)["additionalProperties"] = true
	for _, tc := range []struct {
		input any
		valid bool
	}{
		{map[string]any{"value": []any{"whole"}}, true},
		{map[string]any{"op": "set", "value": []any{"whole"}}, true},
		{map[string]any{"op": "append", "value": "element"}, true},
		{map[string]any{"op": "append", "value": []any{"wrong branch"}}, false},
		{map[string]any{"value": "missing op"}, false},
		{map[string]any{"op": "set", "value": "wrong branch"}, false},
		{map[string]any{"op": "append", "value": "element", "unknown": nil}, false},
	} {
		if err := schema.Validate(tc.input); (err == nil) != tc.valid {
			t.Fatalf("payload %#v valid=%v err=%v", tc.input, tc.valid, err)
		}
	}
	if err := schema.ValidateAssignableTo("same", roundtrip); err != nil {
		t.Fatal(err)
	}
	if err := MustToolInputSchema(ToolSchemaObject).ValidateAssignableTo("unconstrained source", schema); err == nil {
		t.Fatal("assignability ignored target oneOf")
	}
	anyWithEnum := MustToolInputSchema(ToolSchemaAny, ToolSchemaEnum(map[string]any{"value": []any{"only"}}))
	if err := schema.ValidateAssignableTo("any with enum", anyWithEnum); err == nil {
		t.Fatal("source union bypassed target Any enum constraint")
	}
}

func TestToolInputSchemaOneOfRejectsMalformedAndMultipleMatches(t *testing.T) {
	for _, raw := range []any{nil, []any{}, "branch", []any{nil}, []any{7}, []any{map[string]any{"type": "string", "unknown": true}}} {
		if _, err := AdmitToolInputSchemaMap(map[string]any{"type": "object", "oneOf": raw}); err == nil {
			t.Fatalf("malformed oneOf admitted: %#v", raw)
		}
	}
	if _, err := NewToolInputSchema(ToolSchemaObject, ToolSchemaOneOf()); err == nil {
		t.Fatal("empty oneOf builder admitted")
	}
	ambiguous := MustToolInputSchema(ToolSchemaString, ToolSchemaOneOf(MustToolInputSchema(ToolSchemaString), MustToolInputSchema(ToolSchemaString)))
	if err := ambiguous.Validate("both"); err == nil || !strings.Contains(err.Error(), "matched 2") {
		t.Fatalf("exclusive branch validation: %v", err)
	}
	for _, body := range []string{
		"type: object\noneOf: null\n",
		"type: object\noneOf: []\n",
		"type: object\noneOf: [{type: object, unknown: true}]\n",
		"type: object\noneOf: [{type: object, additionalProperties: false, properties: {value: {type: string}}, required: [missing]}]\n",
	} {
		snapshot, err := yamlsource.Load([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := AdmitToolInputSchemaValue(snapshot.Document("tools.yaml").Root()); err == nil {
			t.Fatalf("malformed authored oneOf admitted: %s", body)
		}
	}
	cycle := &toolInputSchemaValue{kind: ToolSchemaObject, oneOfDeclared: true}
	cycle.oneOf = []ToolInputSchema{{value: cycle}}
	if err := (ToolInputSchema{value: cycle}).ValidateDefinition(); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("oneOf cycle: %v", err)
	}
}
