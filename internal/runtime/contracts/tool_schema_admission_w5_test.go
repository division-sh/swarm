package contracts

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestW5ToolSchemaSharedBounds(t *testing.T) {
	for _, field := range []string{"minLength", "maxLength", "minItems", "maxItems"} {
		kind := "string"
		if field == "minItems" || field == "maxItems" {
			kind = "array"
		}
		for _, value := range []any{-0.5, 1.0, 1.5, 2.5, -1, "1", nil, math.Inf(1), json.Number("1.0"), json.Number("1e0"), json.Number("1.5"), json.Number("-1"), json.Number("9223372036854775808"), json.Number("+1"), json.Number("01")} {
			raw := map[string]any{"type": kind, field: value}
			if kind == "array" {
				raw["items"] = map[string]any{"type": "string"}
			}
			if _, err := AdmitToolInputSchemaMap(raw); err == nil {
				t.Fatalf("%s=%v (%T) admitted", field, value, value)
			}
		}
	}
	for _, source := range []string{"1.5", "-0.5", "1.0", "null", "'1'", ".inf"} {
		snapshot, err := yamlsource.Load([]byte("type: string\nminLength: " + source + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := AdmitToolInputSchemaValue(snapshot.Document("tools.yaml").Root()); err == nil {
			t.Fatalf("authored minLength=%s admitted", source)
		}
	}
	for _, value := range []any{0, uint32(1), int64(2), json.Number("0"), json.Number("1")} {
		if _, err := AdmitToolInputSchemaMap(map[string]any{"type": "string", "minLength": value}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := AdmitToolInputSchemaMap(map[string]any{"type": "number", "minimum": -0.5, "maximum": 1.5}); err != nil {
		t.Fatal(err)
	}
}

func TestW5ToolAnyVersusOmittedObjectAcrossConsumers(t *testing.T) {
	anySchema, err := AdmitToolInputSchemaMap(map[string]any{})
	if err != nil || anySchema.Kind() != ToolSchemaAny {
		t.Fatalf("empty schema: %v %v", anySchema.Kind(), err)
	}
	objectSchema, err := AdmitToolInputSchemaMap(map[string]any{"type": "object"})
	if err != nil || objectSchema.Equal(anySchema) {
		t.Fatalf("object and any collapsed: %v", err)
	}
	if err := anySchema.Validate("text"); err != nil {
		t.Fatal(err)
	}
	if err := objectSchema.Validate("text"); err == nil {
		t.Fatal("object accepted text")
	}
	if _, err := AdmitToolInputSchemaMap(nil); err == nil {
		t.Fatal("missing schema became any")
	}
}
