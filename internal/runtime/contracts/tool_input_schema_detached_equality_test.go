package contracts

import (
	"reflect"
	"strings"
	"testing"
)

func detachedEqualityObject(t *testing.T, kind, name string, peer any, requiredPeer bool) ToolInputSchema {
	t.Helper()
	required := []any{name}
	if requiredPeer {
		required = append(required, "peer")
	}
	return mustAdmitToolSchemaMap(t, map[string]any{
		"type": "object", "additionalProperties": false, "required": required,
		"properties": map[string]any{
			name:   map[string]any{"type": kind, "x-swarm-equalTo": "peer"},
			"peer": map[string]any{"type": kind, "enum": []any{peer}},
		},
	})
}

func TestSchemaRelation2630DetachedEqualityRelation(t *testing.T) {
	for _, kind := range []string{"any", "string", "integer"} {
		t.Run(kind, func(t *testing.T) {
			var approved, other any = "approved", "other"
			if kind == "integer" {
				approved, other = 1, 2
			}
			source := detachedEqualityObject(t, kind, "mode", approved, true)
			target := detachedEqualityObject(t, kind, "mode", other, false)
			witness := map[string]any{"mode": approved, "peer": approved}
			if err := source.Validate(witness); err != nil {
				t.Fatal(err)
			}
			if err := target.Validate(witness); err == nil {
				t.Fatal("target must reject the source-valid peer value")
			}
			if err := source.ValidateAssignableTo("whole", target); err == nil {
				t.Fatal("whole-object relation must prove the peer domain")
			}
			from, _ := source.Property("mode")
			to, _ := target.Property("mode")
			if err := from.ValidateAssignableTo("detached", to); err == nil || !strings.Contains(err.Error(), "requires its enclosing object relation") {
				t.Errorf("detached matching marker must refuse missing context: %v", err)
			}
			if err := source.ValidateAssignableTo("whole identity", source); err != nil {
				t.Fatal(err)
			}
			if err := from.ValidateAssignableTo("drop equality", MustToolInputSchema(ToolSchemaKind(kind))); err != nil {
				t.Fatal(err)
			}
			if err := MustToolInputSchema(ToolSchemaKind(kind)).ValidateAssignableTo("mismatch", to); err == nil || !strings.Contains(err.Error(), `source equality target ""`) {
				t.Fatalf("earlier mismatching marker refusal changed: %v", err)
			}
		})
	}
}

// This is the exact parsed carrier previously admitted by the public constructor.
// Fixed constructors cannot create it; seed it only to test defensive revalidation
// and the real projection's missing-peer error, not as a valid new entry.
func detachedEqualityPriorCarrier(t *testing.T, entry ToolSchemaEntry, output ToolInputSchema) ToolSchemaEntry {
	t.Helper()
	target, err := compileToolValuePath("projected", "")
	if err != nil {
		t.Fatal(err)
	}
	source, err := compileToolValuePath("result.payload.mode", "result")
	if err != nil {
		t.Fatal(err)
	}
	value := *entry.value
	value.hasCompiledResult = true
	value.compiledResult = ToolCompiledResultProjection{value: &compiledResultProjectionValue{
		fields: []compiledToolResultField{{target: target, source: source}}, outputSchema: output,
	}}
	return ToolSchemaEntry{value: &value}
}

func TestSchemaRelation2630DetachedEqualityEntrances(t *testing.T) {
	for _, kind := range []string{"any", "string", "integer"} {
		for _, entrance := range []string{"constructor", "with_schemas", "with_compiled_result"} {
			t.Run(kind+"/"+entrance, func(t *testing.T) {
				var approved, other any = "approved", "other"
				if kind == "integer" {
					approved, other = 1, 2
				}
				payload := detachedEqualityObject(t, kind, "mode", approved, true)
				source := additionalRelationObject("payload", payload)
				output := detachedEqualityObject(t, kind, "projected", other, false)
				input := MustToolInputSchema(ToolSchemaObject)
				options := []ToolSchemaEntryOption{WithToolHandler(ToolHandlerHTTP), WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.invalid"}), WithToolSchemas(input, source)}
				projection := CompiledResultProjection{Fields: map[string]CompiledResultField{"projected": {From: "result.payload.mode"}}, OutputSchema: output}
				unprojected := MustToolSchemaEntry(options...)
				prior := detachedEqualityPriorCarrier(t, unprojected, output)
				before, _ := prior.CompiledResult()
				witness := map[string]any{"payload": map[string]any{"mode": approved, "peer": approved}}
				if err := source.Validate(witness); err != nil {
					t.Fatal(err)
				}
				compiled, _ := prior.CompiledResultExecution()
				if result, err := compiled.Project(witness); err == nil || !strings.Contains(err.Error(), "target is missing") || result != nil {
					t.Fatalf("pre-repair projection must fail on missing peer: result=%v error=%v", result, err)
				}
				var got ToolSchemaEntry
				var err error
				switch entrance {
				case "constructor":
					got, err = NewToolSchemaEntry(append(options, WithToolCompiledResult(projection))...)
				case "with_schemas":
					got, err = prior.WithSchemas(input, source)
				case "with_compiled_result":
					got, err = unprojected.WithCompiledResult(projection)
				}
				if err == nil || !strings.Contains(err.Error(), "requires its enclosing object relation") || !got.IsZero() {
					t.Errorf("%s must refuse detached equality with zero authority: zero=%v error=%v", entrance, got.IsZero(), err)
				}
				after, _ := prior.CompiledResult()
				if !reflect.DeepEqual(before, after) || !prior.OutputSchema().Equal(source) {
					t.Fatal("revalidation mutated the prior carrier")
				}
				if _, present := unprojected.CompiledResult(); present {
					t.Fatal("unprojected entry acquired a result")
				}
				plain := additionalRelationObject("projected", MustToolInputSchema(ToolSchemaKind(kind)))
				projection.OutputSchema = plain
				valid, err := unprojected.WithCompiledResult(projection)
				if err != nil {
					t.Fatal(err)
				}
				accepted, _ := valid.CompiledResultExecution()
				if result, err := accepted.Project(witness); err != nil || !reflect.DeepEqual(result, map[string]any{"projected": approved}) {
					t.Fatalf("no-equality projection changed: result=%v error=%v", result, err)
				}
			})
		}
	}
}

func TestSchemaRelation2630CatalogEqualityGates(t *testing.T) {
	t.Run("detached_field", func(t *testing.T) {
		err := ValidateCatalogFieldSupply("catalog", map[string]any{"type": "string"}, EntityFieldDecl{Type: "text", Refinements: SchemaRefinements{EqualTo: "peer"}}, TypeCatalogDocument{})
		if err == nil || !strings.Contains(err.Error(), "catalog state schema") || !strings.Contains(err.Error(), "x-swarm-equalTo") {
			t.Fatalf("detached catalog target must fail definition admission: %v", err)
		}
	})
	t.Run("detached_map_key", func(t *testing.T) {
		keys := map[string]any{"type": "string", "x-swarm-equalTo": "peer"}
		object := map[string]any{"type": "object", "propertyNames": keys}
		if err := validateCatalogMapKeySupply("catalog", object, object); err == nil || !strings.Contains(err.Error(), "x-swarm-equalTo") {
			t.Fatalf("map-key earlier definition gate must refuse: %v", err)
		}
	})
	t.Run("nested_record", func(t *testing.T) {
		catalog := TypeCatalogDocument{Types: map[string]NamedTypeDecl{"Pair": {Fields: map[string]TypeFieldSpec{
			"mode": {Type: "text", Refinements: SchemaRefinements{EqualTo: "peer"}}, "peer": {Type: "text"},
		}}}}
		source := detachedEqualityObject(t, "string", "mode", "approved", true)
		if err := ValidateCatalogFieldSupply("catalog", source.Projection(), EntityFieldDecl{Type: "Pair"}, catalog); err != nil {
			t.Fatalf("whole record must preserve sibling proof: %v", err)
		}
	})
}
