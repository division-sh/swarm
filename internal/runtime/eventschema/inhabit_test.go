package eventschema

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestInhabitDeterministicallyCoversAcceptedSchemaVocabulary(t *testing.T) {
	schema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"bool":     map[string]any{"type": "boolean"},
			"count":    map[string]any{"type": "integer", "minimum": 2.2, "maximum": 5.0},
			"list":     map[string]any{"type": "array", "minItems": 2, "maxItems": 3, "items": map[string]any{"type": "string", "minLength": 3}},
			"nullable": map[string]any{"type": "string", "nullable": true},
			"nothing":  map[string]any{"type": "null"},
			"number":   map[string]any{"type": "number", "minimum": -5.0, "maximum": -2.5},
			"object": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"required": map[string]any{"type": "string"},
					"optional": map[string]any{"type": "string"},
				},
				"required": []any{"required"},
			},
		},
		"required": []any{"bool", "count", "list", "nullable", "nothing", "number", "object"},
	}

	first, err := InhabitDeterministically(schema, InhabitationContext{Identity: "scenario\x00flow\x00pin"})
	if err != nil {
		t.Fatalf("InhabitDeterministically first: %v", err)
	}
	second, err := InhabitDeterministically(schema, InhabitationContext{Identity: "scenario\x00flow\x00pin"})
	if err != nil {
		t.Fatalf("InhabitDeterministically second: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("inhabitants differ: first=%#v second=%#v", first, second)
	}
	want := map[string]any{
		"bool":     false,
		"count":    float64(3),
		"list":     []any{"000", "000"},
		"nullable": "",
		"nothing":  nil,
		"number":   -2.5,
		"object":   map[string]any{"required": ""},
	}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("inhabitant = %#v, want %#v", first, want)
	}
	if err := ValidateValueAgainstSchema(schema, first); err != nil {
		t.Fatalf("generated value failed canonical validation: %v", err)
	}
}

func TestInhabitDeterministicallyUsesOneCandidateBeforePatternValidation(t *testing.T) {
	tests := []struct {
		name   string
		schema map[string]any
		want   any
		fails  bool
	}{
		{
			name:   "ordinary candidate accidentally satisfies pattern",
			schema: map[string]any{"type": "string", "minLength": 3, "pattern": "^0+$"},
			want:   "000",
		},
		{
			name:   "ordinary candidate is not retried or searched",
			schema: map[string]any{"type": "string", "minLength": 1, "pattern": "^A$"},
			fails:  true,
		},
		{
			name:   "first authored enum is fully validated",
			schema: map[string]any{"type": "string", "enum": []any{"ABC", "XYZ"}, "pattern": "^[A-Z]+$"},
			want:   "ABC",
		},
		{
			name:   "format candidate is fully validated",
			schema: map[string]any{"type": "string", "format": "uuid", "pattern": "^[0-9a-f-]{36}$"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := InhabitDeterministically(tc.schema, InhabitationContext{Identity: "pattern-matrix"})
			if tc.fails {
				if err == nil || !strings.Contains(err.Error(), "$: pattern") || !strings.Contains(err.Error(), "provide an explicit fixture for this field") {
					t.Fatalf("InhabitDeterministically = %#v, error=%v, want exact-path explicit-fixture error", got, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("InhabitDeterministically: %v", err)
			}
			if tc.want != nil && !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("inhabitant = %#v, want %#v", got, tc.want)
			}
			if err := ValidateValueAgainstSchema(tc.schema, got); err != nil {
				t.Fatalf("canonical validation: %v", err)
			}
		})
	}
}

func TestInhabitantDoesNotOwnRegexInterpretation(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve inhabitant test path")
	}
	path := filepath.Join(filepath.Dir(file), "inhabit.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse inhabitant: %v", err)
	}
	for _, spec := range parsed.Imports {
		if strings.Trim(spec.Path.Value, `"`) == "regexp" {
			t.Fatal("deterministic inhabitant must not import or interpret regular expressions")
		}
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			name = fun.Name
		case *ast.SelectorExpr:
			name = fun.Sel.Name
		}
		if name == "Compile" || name == "MustCompile" || name == "MatchString" {
			t.Errorf("deterministic inhabitant owns forbidden regex operation %s", name)
		}
		return true
	})
}

func TestInhabitDeterministicallyPreservesSourceEnumOrder(t *testing.T) {
	schema := map[string]any{
		"type": "string",
		"enum": []any{"zeta", "alpha"},
	}
	got, err := InhabitDeterministically(schema, InhabitationContext{Identity: "enum-order"})
	if err != nil {
		t.Fatalf("InhabitDeterministically: %v", err)
	}
	if got != "zeta" {
		t.Fatalf("enum inhabitant = %#v, want first authored value zeta", got)
	}
	canonical := CanonicalAcceptanceSchema(schema)
	if reflect.DeepEqual(canonical["enum"], schema["enum"]) {
		t.Fatalf("fixture does not exercise canonical membership sorting: %#v", canonical["enum"])
	}
}

func TestInhabitDeterministicallyUsesContextForConstrainedWitnesses(t *testing.T) {
	schema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"id":   map[string]any{"type": "string", "format": "uuid"},
			"time": map[string]any{"type": "string", "format": "date-time"},
			"text": map[string]any{"type": "string", "minLength": 8, "maxLength": 8},
		},
		"required": []any{"id", "time", "text"},
	}
	left, err := InhabitDeterministically(schema, InhabitationContext{Identity: "scenario-a\x00pin-a"})
	if err != nil {
		t.Fatalf("left inhabitant: %v", err)
	}
	right, err := InhabitDeterministically(schema, InhabitationContext{Identity: "scenario-a\x00pin-b"})
	if err != nil {
		t.Fatalf("right inhabitant: %v", err)
	}
	if reflect.DeepEqual(left, right) {
		t.Fatalf("context-derived inhabitants unexpectedly match: %#v", left)
	}
	if err := ValidateValueAgainstSchema(schema, left); err != nil {
		t.Fatalf("left failed validation: %v", err)
	}
	if err := ValidateValueAgainstSchema(schema, right); err != nil {
		t.Fatalf("right failed validation: %v", err)
	}
}

func TestInhabitDeterministicallyResolvesEqualityDependencies(t *testing.T) {
	schema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"copy":   map[string]any{"type": "string", "x-swarm-equalTo": "source"},
			"source": map[string]any{"type": "string", "minLength": 6},
		},
		"required": []any{"copy"},
	}
	got, err := InhabitDeterministically(schema, InhabitationContext{Identity: "equality"})
	if err != nil {
		t.Fatalf("InhabitDeterministically: %v", err)
	}
	object := got.(map[string]any)
	if object["copy"] != object["source"] || object["copy"] == "" {
		t.Fatalf("equality inhabitant = %#v", object)
	}
	if err := ValidateValueAgainstSchema(schema, got); err != nil {
		t.Fatalf("equality inhabitant failed validation: %v", err)
	}
}

func TestInhabitDeterministicallyFailsClosedWithTeachingPaths(t *testing.T) {
	tests := []struct {
		name   string
		schema map[string]any
		want   []string
	}{
		{
			name:   "pattern requires explicit fixture",
			schema: map[string]any{"type": "object", "properties": map[string]any{"code": map[string]any{"type": "string", "pattern": "^[A-Z]{3}$"}}, "required": []any{"code"}},
			want:   []string{"$.properties[code]", "provide an explicit fixture for this field"},
		},
		{
			name:   "equality target missing",
			schema: map[string]any{"type": "object", "properties": map[string]any{"copy": map[string]any{"type": "string", "x-swarm-equalTo": "missing"}}, "required": []any{"copy"}},
			want:   []string{"$.properties[copy]", "missing equality target", "missing"},
		},
		{
			name: "equality cycle",
			schema: map[string]any{"type": "object", "properties": map[string]any{
				"a": map[string]any{"type": "string", "x-swarm-equalTo": "b"},
				"b": map[string]any{"type": "string", "x-swarm-equalTo": "a"},
			}, "required": []any{"a"}},
			want: []string{"$.properties[a]", "equality cycle"},
		},
		{
			name:   "empty enum",
			schema: map[string]any{"type": "string", "enum": []any{}},
			want:   []string{"$.enum", "at least one"},
		},
		{
			name:   "oversized collection",
			schema: map[string]any{"type": "array", "minItems": MaxInhabitedCollectionItems + 1, "items": map[string]any{"type": "string"}},
			want:   []string{"$.minItems", "provide an explicit fixture"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := InhabitDeterministically(tc.schema, InhabitationContext{Identity: "failure"})
			if err == nil {
				t.Fatalf("InhabitDeterministically = %#v, want error", got)
			}
			if got != nil {
				t.Fatalf("partial inhabitant = %#v", got)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %v, want containing %q", err, want)
				}
			}
		})
	}
}
