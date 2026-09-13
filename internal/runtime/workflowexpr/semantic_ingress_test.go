package workflowexpr

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestSemanticIngressProjectionMatrix(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want any
	}{
		{"integer", `7`, int64(7)},
		{"integral_decimal", `7.0`, int64(7)},
		{"integral_exponent", `7e0`, int64(7)},
		{"canonical_exponent", `1e6`, int64(1000000)},
		{"fraction", `7.5`, float64(7.5)},
		{"fraction_exponent", `75e-1`, float64(7.5)},
		{"negative_integer", `-7.0`, int64(-7)},
		{"negative_fraction", `-7.5`, float64(-7.5)},
		{"zero", `0e0`, int64(0)},
		{"safe_max", `9007199254740991`, int64(9007199254740991)},
		{"safe_min", `-9007199254740991`, int64(-9007199254740991)},
		{"subnormal", `5e-324`, float64(5e-324)},
		{"nested", `{"a":[7,7.0,7e0,7.5,{"z":0.0}],"empty":{},"null":null}`, map[string]any{"a": []any{int64(7), int64(7), int64(7), float64(7.5), map[string]any{"z": int64(0)}}, "empty": map[string]any{}, "null": nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			admitted, err := canonicaljson.Decode([]byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			got, err := ProjectSemanticValue(admitted)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v (%T), want %#v (%T)", got, got, tc.want, tc.want)
			}
			raw, err := canonicaljson.MarshalPreservingNumberKinds(got)
			if err != nil {
				t.Fatal(err)
			}
			var carrier any
			if err := canonicaljson.DecodePreservingNumberLexemes(raw, &carrier); err != nil {
				t.Fatal(err)
			}
			reloaded, err := ProjectCELValue(carrier)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(reloaded, got) {
				t.Fatalf("transport %s changed kind: %#v -> %#v", raw, got, reloaded)
			}
			if _, ok := got.(int64); ok {
				payloadType := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Fields: []runtimecontracts.ResolvedCatalogField{
					{Name: "n", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeInteger}},
				}}
				result, err := EvalValueExpressionWithOptions(`payload.n + 0`, ValueContext{Payload: map[string]any{"n": got}}, ValueExpressionOptions{PayloadType: &payloadType})
				if err != nil || !reflect.DeepEqual(result, got) {
					t.Fatalf("integer execution = %#v, %v", result, err)
				}
			}
		})
	}
}

func TestProjectCELValueUsesCanonicalSemanticProjection(t *testing.T) {
	for _, raw := range []string{`7`, `7.0`, `7e0`, `{"n":[7.0,7e0,7.5]}`} {
		admitted, err := canonicaljson.Decode([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		want, err := ProjectSemanticValue(admitted)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ProjectCELValue(admitted)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("direct semantic %s = %#v, %v; want %#v", raw, got, err, want)
		}
		mixed, err := ProjectCELValue(map[string]any{"semantic": admitted, "double": float64(7)})
		if err != nil || !reflect.DeepEqual(mixed, map[string]any{"semantic": want, "double": float64(7)}) {
			t.Fatalf("mixed = %#v, %v", mixed, err)
		}
	}
	payloadType := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Fields: []runtimecontracts.ResolvedCatalogField{
		{Name: "delta", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeInteger}},
	}}
	result, err := EvalValueExpressionWithOptions(`double(payload.delta) + 1.0`, ValueContext{Payload: map[string]any{"delta": int64(7)}}, ValueExpressionOptions{PayloadType: &payloadType})
	if err != nil || !reflect.DeepEqual(result, float64(8)) {
		t.Fatalf("explicit double execution = %#v, %v", result, err)
	}
}
