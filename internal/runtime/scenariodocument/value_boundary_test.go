package scenariodocument

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

func TestAuthoredResultsRejectNonRuntimeCarriersAcrossPayloadForms(t *testing.T) {
	for _, expression := range []string{
		"${{1:'number','1':'text'}}", "${{true:'bool','true':'text'}}",
		"${{'nested': [{1:'number','1':'text'}]}}", "${{'value': b'abc'}}",
		"${{'value': timestamp('2026-01-01T00:00:00Z')}}", "${{'value': duration('1s')}}",
	} {
		t.Run(expression, func(t *testing.T) {
			if _, err := NewEvaluator("fixed", map[string]any{"value": expression}); err == nil {
				t.Fatal("unsupported vars result admitted")
			}
			evaluator, err := NewEvaluator("fixed", nil)
			if err != nil {
				t.Fatal(err)
			}
			fixture, err := AdmitFixture([]byte("value: \""+expression+"\"\n"), "tests/fixture.yaml")
			if err != nil {
				t.Fatal(err)
			}
			for _, spec := range []any{expression, map[string]any{"value": expression}, map[string]any{"set": map[string]any{"value": expression}}, map[string]any{"from": "fixture.yaml"}} {
				_, err := PreparePayload(spec, evaluator, func(string) (semanticvalue.Value, error) { return fixture, nil })
				if err == nil {
					t.Fatalf("unsupported result admitted through %#v", spec)
				}
			}
		})
	}
}

func TestMaterializedRuntimeValuesRejectDTOsAndIsolateCollections(t *testing.T) {
	for _, value := range []any{[]byte("abc"), map[string]string{"x": "y"}, []string{"x"}, struct{ Value string }{"x"}, make(chan int)} {
		_, err := Materialize(map[string]any{"nested": []any{value}})
		if err == nil {
			t.Fatalf("retained unsupported carrier %T", value)
		}
	}
	source := map[string]any{"nested": []any{map[string]any{"i": int64(7), "d": float64(7), "marker": "${1+1}"}}}
	data, err := Materialize(source)
	if err != nil {
		t.Fatal(err)
	}
	source["nested"].([]any)[0].(map[string]any)["i"] = int64(99)
	first, err := data.Interface()
	if err != nil {
		t.Fatal(err)
	}
	first.(map[string]any)["nested"].([]any)[0].(map[string]any)["d"] = float64(99)
	second, err := data.Interface()
	want := map[string]any{"nested": []any{map[string]any{"i": int64(7), "d": float64(7), "marker": "${1+1}"}}}
	if err != nil || !reflect.DeepEqual(second, want) {
		t.Fatalf("aliased or kind-erased materialization: %#v, %v", second, err)
	}
}

func TestAuthoredValidExactKeysRemainDeterministic(t *testing.T) {
	for i := 0; i < 100; i++ {
		evaluator, err := NewEvaluator("fixed", map[string]any{"choice": "${{'1':'number','true':'bool',' spaced ':'${1+1}'}}"})
		if err != nil {
			t.Fatal(err)
		}
		data, err := PreparePayload("${{'i':7,'d':7.0,'choice':vars.choice}}", evaluator, nil)
		if err != nil {
			t.Fatal(err)
		}
		value, err := data.Interface()
		want := map[string]any{"i": int64(7), "d": float64(7), "choice": map[string]any{"1": "number", "true": "bool", " spaced ": "${1+1}"}}
		if err != nil || !reflect.DeepEqual(value, want) {
			t.Fatalf("preparation %d: %#v, %v", i, value, err)
		}
	}
	if _, err := NewEvaluator("fixed", map[string]any{"bad": map[string]string{"key": "value"}}); err == nil || !strings.Contains(err.Error(), "vars.bad") {
		t.Fatalf("typed vars map admitted: %v", err)
	}
}
