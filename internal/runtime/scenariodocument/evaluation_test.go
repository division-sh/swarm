package scenariodocument

import (
	"math"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

func TestAuthoredExpressionResultsStayDataAcrossStages(t *testing.T) {
	evaluator, err := NewEvaluator(Seed("tests/once.yaml", "once", "seed"), map[string]any{"marker": "${'${1 + 1}'}"})
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range []any{
		map[string]any{"id": "${'${1 + 1}'}"},
		map[string]any{"set": map[string]any{"id": "${'${1 + 1}'}"}},
		"${{'id': '${1 + 1}'}}",
		map[string]any{"id": "${vars.marker}"},
	} {
		data, err := PreparePayload(spec, evaluator, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, err := evaluator.Evaluate(data)
		if err != nil || !reflect.DeepEqual(got, map[string]any{"id": "${1 + 1}"}) {
			t.Fatalf("%#v -> %#v, %v", spec, got, err)
		}
		again, err := PreparePayload(data, evaluator, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, err = again.Interface()
		if err != nil || got.(map[string]any)["id"] != "${1 + 1}" {
			t.Fatalf("rescan: %#v, %v", got, err)
		}
	}
	data, err := PreparePayload("${{'from': 'literal', 'set': {'id': '${1+1}'}, 'nested': ['${2+2}']}}", evaluator, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := data.Interface()
	want := map[string]any{"from": "literal", "set": map[string]any{"id": "${1+1}"}, "nested": []any{"${2+2}"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("expression data controls interpreted: %#v, %v", got, err)
	}
	got.(map[string]any)["nested"].([]any)[0] = "mutated"
	copy, _ := data.Interface()
	if !reflect.DeepEqual(copy, want) {
		t.Fatal("materialized carrier is mutable")
	}
}

func TestFixtureAndInlineUseSameDeterministicContext(t *testing.T) {
	for _, raw := range []string{
		"id: '${scenario.uuid(\"id\")}'\nvalue: '${vars.n + 1}'\nmarker: \"${'${1 + 1}'}\"\n",
		`{"id":"${scenario.uuid('id')}","value":"${vars.n + 1}","marker":"${'${1 + 1}'}"}`,
	} {
		fixture, err := AdmitFixture([]byte(raw), "tests/fixture.yaml")
		if err != nil {
			t.Fatal(err)
		}
		evaluator, err := NewEvaluator(Seed("tests/context.yaml", "context", "fixed"), map[string]any{"n": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		inline, err := PreparePayload(map[string]any{"id": "${scenario.uuid('id')}", "value": "${vars.n + 1}", "marker": "${'${1 + 1}'}"}, evaluator, nil)
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := PreparePayload(map[string]any{"from": "fixture.yaml"}, evaluator, func(string) (semanticvalue.Value, error) { return fixture, nil })
		if err != nil {
			t.Fatal(err)
		}
		one, _ := inline.Interface()
		two, _ := loaded.Interface()
		if !reflect.DeepEqual(one, two) {
			t.Fatalf("fixture = %#v, inline = %#v", two, one)
		}
	}
}

func TestCompoundProjectionPreservesKindsAndRejectsInvalidData(t *testing.T) {
	evaluator, err := NewEvaluator("projection", nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := evaluator.Evaluate("${{'i': 7, 'u': 7u, 'd': 7.0, 'items': [null, {'text': '${2+2}'}]}}")
	want := map[string]any{"i": int64(7), "u": int64(7), "d": float64(7), "items": []any{nil, map[string]any{"text": "${2+2}"}}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("projection = %#v, %v", got, err)
	}
	for _, value := range []any{math.Inf(1), math.NaN(), int64(9007199254740993), make(chan int)} {
		if _, err := Materialize(map[string]any{"nested": []any{value}}); err == nil {
			t.Fatalf("accepted %T %#v", value, value)
		}
	}
	for _, expression := range []string{"${unknown()}", "${}", "${1 / 0}"} {
		if _, err := evaluator.Evaluate(expression); err == nil {
			t.Fatalf("accepted %s", expression)
		}
	}
}

func TestPreparationClosesAllExpressionSitesBeforeExecution(t *testing.T) {
	doc, err := Admit([]byte("setup: {entities: [{as: item, type: task, fields: \"${{'text': '${1+1}'} }\"}]}\n"+ordinary+"expect: {entities: [{ref: item, fields: \"${{'text': '${2+2}'} }\"}]}\n"), "tests/prepare.yaml")
	if err != nil {
		t.Fatal(err)
	}
	projection, _ := doc.Projection()
	evaluator, _ := NewEvaluator("prepare", nil)
	prepared, err := PrepareCLI(projection, evaluator, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{prepared.Setup.Entities[0].Fields, prepared.Expect.Entities[0].Fields, prepared.Steps[0].Payload} {
		if _, ok := value.(Materialized); !ok {
			t.Fatalf("unprepared value: %T", value)
		}
	}
	projection.Steps[0].IdempotencyKey = "${7}"
	if _, err := PrepareCLI(projection, evaluator, nil); err == nil {
		t.Fatal("non-text identity accepted")
	}
}
