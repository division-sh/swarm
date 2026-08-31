package workflowexpr

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

func TestProjectCELValuePreservesLexicalNumberKind(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  any
	}{
		{name: "zero", value: json.Number("0"), want: int64(0)},
		{name: "negative integer", value: json.Number("-7"), want: int64(-7)},
		{name: "positive safe boundary", value: json.Number("9007199254740991"), want: int64(9007199254740991)},
		{name: "negative safe boundary", value: json.Number("-9007199254740991"), want: int64(-9007199254740991)},
		{name: "decimal integral", value: json.Number("7.0"), want: float64(7)},
		{name: "decimal fractional", value: json.Number("-7.25"), want: float64(-7.25)},
		{name: "positive exponent", value: json.Number("1e3"), want: float64(1000)},
		{name: "negative exponent", value: json.Number("5e-1"), want: float64(0.5)},
		{name: "native integer", value: int(12), want: int64(12)},
		{name: "native float remains float", value: float64(12), want: float64(12)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ProjectCELValue(test.value)
			if err != nil {
				t.Fatalf("ProjectCELValue(%#v): %v", test.value, err)
			}
			if reflect.TypeOf(got) != reflect.TypeOf(test.want) || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ProjectCELValue(%#v) = %#v (%T), want %#v (%T)", test.value, got, got, test.want, test.want)
			}
		})
	}
}

func TestProjectCELValueRecursesAndNamesHostilePath(t *testing.T) {
	projected, err := ProjectCELValue(map[string]any{
		"items": []any{
			map[string]any{"count": json.Number("2"), "score": json.Number("7.25")},
		},
	})
	if err != nil {
		t.Fatalf("ProjectCELValue nested value: %v", err)
	}
	want := map[string]any{"items": []any{map[string]any{"count": int64(2), "score": float64(7.25)}}}
	if !reflect.DeepEqual(projected, want) {
		t.Fatalf("ProjectCELValue nested value = %#v, want %#v", projected, want)
	}

	for _, hostile := range []json.Number{"not-a-number", "9007199254740992", "-9007199254740992"} {
		_, err := ProjectCELValue(map[string]any{"items": []any{map[string]any{"score": hostile}}})
		if err == nil {
			t.Fatalf("ProjectCELValue(%q) succeeded", hostile)
		}
		var projection *CELProjectionError
		if !errors.As(err, &projection) || projection.Path != "$.items[0].score" {
			t.Fatalf("ProjectCELValue(%q) error = %#v, want typed path $.items[0].score", hostile, err)
		}
		if !canonicaljson.IsAdmissionError(err) {
			t.Fatalf("ProjectCELValue(%q) error = %v, want canonical admission cause", hostile, err)
		}
		if strings.Contains(string(hostile), "9007199254740992") && !strings.Contains(err.Error(), "declare the field as string") {
			t.Fatalf("ProjectCELValue(%q) error = %v, want exact-integer remediation", hostile, err)
		}
	}
}

func TestEvalValueExpressionProjectsEveryRuntimeRootAndResult(t *testing.T) {
	ctx := ValueContext{
		Entity:         map[string]any{"value": json.Number("1")},
		PlatformEntity: map[string]any{"value": json.Number("2")},
		Event:          map[string]any{"id": "event-3"},
		Payload:        map[string]any{"value": json.Number("4")},
		Policy:         map[string]any{"value": json.Number("5")},
		Computed:       map[string]any{"value": json.Number("6")},
		FanOut:         map[string]any{"item": map[string]any{"value": json.Number("7")}},
		Join:           map[string]any{"completed": json.Number("8")},
		Loop:           map[string]any{"index": json.Number("9")},
	}
	value, err := EvalValueExpressionWithOptions(
		`[entity.value, _entity.value, event.id, payload.value, policy.value, computed.value, row.value, join.completed, _loop.index]`,
		ctx,
		ValueExpressionOptions{ItemAlias: "row", AllowJoin: true},
	)
	if err != nil {
		t.Fatalf("EvalValueExpressionWithOptions: %v", err)
	}
	want := []any{int64(1), int64(2), "event-3", int64(4), int64(5), int64(6), int64(7), int64(8), int64(9)}
	if !reflect.DeepEqual(value, want) {
		t.Fatalf("projected roots/result = %#v, want %#v", value, want)
	}
}

func TestEvalValueExpressionBareItemWithoutFanOutDoesNotPanic(t *testing.T) {
	if _, err := EvalValueExpressionWithOptions(`item`, ValueContext{}, ValueExpressionOptions{AllowBareItem: true}); err != nil {
		t.Fatalf("bare item without fan_out: %v", err)
	}
}
