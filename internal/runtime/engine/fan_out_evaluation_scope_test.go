package engine

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func TestFanOutEvaluationScopeFreshAndMixedFields(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		name := "cel"
		if mixed {
			name = "mixed"
		}
		t.Run(name, func(t *testing.T) {
			exec, intent, trigger, _ := preparedFanOutFixture(t)
			p, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
			if err != nil {
				t.Fatal(err)
			}
			if mixed {
				p.emit.Fields["count"] = rc.LiteralExpression(int64(33))
			}
			for _, ordinal := range []int{0, 1, 0} {
				emit, err := p.EvaluateOrdinal(context.Background(), int64(ordinal+10), ordinal)
				if err != nil {
					t.Fatal(err)
				}
				var payload preparedFanOutPayload
				if err := json.Unmarshal(emit.Event.Payload(), &payload); err != nil || payload.Value != int64(ordinal+15) || payload.Index != int64(ordinal) || payload.Count != 33 {
					t.Fatalf("ordinal reused activation: %+v %v", payload, err)
				}
			}
		})
	}
}

func TestFanOutEvaluationScopeDefersProjectionUntilFieldChecks(t *testing.T) {
	exec, intent, trigger, _ := preparedFanOutFixture(t)
	p, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
	if err != nil {
		t.Fatal(err)
	}
	p.emit.Fields = map[string]rc.ExpressionValue{"value": p.emit.Fields["value"]}
	key := fanOutFieldKey{"value", p.emit.Fields["value"].CEL, p.emit.Fields["value"].Kind}
	field := p.fields[key]
	sentinel := errors.New("retained preparation diagnostic")
	field.err = sentinel
	p.fields[key] = field
	if _, err := p.EvaluateOrdinal(context.Background(), math.Inf(1), 0); !errors.Is(err, sentinel) {
		t.Fatalf("projection displaced preparation failure: %v", err)
	}
	field.err = nil
	p.fields[key] = field
	if _, err := p.EvaluateOrdinal(context.Background(), math.Inf(1), 0); err == nil {
		t.Fatal("hostile input passed")
	} else {
		var projection *workflowexpr.CELProjectionError
		if !errors.As(err, &projection) || !strings.HasPrefix(err.Error(), "emit field value:") {
			t.Fatalf("projection diagnostic changed: %T %v", err, err)
		}
	}
}

func TestFanOutEvaluationScopeExactWrapperDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, expression string
		item             any
	}{
		{"projection", "row + entity.offset", math.Inf(1)},
		{"evaluation", "row / 0", int64(1)},
		{"normalization", "row + entity.offset", int64(9007199254740991)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, intent, trigger, _ := preparedFanOutFixture(t)
			p, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
			if err != nil {
				t.Fatal(err)
			}
			original := p.emit.Fields["value"]
			field := p.fields[fanOutFieldKey{"value", original.CEL, original.Kind}]
			expression := rc.CELExpression(tc.expression)
			field.program, field.err = workflowexpr.PrepareValueExpression(expression.CEL, field.options)
			if field.err != nil {
				t.Fatal(field.err)
			}
			p.fields[fanOutFieldKey{"value", expression.CEL, expression.Kind}] = field
			p.emit.Fields = map[string]rc.ExpressionValue{"value": expression}
			_, got := p.EvaluateOrdinal(context.Background(), tc.item, 0)
			// A harmless literal selects the unchanged per-field path. It cannot
			// fail or change the sole CEL field's input, regardless of map order.
			p.emit.Fields["legacy_literal"] = rc.LiteralExpression(true)
			_, want := p.EvaluateOrdinal(context.Background(), tc.item, 0)
			if got == nil || want == nil || got.Error() != want.Error() {
				t.Fatalf("wrapper text changed: got %v; want %v", got, want)
			}
			for left, right := got, want; left != nil || right != nil; left, right = errors.Unwrap(left), errors.Unwrap(right) {
				if reflect.TypeOf(left) != reflect.TypeOf(right) {
					t.Fatalf("diagnostic chain changed: got %T; want %T", left, right)
				}
			}
		})
	}
}
