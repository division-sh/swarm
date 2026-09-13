package entityquery

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestSparsePredicatePresence(t *testing.T) {
	for _, field := range []string{"score", "profile.score"} {
		for _, op := range []string{"==", "!=", ">", "<", ">=", "<="} {
			t.Run(field+op, func(t *testing.T) {
				if Matches(map[string]any{"fields": map[string]any{}}, Predicate{Field: field, Op: op, Value: 0}) {
					t.Fatal("unassigned field matched a concrete-value predicate")
				}
			})
		}
	}
	for _, value := range []any{0, "", false} {
		row := map[string]any{"fields": map[string]any{"value": value, "profile": map[string]any{"value": value}}, "current_state": "ready"}
		for _, field := range []string{"value", "profile.value"} {
			if !Matches(row, Predicate{Field: field, Op: "==", Value: value}) {
				t.Fatalf("present %#v did not match %s", value, field)
			}
		}
		if !Matches(row, Predicate{Field: "current_state", Op: "==", Value: "ready"}) {
			t.Fatal("row metadata selector stopped matching")
		}
	}
}

func TestSparsePredicateRejectsNullOperand(t *testing.T) {
	request := Request{
		RunID: "run", Source: semanticview.Wrap(&contracts.WorkflowContractBundle{}),
		Contract:  entityruntime.Contract{FlowID: "child"},
		Predicate: Predicate{Field: "score", Op: "=="},
	}
	if err := request.Validate(); err == nil {
		t.Fatal("null operand accepted as a presence predicate")
	}
	if Matches(map[string]any{"fields": map[string]any{}}, request.Predicate) {
		t.Fatal("missing field matched null operand")
	}
}
