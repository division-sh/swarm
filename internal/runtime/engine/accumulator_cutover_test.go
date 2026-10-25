package engine

import (
	"context"
	"errors"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func requireAccumulatorFailure(t *testing.T, err error, class failures.Class) {
	t.Helper()
	var failure *failures.Error
	if !errors.As(err, &failure) || failure.Failure.Class != class {
		t.Fatalf("error = %v, want %s", err, class)
	}
}

func TestA2M15AccumulatorFilterReduceCountAndComputeUseBusinessItems(t *testing.T) {
	acc := &Accumulator{}
	spec := &runtimecontracts.AccumulateSpec{Key: "id"}
	for _, item := range []map[string]any{
		{"id": "a", "score": int64(8), "payload": map[string]any{"score": int64(0)}},
		{"id": "b", "score": int64(2), "payload": map[string]any{"score": int64(100)}},
	} {
		if _, err := acc.Admit(spec, item); err != nil {
			t.Fatal(err)
		}
	}
	path := paths.Parse("accumulated.items")
	itemType := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Fields: []runtimecontracts.ResolvedCatalogField{
		{Name: "id", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}},
		{Name: "score", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeInteger}},
	}}
	resolution := runtimecontracts.WorkflowCollectionItemResolution{Kind: runtimecontracts.WorkflowCollectionSourcePath, Source: "accumulated.items", Path: path, ItemType: itemType}
	frame := &executionFrame{
		ctx: context.Background(),
		req: ExecutionRequest{Handler: runtimecontracts.SystemNodeEventHandler{
			Filter: &runtimecontracts.FilterSpec{ItemsFrom: "accumulated.items", ItemsPath: path, Condition: "item.score > 5", StoreAs: "computed.selected"},
			Reduce: &runtimecontracts.ReduceSpec{ItemsFrom: "accumulated.items", ItemsPath: path, Operation: "count", StoreAs: "computed.reduced"},
			Count:  &runtimecontracts.CountSpec{ItemsFrom: "accumulated.items", ItemsPath: path, StoreAs: "computed.counted"},
		}},
		state:          ExecutionState{Accumulated: accumulatorExpressionValue(acc), Computed: map[string]any{}},
		collectionPlan: runtimecontracts.WorkflowHandlerCollectionPlan{Filter: &resolution, Reduce: &resolution, Count: &resolution},
	}
	executor := &Executor{}
	if err := executor.stepFilter(frame); err != nil {
		t.Fatal(err)
	}
	if err := executor.stepReduce(frame); err != nil {
		t.Fatal(err)
	}
	if err := executor.stepCount(frame); err != nil {
		t.Fatal(err)
	}
	computed, err := computeValue(acc, nil, &runtimecontracts.ComputeSpec{Operation: runtimecontracts.ComputeOpCount})
	if err != nil || computed != 2 || frame.state.Computed["reduced"] != 2 || frame.state.Computed["counted"] != 2 {
		t.Fatalf("count consumers = %#v, compute=%v, error=%v", frame.state.Computed, computed, err)
	}
	selected := frame.state.Computed["selected"].([]any)
	if len(selected) != 1 || selected[0].(map[string]any)["id"] != "a" {
		t.Fatalf("filter reinterpreted nested payload: %#v", selected)
	}
}
