package engine

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeaccumulator "github.com/division-sh/swarm/internal/runtime/accumulator"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func scopedComponentAccumulatorSource(t *testing.T) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	for path, text := range map[string]string{
		"manifest.yaml":           "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":             "connect: []\n",
		"operating/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"operating/schema.yaml":   "stages:\n  pending: {}\ninputs: [component.scaffolded]\n",
		"operating/state.yaml":    "items: [Component]\ncomponent_count: integer\n",
		"operating/types.yaml":    "Component:\n  component_id: text\n",
		"operating/events.yaml":   "component.scaffolded:\n  _embed: [Component]\n",
	} {
		writeEngineProjectionFixtureFile(t, filepath.Join(root, path), text)
	}
	repo := repoRootForEngineProjectionTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func a2AccumulatorExecutor(t *testing.T) (*Executor, *persistentStateRepo) {
	t.Helper()
	itemFields := map[string]runtimecontracts.TypeFieldSpec{
		"id": {Type: "text"}, "marker": {Type: "text"},
		"event_id": {Type: "text", IsOptional: true}, "event_type": {Type: "text", IsOptional: true},
		"source": {Type: "text", IsOptional: true}, "received_at": {Type: "text", IsOptional: true},
		"payload": {Type: "BusinessRecord", IsOptional: true},
	}
	source := mustCompileEngineSource(&runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{"state": {Fields: map[string]runtimecontracts.EntityFieldDecl{
			"marker": {Type: "text"}, "items": {Type: "[ArrivalItem]"},
		}}},
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"BusinessRecord": {Fields: map[string]runtimecontracts.TypeFieldSpec{"id": {Type: "text"}, "marker": {Type: "text"}}},
			"ArrivalItem":    {Fields: itemFields},
		}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.received": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
				"id": {Type: "text"}, "marker": {Type: "text"}, "event_id": {Type: "text"}, "event_type": {Type: "text"},
				"source": {Type: "text"}, "received_at": {Type: "text"}, "payload": {Type: "BusinessRecord"},
			}, Required: []string{"id", "marker"}}},
		},
	})
	repo := &persistentStateRepo{found: true, snapshot: StateSnapshot{CurrentState: "active", StateCarrier: NewStateCarrier(map[string]any{}, nil)}}
	executor, err := NewExecutor(RuntimeDependencies{
		Source: sourceWithFixtureStages(source, ".", "active", "active"), StateRepo: repo,
		MutationOwner: stubMutationOwner{state: repo}, Locker: stubLocker{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return executor, repo
}

func a2AccumulatorRequest(t *testing.T, key, payload, eventID string) ExecutionRequest {
	t.Helper()
	return ExecutionRequest{
		EntityID: "entity-1", Node: testRootExecutableNode(t, "collector"), HandlerEventKey: "item.received",
		Event: eventtest.RunCreatingRootIngress(eventID, "item.received", "transport-source", "", json.RawMessage(payload), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Accumulate:       &runtimecontracts.AccumulateSpec{Target: "items", Key: key},
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{SourceField: "marker", TargetField: "marker"}}},
			Emit:             runtimecontracts.EmitSpec{Event: "item.recorded"},
		},
	}
}

func requireAccumulatorFailure(t *testing.T, err error, class failures.Class) {
	t.Helper()
	var failure *failures.Error
	if !errors.As(err, &failure) || failure.Failure.Class != class {
		t.Fatalf("error = %v, want %s", err, class)
	}
}

func TestA2M10M11AccumulatorDuplicateAndConflictStopEffects(t *testing.T) {
	executor, repo := a2AccumulatorExecutor(t)
	const original = `{"id":"item","marker":"first","event_id":"business-event","event_type":"business-type","source":"business-source","received_at":"business-time","payload":{"id":"nested","marker":"nested-marker"}}`
	first := a2AccumulatorRequest(t, "id", original, "event-1")
	result, err := executor.ExecuteSemanticFixture(context.Background(), first)
	if err != nil || len(result.EmitIntents) != 1 {
		t.Fatalf("first execution = %#v, %v", result, err)
	}
	beforeFields := cloneStringAnyMap(repo.snapshot.StateCarrier.Fields)
	beforeBuckets := cloneStateBucketSet(repo.snapshot.StateCarrier.StateBuckets)
	for _, tc := range []struct {
		name, payload string
		class         failures.Class
	}{
		{"reordered-same-payload", `{"payload":{"marker":"nested-marker","id":"nested"},"source":"business-source","received_at":"business-time","event_type":"business-type","event_id":"business-event","marker":"first","id":"item"}`, ""},
		{"changed-payload", `{"id":"item","marker":"changed","event_id":"business-event","event_type":"business-type","source":"business-source","received_at":"business-time","payload":{"id":"nested","marker":"nested-marker"}}`, failures.ClassConflictingDuplicate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := executor.ExecuteSemanticFixture(context.Background(), a2AccumulatorRequest(t, "id", tc.payload, "different-transport-event"))
			if tc.class != "" {
				requireAccumulatorFailure(t, err, tc.class)
			} else if err != nil || result.Status != OutcomeDiscarded {
				t.Fatalf("duplicate = %#v, %v", result, err)
			}
			if len(result.EmitIntents) != 0 || !reflect.DeepEqual(repo.snapshot.StateCarrier.Fields, beforeFields) || !reflect.DeepEqual(repo.snapshot.StateCarrier.StateBuckets, beforeBuckets) {
				t.Fatalf("refusal/duplicate changed state or effects: %#v", result)
			}
		})
	}
	acc := runtimeaccumulator.Load(repo.snapshot.StateCarrier.Fields["items"])
	if acc.Err() != nil || len(acc.Items) != 1 {
		t.Fatalf("accumulator = %#v", acc)
	}
	var expected map[string]any
	if err := json.Unmarshal([]byte(original), &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(acc.Items[0], expected) {
		t.Fatalf("transport decorated business item: %#v", acc.Items[0])
	}
}

func TestA2M12AccumulatorKeyRefusalHasNoStateOrEffects(t *testing.T) {
	for _, payload := range []string{`{"marker":"missing"}`, `{"id":null,"marker":"null"}`, `{"id":{"value":"record"},"marker":"record"}`, `{"id":["list"],"marker":"list"}`} {
		t.Run(payload, func(t *testing.T) {
			executor, repo := a2AccumulatorExecutor(t)
			before := cloneStateBucketSet(repo.snapshot.StateCarrier.StateBuckets)
			result, err := executor.ExecuteSemanticFixture(context.Background(), a2AccumulatorRequest(t, "id", payload, "not-a-business-key"))
			requireAccumulatorFailure(t, err, failures.ClassSchemaInvalid)
			if len(result.EmitIntents) != 0 || len(repo.snapshot.StateCarrier.Fields) != 0 || !reflect.DeepEqual(before, repo.snapshot.StateCarrier.StateBuckets) {
				t.Fatalf("invalid key changed state/effects: %#v %#v", repo.snapshot, result)
			}
		})
	}
}

func TestA2M15AccumulatorFilterReduceCountAndComputeUseBusinessItems(t *testing.T) {
	acc := &Accumulator{}
	spec := &runtimecontracts.AccumulateSpec{Target: "items", Key: "id"}
	for _, item := range []map[string]any{
		{"id": "a", "score": int64(8), "payload": map[string]any{"score": int64(0)}},
		{"id": "b", "score": int64(2), "payload": map[string]any{"score": int64(100)}},
	} {
		if _, err := acc.Admit(spec, item); err != nil {
			t.Fatal(err)
		}
	}
	path := paths.Parse("state.items")
	itemType := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Fields: []runtimecontracts.ResolvedCatalogField{
		{Name: "id", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}},
		{Name: "score", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeInteger}},
	}}
	resolution := runtimecontracts.WorkflowCollectionItemResolution{Kind: runtimecontracts.WorkflowCollectionSourcePath, Source: "state.items", Path: path, ItemType: itemType}
	frame := &executionFrame{
		ctx: context.Background(),
		req: ExecutionRequest{Handler: runtimecontracts.SystemNodeEventHandler{
			Filter: &runtimecontracts.FilterSpec{ItemsFrom: "state.items", ItemsPath: path, Condition: "item.score > 5", StoreAs: "computed.selected"},
			Reduce: &runtimecontracts.ReduceSpec{ItemsFrom: "state.items", ItemsPath: path, Operation: "count", StoreAs: "computed.reduced"},
			Count:  &runtimecontracts.CountSpec{ItemsFrom: "state.items", ItemsPath: path, StoreAs: "computed.counted"},
		}},
		state:          ExecutionState{State: StateSnapshot{StateCarrier: NewStateCarrier(map[string]any{"items": acc.Items}, nil)}, Computed: map[string]any{}},
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
