package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/accprojection"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

func a2AccumulatorExecutor(t *testing.T) (*Executor, *persistentStateRepo) {
	t.Helper()
	source := mustCompileEngineSource(&runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{"record": {Fields: map[string]runtimecontracts.EntityFieldDecl{"marker": {Type: "text"}}}},
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"BusinessRecord": {Fields: map[string]runtimecontracts.TypeFieldSpec{"id": {Type: "text"}, "marker": {Type: "text"}}},
		}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.received": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
				"id": {Type: "text"}, "marker": {Type: "text"}, "event_id": {Type: "text"}, "event_type": {Type: "text"}, "source": {Type: "text"}, "received_at": {Type: "text"}, "payload": {Type: "BusinessRecord"},
			}}},
		},
	})
	repo := &persistentStateRepo{found: true, snapshot: StateSnapshot{CurrentState: "active", StateCarrier: NewStateCarrier(map[string]any{}, nil, nil)}}
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
			Accumulate:       &runtimecontracts.AccumulateSpec{Into: "items", Key: key},
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
	first := a2AccumulatorRequest(t, "payload.id", original, "event-1")
	result, err := executor.ExecuteSemanticFixture(context.Background(), first)
	if err != nil || len(result.EmitIntents) != 1 {
		t.Fatalf("first execution = %#v, %v", result, err)
	}
	before := cloneStateBucketSet(repo.snapshot.StateCarrier.StateBuckets)
	for _, tc := range []struct {
		name, payload string
		class         failures.Class
	}{
		{"reordered-same-payload", `{"payload":{"marker":"nested-marker","id":"nested"},"source":"business-source","received_at":"business-time","event_type":"business-type","event_id":"business-event","marker":"first","id":"item"}`, ""},
		{"changed-payload", `{"id":"item","marker":"changed","event_id":"business-event","event_type":"business-type","source":"business-source","received_at":"business-time","payload":{"id":"nested","marker":"nested-marker"}}`, failures.ClassConflictingDuplicate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := executor.ExecuteSemanticFixture(context.Background(), a2AccumulatorRequest(t, "payload.id", tc.payload, "different-transport-event"))
			if tc.class != "" {
				requireAccumulatorFailure(t, err, tc.class)
			} else if err != nil || result.Status != OutcomeDiscarded {
				t.Fatalf("duplicate = %#v, %v", result, err)
			}
			if len(result.EmitIntents) != 0 || repo.snapshot.StateCarrier.Fields["marker"] != "first" || !reflect.DeepEqual(repo.snapshot.StateCarrier.StateBuckets, before) {
				t.Fatalf("refusal/duplicate changed state or effects: %#v", result)
			}
		})
	}
	acc, ok := loadAccumulator(repo.snapshot, first.Node, "item.received")
	if !ok || acc.Err() != nil || len(acc.Items) != 1 {
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
	for _, payload := range []string{`{"marker":"missing"}`, `{"id":null,"marker":"null"}`, `{"id":"","marker":"empty"}`, `{"id":1,"marker":"numeric"}`, `{"id":true,"marker":"boolean"}`, `{"id":{"value":"record"},"marker":"record"}`, `{"id":["list"],"marker":"list"}`} {
		t.Run(payload, func(t *testing.T) {
			executor, repo := a2AccumulatorExecutor(t)
			before := cloneStateBucketSet(repo.snapshot.StateCarrier.StateBuckets)
			result, err := executor.ExecuteSemanticFixture(context.Background(), a2AccumulatorRequest(t, "payload.id", payload, "not-a-business-key"))
			requireAccumulatorFailure(t, err, failures.ClassSchemaInvalid)
			if len(result.EmitIntents) != 0 || len(repo.snapshot.StateCarrier.Fields) != 0 || !reflect.DeepEqual(before, repo.snapshot.StateCarrier.StateBuckets) {
				t.Fatalf("invalid key changed state/effects: %#v %#v", repo.snapshot, result)
			}
		})
	}
}

func a2DeliveryContext(t *testing.T, deliveryID string) context.Context {
	t.Helper()
	claim, err := runtimedelivery.AdmitPersistedClaim(deliveryID, semanticExecutionFixtureRunID, "exact-route", "fence", 1, runtimedelivery.SubscriberNode, testRootExecutableNode(t, "collector").Key())
	if err != nil {
		t.Fatal(err)
	}
	return runtimedelivery.WithClaim(context.Background(), claim)
}

func TestA2M14UnkeyedAccumulatorUsesDurableDeliveryNotEventIdentity(t *testing.T) {
	executor, repo := a2AccumulatorExecutor(t)
	request := a2AccumulatorRequest(t, "", `{"marker":"same","id":"same"}`, "same-event-identity")
	for _, delivery := range []string{"delivery-2-committed-first", "delivery-1-committed-second"} {
		result, err := executor.ExecuteSemanticFixture(a2DeliveryContext(t, delivery), request)
		if err != nil || len(result.EmitIntents) != 1 {
			t.Fatalf("distinct delivery = %#v, %v", result, err)
		}
	}
	result, err := executor.ExecuteSemanticFixture(a2DeliveryContext(t, "delivery-2-committed-first"), request)
	if err != nil || result.Status != OutcomeDiscarded || len(result.EmitIntents) != 0 {
		t.Fatalf("delivery retry = %#v, %v", result, err)
	}
	acc, _ := loadAccumulator(repo.snapshot, request.Node, "item.received")
	if len(acc.Items) != 2 || len(acc.Received) != 0 || len(acc.Deliveries) != 2 {
		t.Fatalf("unkeyed multiplicity = %#v", acc)
	}
	result, err = executor.ExecuteSemanticFixture(context.Background(), request)
	requireAccumulatorFailure(t, err, failures.ClassLifecycleConflict)
	if len(result.EmitIntents) != 0 {
		t.Fatalf("unclaimed delivery emitted: %#v", result)
	}
}

func TestA2M15AccumulatorBucketIsolationAndCapturedGeneration(t *testing.T) {
	executor, _ := a2AccumulatorExecutor(t)
	firstNode, secondNode := testRootExecutableNode(t, "collector"), testRootExecutableNode(t, "other-collector")
	instanceA, instanceB := StateSnapshot{}, StateSnapshot{}
	activation := &loopruntime.Activation{LoopID: "loop", ActivationID: "activation", RevisionField: "revision", RevisionID: "revision-1", Attempt: 1}
	for _, tc := range []struct {
		state      *StateSnapshot
		node       identity.ExecutableNode
		event      string
		activation *loopruntime.Activation
	}{
		{&instanceA, firstNode, "item.received", nil},
		{&instanceA, secondNode, "item.received", nil},
		{&instanceA, firstNode, "other.handler", nil},
		{&instanceA, firstNode, "item.received", activation},
		{&instanceB, firstNode, "item.received", nil},
	} {
		frame := &executionFrame{req: ExecutionRequest{Node: tc.node, HandlerEventKey: tc.event}, loopActivation: tc.activation}
		bucket, ok, err := executor.resolveAccumulatorBucketRef(frame, nil)
		if err != nil || !ok || !bucket.Valid() {
			t.Fatalf("bucket = %#v, %v", bucket, err)
		}
		acc := &Accumulator{}
		if _, err := acc.Admit(&runtimecontracts.AccumulateSpec{Key: "payload.id"}, map[string]any{"id": "same-key"}, ""); err != nil {
			t.Fatal(err)
		}
		storeAccumulatorForBucket(tc.state, bucket, acc)
		loaded, ok := loadAccumulatorForBucket(*tc.state, bucket)
		if !ok || loaded.Err() != nil || len(loaded.Items) != 1 || accumulatorExpressionValue(loaded)["received_count"] != 1 {
			t.Fatalf("isolated accumulator = %#v", loaded)
		}
	}
	rootBucket := timeridentity.NewAccumulatorBucketRef(firstNode, "item.received")
	delete(instanceA.StateCarrier.StateBuckets[firstNode.Key()][handlerAccumulatorBucketKey].(map[string]any), rootBucket.Key())
	if _, ok := loadAccumulatorForBucket(instanceA, rootBucket); ok {
		t.Fatal("reset retained its selected accumulator")
	}
	generationBucket := timeridentity.NewAccumulatorBucketRefForGeneration(firstNode, "item.received", activation.Generation())
	if _, ok := loadAccumulatorForBucket(instanceA, generationBucket); !ok {
		t.Fatal("reset crossed the captured-generation boundary")
	}
	if _, ok := loadAccumulatorForBucket(instanceB, rootBucket); !ok {
		t.Fatal("reset crossed the instance boundary")
	}
	frame := &executionFrame{req: ExecutionRequest{Node: firstNode, HandlerEventKey: "item.received"}, loopActivation: activation}
	captured, _, err := executor.resolveAccumulatorBucketRef(frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	activation.Attempt++
	cached, _, err := executor.resolveAccumulatorBucketRef(frame, nil)
	if err != nil || cached.Key() != captured.Key() {
		t.Fatalf("consumer retargeted captured generation: %#v, %v", cached, err)
	}
	frame = &executionFrame{req: ExecutionRequest{Node: firstNode, HandlerEventKey: "item.received"}, loopActivation: &loopruntime.Activation{}}
	_, _, err = executor.resolveAccumulatorBucketRef(frame, nil)
	requireAccumulatorFailure(t, err, failures.ClassLifecycleConflict)
}

func TestA2M15AccumulatorProjectionPreservesMetadataNamesAndNestedFields(t *testing.T) {
	executor := &Executor{}
	text := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}
	record := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Fields: []runtimecontracts.ResolvedCatalogField{{Name: "id", Type: text}}}
	binding := accprojection.Binding{SourceItemType: "Business", TargetItemType: "Projected", Project: map[string]any{}}
	item := map[string]any{"event_id": "authored-id", "event_type": "authored-type", "source": "authored-source", "received_at": "authored-time", "payload": map[string]any{"id": "nested"}}
	for _, name := range []string{"event_id", "event_type", "source", "received_at", "payload"} {
		fieldType := text
		if name == "payload" {
			fieldType = record
		}
		field := runtimecontracts.ResolvedCatalogField{Name: name, Type: fieldType}
		binding.SourceType.Fields = append(binding.SourceType.Fields, field)
		binding.TargetType.Fields = append(binding.TargetType.Fields, field)
		binding.Project[name] = "source." + name
	}
	projected, err := executor.projectAccumulatorItems(&executionFrame{}, binding, []map[string]any{item})
	if err != nil || len(projected) != 1 || !reflect.DeepEqual(projected[0], item) {
		t.Fatalf("business projection = %#v, %v", projected, err)
	}
	projected[0].(map[string]any)["payload"].(map[string]any)["id"] = "changed"
	if item["payload"].(map[string]any)["id"] != "nested" {
		t.Fatal("projection aliases nested business fields")
	}
}

func TestA2M15AccumulatorFilterReduceCountAndComputeUseBusinessItems(t *testing.T) {
	acc := &Accumulator{}
	spec := &runtimecontracts.AccumulateSpec{Key: "payload.id"}
	for _, item := range []map[string]any{
		{"id": "a", "score": int64(8), "payload": map[string]any{"score": int64(0)}},
		{"id": "b", "score": int64(2), "payload": map[string]any{"score": int64(100)}},
	} {
		if _, err := acc.Admit(spec, item, ""); err != nil {
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
