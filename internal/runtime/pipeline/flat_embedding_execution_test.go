package pipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func VerifyFlatEmbeddingExecutionBothStoresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := loadWorkflowTempSource(t, map[string]string{
		"schema.yaml": "name: embedding\npins:\n  inputs: [work.ready]\n  outputs: [work.done]\nstages:\n  active: {}\n",
		"types.yaml":  "types:\n  Marker:\n    marker: {type: text, pattern: '^[a-z]+$'}\n",
		"state.yaml":  "test_entity:\n  _embed: [Marker]\n  sentinel: {type: integer, initial: 7, immutable: true, indexed: true}\n",
		"events.yaml": "work.ready:\n  _key: marker\n  _embed: [Marker]\nwork.done:\n  _embed: [Marker]\n",
		"nodes.yaml":  "node-a:\n  execution_type: system_node\n  subscribes_to: [work.ready]\n  event_handlers:\n    work.ready:\n      data_accumulation:\n        writes:\n          - target_field: marker\n            value: payload.marker\n      emit:\n        event: work.done\n        fields:\n          marker: entity.marker\n",
	})
	fixture := open(t, source)
	ctx := nativeWorkflowHandlerRunContextForTest(t, fixture)
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: source}})
	constructor, err := CompileFlowConstructor(source, ".", "")
	if err != nil {
		t.Fatal(err)
	}
	initial, err := constructor.InitialFields(nil, nil)
	if err != nil {
		t.Fatalf("construct embedded fields: %v", err)
	}
	instance, result := executeExistingOwnerBehavior(t, fixture, ctx, pc, "embedded-field", "work.ready", json.RawMessage(`{"marker":"after"}`), initial, nil)
	if !result.handled || instance.Fields["marker"] != "after" || instance.Fields["sentinel"] != int64(7) || len(result.emissions) != 1 {
		t.Fatalf("embedded write/emit: fields=%+v result=%+v", instance.Fields, result)
	}
	persisted, err := fixture.PublishedEvent(ctx, result.emissions[0].ID())
	if err != nil || !bytes.Equal(persisted.Payload(), json.RawMessage(`{"marker":"after"}`)) {
		t.Fatalf("embedded publication missing: %+v %v", persisted, err)
	}
	// A fresh persistence projection must resolve the same captured state shape.
	loaded, found, err := fixture.FreshProjection().LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef))
	if err != nil || !found || !reflect.DeepEqual(loaded.Fields, instance.Fields) {
		t.Fatalf("embedded cold readback differs: %+v %v", loaded, err)
	}
	before, err := fixture.ApplicationStorage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := constructor.InitialFields(nil, map[string]any{"marker": "before"}); err == nil || !strings.Contains(err.Error(), "no-argument constructor") {
		t.Fatalf("embedding changed the existing keyless constructor boundary: %v", err)
	}
	repo := pipelineEngineStateRepo{coordinator: pc}
	address := testEngineStateAddress(".", instance.StorageRef, instance.EntityID)
	address.FlowInstance = testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef)
	owner := pipelineEngineMutationOwner{store: fixture.Persistence.store, state: repo}
	_, err = owner.CommitEngineMutation(ctx, runtimeengine.EngineMutation{
		Address: address, EvaluatedState: loadEngineEvaluationForTest(t, ctx, repo, address),
		State: testEngineStateMutation(map[string]any{"marker": "123"}, nil, nil),
	})
	if err == nil || !strings.Contains(err.Error(), "pattern") {
		t.Fatalf("invalid embedded mutation accepted or refused elsewhere: %v", err)
	}
	contract, found := entityruntime.ResolveForFlow(source, ".")
	if !found {
		t.Fatal("missing admitted entity operation contract")
	}
	mutation, err := entityruntime.NewMutationPlan(contract, loaded.Fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := mutation.Append(entityruntime.Mutation{Target: "sentinel", Value: 8}); !errors.Is(err, entityruntime.ErrImmutableMutation) {
		t.Fatalf("own-field operation lost immutability: %v", err)
	}
	after, err := fixture.ApplicationStorage(ctx)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("invalid embedded candidate changed durable storage: %v\nbefore=%s\nafter=%s", err, before, after)
	}
}
