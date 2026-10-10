package pipeline

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func VerifyEntityLastFieldClearAndColdReloadBothStoresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":   "stages:\n  active: {}\n",
		"entities.yaml": "test_entity:\n  revision_count: integer?\n",
		"events.yaml":   "work.clear:\n",
		"nodes.yaml": `node-a:
  execution_type: system_node
  subscribes_to: [work.clear]
  event_handlers:
    work.clear:
      data_accumulation:
        writes:
          - op: clear
            target: entity.revision_count
`,
	})
	fixture := open(t, source)
	newCoordinator := func() *PipelineCoordinator {
		pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
			Module: staticSemanticWorkflowModule{source: source},
		})
		return pc
	}
	ctx := nativeWorkflowHandlerRunContextForTest(t, fixture)
	pc := newCoordinator()
	instance, result := executeExistingOwnerBehavior(t, fixture, ctx, pc, "last-field-clear", "work.clear", json.RawMessage(`{}`), map[string]any{"revision_count": 0}, nil)
	if !result.handled {
		t.Fatal("selected handler did not clear the entity")
	}
	if len(instance.Fields) != 0 {
		t.Fatalf("last field clear retained fields: %#v", instance.Fields)
	}
	if _, found := instance.StateBuckets["entity_projection"]; found {
		t.Fatal("canonical empty entity acquired a shadow field copy")
	}
	// A new coordinator has no prior handler draft. The complete stored
	// empty map must still load as found, not as missing owner/state.
	pc = newCoordinator()
	repo := pipelineEngineStateRepo{coordinator: pc}
	address := testEngineStateAddress(".", instance.StorageRef, instance.EntityID)
	address.FlowInstance = testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef)
	for attempt := 0; attempt < 2; attempt++ {
		loaded, found, err := repo.LoadState(ctx, address)
		if err != nil || !found || len(loaded.Fields) != 0 {
			t.Fatalf("cold complete-empty load: found=%v fields=%#v err=%v", found, loaded.Fields, err)
		}
	}
	wrongOwner := address
	wrongOwner.EntityID = identity.NormalizeEntityID(uuid.NewString())
	if _, found, err := repo.LoadState(ctx, wrongOwner); err == nil || found {
		t.Fatalf("complete-empty state bypassed exact owner admission: found=%v err=%v", found, err)
	}
	before, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, address.FlowInstance)
	if err != nil || !found {
		t.Fatal("missing cleared entity before hostile candidate")
	}
	owner := pipelineEngineMutationOwner{store: fixture.Persistence.store, state: repo}
	evaluated := loadEngineEvaluationForTest(t, ctx, repo, address)
	_, err = owner.CommitEngineMutation(ctx, runtimeengine.EngineMutation{
		Address: address, EvaluatedState: evaluated,
		State: testEngineStateMutation(map[string]any{"undeclared": "poison"}, nil, nil),
	})
	if err == nil || !strings.Contains(err.Error(), "undeclared") {
		t.Fatalf("unvalidated commit candidate accepted: %v", err)
	}
	after, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, address.FlowInstance)
	if err != nil || !found || after.Revision != before.Revision || !reflect.DeepEqual(after.Fields, before.Fields) {
		t.Fatalf("invalid candidate changed durable state: before=%#v after=%#v err=%v", before, after, err)
	}
}

func VerifySelectedHandlerSparsePresenceWriteAndEmitBothStoresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":   "stages:\n  active: {}\n",
		"entities.yaml": "test_entity:\n  kill_reason: text?\n  observed_absence: boolean?\n",
		"events.yaml":   "work.ready:\nwork.emitted:\n  observed_absence: boolean\n",
		"nodes.yaml": `node-a:
  execution_type: system_node
  subscribes_to: [work.ready]
  event_handlers:
    work.ready:
      data_accumulation:
        writes:
          - target_field: observed_absence
            value: |-
                     !has(entity.kill_reason)
      emit:
        event: work.emitted
        fields:
          observed_absence: entity.observed_absence
`,
	})
	fixture := open(t, source)
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	ctx := nativeWorkflowHandlerRunContextForTest(t, fixture)
	instance, result := executeExistingOwnerBehavior(t, fixture, ctx, pc, "sparse-presence-emit", "work.ready", json.RawMessage(`{}`), nil, nil)
	if !result.handled || instance.Fields["observed_absence"] != true {
		t.Fatalf("selected handler did not persist absence decision: handled=%v fields=%#v", result.handled, instance.Fields)
	}
	if _, present := instance.Fields["kill_reason"]; present {
		t.Fatalf("unassigned field materialized: %#v", instance.Fields)
	}
	if len(result.emissions) != 1 {
		t.Fatalf("selected handler emissions = %d, want one", len(result.emissions))
	}
	var payload map[string]any
	if err := json.Unmarshal(result.emissions[0].Payload(), &payload); err != nil || payload["observed_absence"] != true {
		t.Fatalf("sparse presence payload = %#v, err=%v", payload, err)
	}
}

func VerifySelectedHandlerSparseEqualityMutationsBothStoresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	for _, tc := range []struct {
		name       string
		event      string
		wantFields map[string]any
	}{
		{"ordered-pair-write", "work.set", map[string]any{"left": "new", "right": "new"}},
		{"paired-clear", "work.clear", map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadWorkflowTempSource(t, map[string]string{
				"schema.yaml": "stages:\n  active: {}\n",
				"entities.yaml": `test_entity:
  left: text?
  right:
    type: text?
    equal_to: left
`,
				"events.yaml": "work.set:\nwork.clear:\n",
				"nodes.yaml": `node-a:
  execution_type: system_node
  subscribes_to: [work.set, work.clear]
  event_handlers:
    work.set:
      data_accumulation:
        writes:
          - target_field: left
            value: |-
                     'new'
          - target_field: right
            value: entity.left
    work.clear:
      data_accumulation:
        writes:
          - op: clear
            target: entity.left
          - op: clear
            target: entity.right
`,
			})
			fixture := open(t, source)
			pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
				Module: staticSemanticWorkflowModule{source: source},
			})
			ctx := nativeWorkflowHandlerRunContextForTest(t, fixture)
			instance, result := executeExistingOwnerBehavior(t, fixture, ctx, pc, tc.name, tc.event, json.RawMessage(`{}`), map[string]any{"left": "old", "right": "old"}, nil)
			if !result.handled || !reflect.DeepEqual(instance.Fields, tc.wantFields) {
				t.Fatalf("selected handler result: handled=%t fields=%#v, want %#v", result.handled, instance.Fields, tc.wantFields)
			}
		})
	}
}
