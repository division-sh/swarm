package pipeline

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

func TestEntityLastFieldClearAndColdReloadBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			source := loadWorkflowTempSource(t, map[string]string{
				"schema.yaml":   "initial_state: active\nstates: [active]\n",
				"entities.yaml": "test_entity:\n  revision_count: integer?\n",
				"events.yaml":   "work.clear: {}\n",
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
			newCoordinator := func() *PipelineCoordinator {
				pc := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
					Module: staticSemanticWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store),
					PipelineObligations: unavailablePipelineTestObligationOwner{},
				})
				configureWorkflowLifecycleForTest(t, pc)
				configurePipelineTestDeliveryOwner(t, pc)
				return pc
			}
			var ctx context.Context
			if backend == "sqlite" {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}
			pc := newCoordinator()
			instance, result := executeExistingOwnerBehavior(t, ctx, pc, "last-field-clear", "work.clear", json.RawMessage(`{}`), map[string]any{"revision_count": 0}, nil)
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
			before, found, err := store.Load(ctx, address.FlowInstance)
			if err != nil || !found {
				t.Fatal("missing cleared entity before hostile candidate")
			}
			owner := pipelineEngineMutationOwner{store: store, state: repo}
			_, err = owner.CommitEngineMutation(ctx, runtimeengine.EngineMutation{
				Address: address,
				State:   testEngineStateMutation(map[string]any{"undeclared": "poison"}, nil, nil),
			})
			if err == nil || !strings.Contains(err.Error(), "undeclared") {
				t.Fatalf("unvalidated commit candidate accepted: %v", err)
			}
			after, found, err := store.Load(ctx, address.FlowInstance)
			if err != nil || !found || after.Revision != before.Revision || !reflect.DeepEqual(after.Fields, before.Fields) {
				t.Fatalf("invalid candidate changed durable state: before=%#v after=%#v err=%v", before, after, err)
			}
		})
	}
}

func TestSelectedHandlerSparsePresenceWriteAndEmitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			source := loadWorkflowTempSource(t, map[string]string{
				"schema.yaml":   "initial_state: active\nstates: [active]\n",
				"entities.yaml": "test_entity:\n  kill_reason: text?\n  observed_absence: boolean?\n",
				"events.yaml":   "work.ready: {}\nwork.emitted:\n  observed_absence: boolean\n",
				"nodes.yaml": `node-a:
  execution_type: system_node
  subscribes_to: [work.ready]
  event_handlers:
    work.ready:
      data_accumulation:
        writes:
          - target_field: observed_absence
            expression: "!has(entity.kill_reason)"
      emit:
        event: work.emitted
        fields:
          observed_absence: {ref: entity.observed_absence}
`,
			})
			pc := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
				Module: staticSemanticWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store),
				PipelineObligations: unavailablePipelineTestObligationOwner{},
			})
			configureWorkflowLifecycleForTest(t, pc)
			configurePipelineTestDeliveryOwner(t, pc)
			var ctx context.Context
			if backend == "sqlite" {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}
			instance, result := executeExistingOwnerBehavior(t, ctx, pc, "sparse-presence-emit", "work.ready", json.RawMessage(`{}`), nil, nil)
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
		})
	}
}
