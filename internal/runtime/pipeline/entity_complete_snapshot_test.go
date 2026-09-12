package pipeline

import (
	"context"
	"reflect"
	"strings"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

func TestEntityLastFieldClearAndColdReloadBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, engine := range []string{"bridge", "declarative"} {
			t.Run(backend+"/"+engine, func(t *testing.T) {
				db, store := openHandlerEntityRequirementStore(t, backend)
				source := handlerEntityRequirementExecutionSource()
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
				instance, _ := executeExistingOwnerBehavior(t, ctx, pc, engine, "last-field-clear", rc.SystemNodeEventHandler{
					DataAccumulation: rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{{Operation: "clear", TargetRef: "entity.revision_count"}}},
					Emit:             rc.EmitSpec{Event: "work.emitted"},
				}, nil, map[string]any{"revision_count": 0}, nil)
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
}
