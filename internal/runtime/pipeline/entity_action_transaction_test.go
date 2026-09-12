package pipeline

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type presenceActionResult func(engine.ExecutionContext) engine.ActionExecution

func (f presenceActionResult) ExecuteAction(_ context.Context, _ rc.ActionSpec, _ registry.ActionInstruction, current engine.ExecutionContext) (engine.ActionExecution, error) {
	return f(current), nil
}

// Only the inline action result is supplied by the harness. Loading, ordered
// application, commit, rollback and outbox settlement use production owners.
func TestEntityActionMutationPresenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			base := handlerEntityRequirementExecutionSource().(handlerEntityRequirementSemanticSource)
			bundle, _ := semanticview.Bundle(base.Source)
			bundle.RootEntities = rc.EntityContractsDocument{"test_entity": {Fields: map[string]rc.EntityFieldDecl{
				"note": {Type: "text", IsOptional: true}, "keep": {Type: "text"}, "visits": {Type: "list<text>"},
			}}}
			source := handlerEntityRequirementSemanticSource{Source: semanticview.Wrap(bundle)}
			bus := &recordingPipelineBus{}
			pc := newDurablePipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{
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
			instance, _ := executeExistingOwnerBehavior(t, ctx, pc, "declarative", "action-presence", rc.SystemNodeEventHandler{Guard: &rc.GuardSpec{Check: "true"}}, nil, map[string]any{"note": "old", "keep": "stored"}, nil)
			owner := testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef)
			node := pipelineNode(t, ".", "node-a")
			target := events.RouteIdentity{FlowID: ".", FlowInstance: instance.StorageRef, EntityID: instance.EntityID}
			for _, step := range []struct {
				name       string
				operations []entityruntime.Mutation
				snapshot   bool
				reject     string
			}{
				{name: "clear_and_append_once", operations: []entityruntime.Mutation{{Operation: entityruntime.MutationClear, Target: "note"}, {Operation: "append", Target: "entity.visits", Value: "once"}}},
				{name: "omitted_effect_is_unchanged"},
				{name: "invalid_effect_rolls_back_prior_append", operations: []entityruntime.Mutation{{Operation: "append", Target: "entity.visits", Value: "must-rollback"}, {Operation: entityruntime.MutationClear, Target: "keep"}}, reject: "cannot clear bare entity field keep"},
				{name: "stale_snapshot_is_not_an_effect", snapshot: true, reject: "snapshot"},
			} {
				t.Run(step.name, func(t *testing.T) {
					before, found, err := store.Load(ctx, owner)
					if err != nil || !found {
						t.Fatalf("load before: %v %v", found, err)
					}
					publications, calls := bus.outboxCount(), 0
					deps := coordinatorEngineDependencies(pc)
					deps.ActionRunner = presenceActionResult(func(current engine.ExecutionContext) engine.ActionExecution {
						calls++
						if current.Request.State.Fields["keep"] != "current" {
							t.Fatalf("action saw stale draft: %#v", current.Request.State.Fields)
						}
						result := engine.ActionExecution{Handled: true, EntityMutations: step.operations}
						if step.snapshot {
							state := current.Request.State.StateCarrier
							state.Fields = map[string]any{"note": "resurrected", "keep": "stale"}
							result.State = &engine.StateMutation{StateCarrier: state}
						}
						return result
					})
					executor, err := engine.NewExecutor(deps, newCoordinatorEngineEvaluator(pc))
					if err != nil {
						t.Fatal(err)
					}
					runner := &coordinatorHandlerExecutionEngine{coordinator: pc, nodeRef: node, executor: executor, node: engine.NewDeclarativeNode(node, executor)}
					evt := handlerTestRootIngress(uuid.NewString(), "work.ready", "", "", []byte(`{}`), 0, runtimeRunID(ctx), "", handlerTestWorkflowEnvelope(".", instance.StorageRef, instance.EntityID), time.Now().UTC())
					seedExactOnceEvent(t, store, ctx, evt)
					evt = eventtest.TargetRouted(evt, target)
					deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)})
					handler := rc.SystemNodeEventHandler{Action: rc.ActionSpec{ID: "record_evidence"}, DataAccumulation: rc.WorkflowDataAccumulation{Writes: []rc.WorkflowDataWrite{{TargetField: "keep", Value: rc.LiteralExpression("current")}}}, Emit: rc.EmitSpec{Event: "work.emitted"}}
					_, err = runner.ExecuteHandlerSteps(deliveryCtx, handler, evt, "work.ready")
					if calls != 1 || (err != nil) != (step.reject != "") || err != nil && !strings.Contains(err.Error(), step.reject) {
						t.Fatalf("calls=%d error=%v; want %q", calls, err, step.reject)
					}
					after, found, err := store.Load(ctx, owner)
					if err != nil || !found {
						t.Fatalf("load after: %v %v", found, err)
					}
					if step.reject != "" {
						if !reflect.DeepEqual(before.Fields, after.Fields) || before.Revision != after.Revision || bus.outboxCount() != publications {
							t.Fatalf("rejection escaped: %#v -> %#v", before, after)
						}
					} else {
						want := map[string]any{"keep": "current", "visits": []any{"once"}}
						if !reflect.DeepEqual(after.Fields, want) || bus.outboxCount() != publications+1 {
							t.Fatalf("fields=%#v want=%#v outbox=%d->%d", after.Fields, want, publications, bus.outboxCount())
						}
					}
				})
			}
		})
	}
}
