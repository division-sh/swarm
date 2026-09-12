package pipeline

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestEntitySparseEqualityAtomicMutationsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, engine := range []string{"bridge", "declarative"} {
			t.Run(backend+"/"+engine, func(t *testing.T) {
				db, store := openHandlerEntityRequirementStore(t, backend)
				base := handlerEntityRequirementExecutionSource().(handlerEntityRequirementSemanticSource)
				bundle, _ := semanticview.Bundle(base.Source)
				bundle.RootEntities = rc.EntityContractsDocument{"test_entity": {Fields: map[string]rc.EntityFieldDecl{
					"left":     {Type: "integer", IsOptional: true, Refinements: rc.SchemaRefinements{EqualTo: "right"}},
					"right":    {Type: "integer", IsOptional: true},
					"bare":     {Type: "integer", Refinements: rc.SchemaRefinements{EqualTo: "optional"}},
					"optional": {Type: "integer", IsOptional: true},
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
				// Seed an actually valid, entirely unassigned root. Every later
				// operation goes through the existing delivery/engine transaction.
				instance, _ := executeExistingOwnerBehavior(t, ctx, pc, engine, "empty-equality", rc.SystemNodeEventHandler{Guard: &rc.GuardSpec{Check: "true"}}, nil, nil, nil)
				owner := testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef)
				node := pipelineNode(t, ".", "node-a")
				target := events.RouteIdentity{FlowID: ".", FlowInstance: instance.StorageRef, EntityID: instance.EntityID}
				set := func(field, expression string) rc.WorkflowDataWrite {
					return rc.WorkflowDataWrite{TargetField: field, Value: rc.CELExpression(expression)}
				}
				clear := func(field string) rc.WorkflowDataWrite {
					return rc.WorkflowDataWrite{Operation: rc.WorkflowDataOperationClear, TargetRef: "entity." + field}
				}
				for _, step := range []struct {
					name   string
					writes []rc.WorkflowDataWrite
					want   map[string]any
					reject string
					retry  bool
				}{
					{name: "ordered_pair_captured_payload_retry", writes: []rc.WorkflowDataWrite{{SourceField: "score", TargetField: "left"}, set("right", "entity.left")}, want: map[string]any{"left": float64(5), "right": float64(5)}, retry: true},
					{name: "ordered_increment", writes: []rc.WorkflowDataWrite{set("left", "entity.left + 1"), set("right", "entity.left")}, want: map[string]any{"left": float64(6), "right": float64(6)}},
					{name: "clear_one_rejected", writes: []rc.WorkflowDataWrite{clear("left")}, reject: "must equal entity.right but source is missing"},
					{name: "unequal_pair_rejected", writes: []rc.WorkflowDataWrite{set("left", "7"), set("right", "8")}, reject: "must equal entity.right"},
					{name: "missing_mapped_source_rejected", writes: []rc.WorkflowDataWrite{{SourceField: "missing", TargetField: "left"}, set("right", "9")}, reject: "source payload.missing is absent"},
					{name: "clear_both", writes: []rc.WorkflowDataWrite{clear("left"), clear("right")}, want: map[string]any{}},
					{name: "repeat_clear_both", writes: []rc.WorkflowDataWrite{clear("left"), clear("right")}, want: map[string]any{}},
					{name: "half_pair_rejected", writes: []rc.WorkflowDataWrite{set("right", "1")}, reject: "must equal entity.right but source is missing"},
					{name: "explicit_zero_pair", writes: []rc.WorkflowDataWrite{set("left", "0"), set("right", "entity.left")}, want: map[string]any{"left": float64(0), "right": float64(0)}},
					{name: "mixed_pair_assignment", writes: []rc.WorkflowDataWrite{set("bare", "0"), set("optional", "entity.bare")}, want: map[string]any{"left": float64(0), "right": float64(0), "bare": float64(0), "optional": float64(0)}},
					{name: "mixed_pair_same_value", writes: []rc.WorkflowDataWrite{set("bare", "0")}, want: map[string]any{"left": float64(0), "right": float64(0), "bare": float64(0), "optional": float64(0)}},
					{name: "mixed_pair_half_clear_rejected", writes: []rc.WorkflowDataWrite{clear("optional")}, reject: "must equal entity.optional"},
					{name: "mixed_pair_both_clear_rejected", writes: []rc.WorkflowDataWrite{clear("optional"), clear("bare")}, reject: "cannot clear bare entity field bare"},
				} {
					t.Run(step.name, func(t *testing.T) {
						before, found, err := store.Load(ctx, owner)
						if err != nil || !found {
							t.Fatalf("load before: found=%v err=%v", found, err)
						}
						publications := bus.outboxCount()
						evt := handlerTestRootIngress(uuid.NewString(), "work.ready", "", "", []byte(`{"score":5}`), 0, runtimeRunID(ctx), "", handlerTestWorkflowEnvelope(".", instance.StorageRef, instance.EntityID), time.Now().UTC())
						seedExactOnceEvent(t, store, ctx, evt)
						evt = eventtest.TargetRouted(evt, target)
						deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)})
						handler := rc.SystemNodeEventHandler{DataAccumulation: rc.WorkflowDataAccumulation{SourceEvent: "work.ready", Writes: step.writes}, Emit: rc.EmitSpec{Event: "work.emitted"}}
						execute := func() error {
							if engine == "bridge" {
								_, err := pc.executeNodeContractHandler(deliveryCtx, node, handler, workflowTriggerContext{Event: evt, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(instance.StorageRef), instance.EntityID)}, false)
								return err
							}
							_, err := newCoordinatorHandlerExecutionEngine(pc, node).ExecuteHandlerSteps(deliveryCtx, handler, evt, "work.ready")
							return err
						}
						if step.retry {
							bus.outboxErr = errors.New("presence retry commit failure")
							if err := execute(); err == nil || !strings.Contains(err.Error(), "presence retry commit failure") {
								t.Fatalf("missing injected commit failure: %v", err)
							}
							rolledBack, found, err := store.Load(ctx, owner)
							if err != nil || !found || !reflect.DeepEqual(rolledBack.Fields, before.Fields) || rolledBack.Revision != before.Revision || bus.outboxCount() != publications {
								t.Fatalf("failed first attempt escaped: state=%#v err=%v", rolledBack, err)
							}
							bus.outboxErr = nil
						}
						err = execute()
						if string(evt.Payload()) != `{"score":5}` {
							t.Fatal("entity mutation changed captured event payload")
						}
						if (err != nil) != (step.reject != "") || err != nil && !strings.Contains(err.Error(), step.reject) {
							t.Fatalf("want error containing %q, got %v", step.reject, err)
						}
						after, found, loadErr := store.Load(ctx, owner)
						if loadErr != nil || !found {
							t.Fatalf("load after: found=%v err=%v", found, loadErr)
						}
						if step.reject != "" {
							if !reflect.DeepEqual(after.Fields, before.Fields) || after.Revision != before.Revision || bus.outboxCount() != publications {
								t.Fatalf("rejected operation escaped: before=%#v after=%#v publications=%d->%d", before, after, publications, bus.outboxCount())
							}
						} else if !reflect.DeepEqual(after.Fields, step.want) || bus.outboxCount() != publications+1 {
							t.Fatalf("commit fields=%#v want=%#v publications=%d->%d", after.Fields, step.want, publications, bus.outboxCount())
						}
					})
				}
			})
		}
	}
}
