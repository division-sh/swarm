package pipeline

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

// The generation changes after the authored node owns its input, rather than
// before a lower-level store callback is entered.
func VerifySystemNodePredecessorCannotCommitAfterGenerationAdvanceForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest, advance func(*testing.T, context.Context, deliverylifecycle.Store) func()) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			source := loadWorkflowTempSource(t, map[string]string{
				"manifest.yaml": "version: 1.0.0\nplatform_version: \">=0.7.0 <0.8.0\"\n",
				"schema.yaml":   "stages:\n  queued: {}\n  done: {final: true}\n",
				"entities.yaml": "work:\n  status: {type: text, initial: pending}\n",
				"events.yaml":   "source.evt:\n  status: text\nnode.completed:\n",
				"nodes.yaml":    "writer:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      data_accumulation:\n        writes: [status]\n      advances_to: done\n      emit: node.completed\n",
			})
			bundle, _ := semanticview.Bundle(source)
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			run := correlation.RunIDFromContext(ctx)
			at := time.Now().UTC()
			if err := fixture.Construct(ctx, WorkflowInstance{
				InstanceID: run, StorageRef: run, EntityID: run, EntityType: "work",
				WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), Mode: "static",
				CurrentState: "queued", StageDefined: true, Status: "active",
				Fields: map[string]any{"status": "pending"}, CreatedAt: at, EnteredStageAt: at, UpdatedAt: at,
			}); err != nil {
				t.Fatal(err)
			}
			target := events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: run}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineSourceNode(t, source, ".", "writer")), Target: events.MustExistingEntityTarget(target)}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`{"status":"complete"}`), 0, run, events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), at)
			if err := fixture.PublishNode(ctx, event, route); err != nil {
				t.Fatal(err)
			}
			id, err := deliverylifecycle.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			var claimed deliverylifecycle.Snapshot
			var release func()
			pc.SetTestWorkflowNodeHandlerStartHook(func(claimCtx context.Context, _ string, _ events.Event) error {
				claimed, err = fixture.Store.Snapshot(claimCtx, id)
				if err != nil || claimed.Status != deliverylifecycle.StatusInProgress || claimed.ClaimVersion != 1 {
					t.Fatalf("authored handler did not own its exact input: %+v/%v", claimed, err)
				}
				release = advance(t, claimCtx, fixture.Store)
				return nil
			})
			if _, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), event); err == nil || (!strings.Contains(err.Error(), "mutation original process generation is no longer current") && !strings.Contains(err.Error(), "process startup/topology capability is terminal")) {
				t.Fatalf("predecessor authored node did not reach native generation refusal: %v", err)
			}
			if release == nil {
				t.Fatal("generation comparison did not reach the claimed authored handler")
			}
			after, err := fixture.Store.Snapshot(ctx, id)
			outcomes, outcomeErr := fixture.Store.Outcomes(ctx, id)
			instance, found, readErr := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceForRun(run, run))
			count, countErr := fixture.EventCount(ctx, run, "node.completed")
			if err != nil || outcomeErr != nil || readErr != nil || countErr != nil || !found || !reflect.DeepEqual(claimed, after) || len(outcomes) != 0 || instance.CurrentState != "queued" || instance.Fields["status"] != "pending" || count != 0 {
				t.Fatalf("predecessor changed authored state/output/settlement: delivery=%+v outcomes=%+v state=%+v output=%d errors=%v/%v/%v/%v", after, outcomes, instance, count, err, outcomeErr, readErr, countErr)
			}
			release()
			next := fixture.ReopenExecution()
			nextPC, nextCtx := nativePipelineDeliveryCoordinatorForTest(t, next, pc.module)
			nextCtx = correlation.WithRunID(nextCtx, run)
			fresh := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`{"status":"complete"}`), 0, run, events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC())
			if err := next.PublishNode(nextCtx, fresh, route); err != nil {
				t.Fatal(err)
			}
			if handled, err := nextPC.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(nextCtx, route), fresh); err != nil || !handled {
				t.Fatalf("fresh authored handler: handled=%t err=%v", handled, err)
			}
			freshID, err := deliverylifecycle.DeliveryID(fresh.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			settled, err := next.Store.Snapshot(nextCtx, freshID)
			instance, found, readErr = next.Persistence.LoadWorkflowInstance(nextCtx, testRunScopedWorkflowInstanceForRun(run, run))
			count, countErr = next.EventCount(nextCtx, run, "node.completed")
			if err != nil || readErr != nil || countErr != nil || !found || settled.Status != deliverylifecycle.StatusDelivered || instance.CurrentState != "done" || instance.Fields["status"] != "complete" || count != 1 {
				t.Fatalf("fresh generation did not atomically write/emit/settle: delivery=%+v state=%+v output=%d errors=%v/%v/%v", settled, instance, count, err, readErr, countErr)
			}
		})
	}
}
