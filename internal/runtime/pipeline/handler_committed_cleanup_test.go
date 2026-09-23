package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestHandlerCommittedCleanupErrorRetainsExactOutcomeBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			store, ctx := backend.open(t)
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":   "name: committed-cleanup\ninitial_state: queued\nstates: [queued, done]\nterminal_states: [done]\n",
				"entities.yaml": "test_entity: {}\n",
				"events.yaml":   "source.evt: {}\nsource.done: {}\n",
				"nodes.yaml":    "node-a:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      advances_to: done\n      emit: {event: source.done}\n",
			})
			module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a").(*previewWorkflowModule)
			module.workflowNodes = []WorkflowNode{{
				Node: pipelineNode(t, ".", "node-a"), Subscriptions: []events.EventType{"source.evt"},
				Policies: map[string]WorkflowEventPolicy{"source.evt": {Consume: true}},
			}}
			bus := &recordingPipelineBus{}
			pc := newPostgresPipelineCoordinatorForTest(bus, store.testDB(), PipelineCoordinatorOptions{
				Module: module, DeliveryStore: newPipelineTestDeliveryOwnerForDB(t, store.testDB()),
			})
			pc.workflowStore = store
			owner := configurePipelineTestDeliveryOwner(t, pc)
			runID := correlation.RunIDFromContext(ctx)
			entityID := uuid.NewString()
			evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`{}`), 0, runID, "", handlerTestWorkflowEnvelope(".", runID, entityID), time.Now().UTC())
			dialect := authoractivityfixture.DialectPostgres
			if store.isSQLite() {
				dialect = authoractivityfixture.DialectSQLite
			}
			seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, evt)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: "v-test",
				CurrentState: "queued", EntityType: "test_entity", Fields: map[string]any{},
			})); err != nil {
				t.Fatal(err)
			}
			node := pipelineNode(t, ".", "node-a")
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID})}
			if err := owner.commitInitial(ctx, evt, route); err != nil {
				t.Fatal(err)
			}
			deliveryID, err := deliverylifecycle.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected postcommit publication cleanup")
			bus.finalizeErr = injected
			attemptCtx := withWorkflowNodeDeliveryRoute(ctx, route)
			handled, err := pc.dispatchWorkflowNodeEventResult(attemptCtx, evt)
			if !handled || !errors.Is(err, injected) {
				t.Fatalf("handled=%t error=%v, want committed cleanup diagnostic", handled, err)
			}
			assertCommittedHandlerCleanupRows(t, ctx, store, owner, bus, runID, entityID, deliveryID)
			bus.finalizeErr = nil
			if _, err := pc.dispatchWorkflowNodeEventResult(attemptCtx, evt); err != nil {
				t.Fatalf("duplicate delivery after acknowledged cleanup failure: %v", err)
			}
			assertCommittedHandlerCleanupRows(t, ctx, store, owner, bus, runID, entityID, deliveryID)
		})
	}
}

func assertCommittedHandlerCleanupRows(t *testing.T, ctx context.Context, store *workflowInstanceStore, owner *pipelineTestDeliveryOwner, bus *recordingPipelineBus, runID, entityID, deliveryID string) {
	t.Helper()
	snapshot, err := owner.Snapshot(ctx, deliveryID)
	if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered {
		t.Fatalf("delivery snapshot=%+v error=%v", snapshot, err)
	}
	outcomes, err := owner.Outcomes(ctx, deliveryID)
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("delivery outcomes=%+v error=%v, want one", outcomes, err)
	}
	stateQuery, eventQuery := "SELECT current_state FROM entity_state WHERE run_id=? AND entity_id=?", "SELECT COUNT(*) FROM events WHERE run_id=? AND event_name='source.done'"
	if !store.isSQLite() {
		stateQuery = "SELECT current_state FROM entity_state WHERE run_id=$1::uuid AND entity_id=$2::uuid"
		eventQuery = "SELECT COUNT(*) FROM events WHERE run_id=$1::uuid AND event_name='source.done'"
	}
	var state string
	if err := store.testDB().QueryRowContext(ctx, stateQuery, runID, entityID).Scan(&state); err != nil || state != "done" {
		t.Fatalf("state=%q error=%v, want done", state, err)
	}
	var eventsCount int
	if err := store.testDB().QueryRowContext(ctx, eventQuery, runID).Scan(&eventsCount); err != nil || eventsCount != 1 {
		t.Fatalf("emitted events=%d error=%v, want one", eventsCount, err)
	}
	bus.deliveryContinuations.mu.Lock()
	retained, exists := bus.deliveryContinuations.held[deliveryID]
	bus.deliveryContinuations.mu.Unlock()
	if exists || bus.outboxCount() != 1 {
		t.Fatalf("committed delivery retained continuation=%t/%t outbox=%d", retained, exists, bus.outboxCount())
	}
}
