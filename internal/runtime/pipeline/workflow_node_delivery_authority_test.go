package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPipelineCoordinatorInterceptSkipsNodeWithoutPersistedDeliveryAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc, bus := newDeliveryAuthorityCoordinator(t, db)
	runCtx := testPipelineCoordinatorRunContext(t, pc)
	evt := seedDeliveryAuthorityEvent(t, db, runCtx)
	seedDeliveryAuthorityWorkflowInstance(t, pc, runCtx, evt.EntityID())

	postCommit := make([]OwnerAction, 0, 1)
	ictx := WithPipelinePostCommitActions(runCtx, &postCommit)
	passthrough, _, err := pc.Intercept(ictx, evt)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if passthrough {
		t.Fatal("Intercept passthrough = true, want consumed event with skipped node execution")
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("published events = %d, want 0 without node delivery authority", got)
	}
	assertDeliveryAuthorityOutcomeCount(t, db, evt.ID(), "node-a", 0)
	assertDeliveryAuthorityDeliveryCount(t, db, evt.ID(), "node-a", 0)
}

func TestPipelineCoordinatorInterceptDeliveryRouteConsumesTargetWithoutGenericAuthorityLog(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc, bus := newDeliveryAuthorityCoordinator(t, db)
	runCtx := testPipelineCoordinatorRunContext(t, pc)
	evt := seedDeliveryAuthorityEvent(t, db, runCtx)
	seedDeliveryAuthorityWorkflowInstance(t, pc, runCtx, evt.EntityID())

	target := events.RouteIdentity{
		EntityID: evt.EntityID(),
	}
	route := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "node-a",
		Target:         target,
	}
	seedDeliveryAuthorityNodeDeliveryForTarget(t, db, evt.ID(), route.SubscriberID, target)
	targetEvt := eventtest.TargetRouted(evt, target)
	delivery, err := events.NewDeliveryEvent(targetEvt, route)
	if err != nil {
		t.Fatalf("NewDeliveryEvent: %v", err)
	}
	targetPostCommit := make([]OwnerAction, 0, 1)
	targetCtx := WithPipelinePostCommitActions(runCtx, &targetPostCommit)
	passthrough, _, err := pc.InterceptDeliveryRoute(targetCtx, delivery, route)
	if err != nil {
		t.Fatalf("target InterceptDeliveryRoute: %v", err)
	}
	if passthrough {
		t.Fatal("target InterceptDeliveryRoute passthrough = true, want false for consumed target-routed node event")
	}
	if deliveryAuthorityLogCount(bus.runtimeLogEntries()) != 0 {
		t.Fatalf("target runtime logs = %#v, want no false delivery_authority_missing log", bus.runtimeLogEntries())
	}
	assertDeliveryAuthorityOutcomeCount(t, db, evt.ID(), route.SubscriberID, 1)
}

func TestPipelineCoordinatorInterceptDeliveryRouteRejectsAmbiguousConnectedInputReplayWithoutReceiptOrHandler(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	source := testWorkflowNodeConnectedInputCollisionSource()
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("connected-input collision source has no contract bundle")
	}
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		DeliveryStore: newPipelineTestDeliveryOwnerForDB(t, db),
		Module: &previewWorkflowModule{
			bundle: bundle,
			workflowNodes: []WorkflowNode{{
				ID:            "receiver-node",
				Subscriptions: []events.EventType{"deploy.accepted", "deploy.audited"},
			}},
		},
	})
	testPipelineCoordinatorRunContext(t, pc)

	entityID := uuid.NewString()
	eventID := uuid.NewString()
	target := events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver", EntityID: entityID}
	evt := eventtest.RunCreatingRootIngress(eventID, "producer/deploy.done", "producer", "", []byte(`{}`), 0, testPipelineRunID, "", events.EventEnvelope{
		EntityID:     target.EntityID,
		FlowInstance: target.FlowInstance,
		Source:       events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString()},
		Target:       target,
	}, time.Now().UTC())
	ctx := testAuthorActivityContext(t, context.Background())
	seedPipelineEventRecord(t, ctx, db, evt)
	route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "receiver-node", Target: target}
	seedDeliveryAuthorityNodeDeliveryForTarget(t, db, eventID, route.SubscriberID, target)
	delivery, err := events.NewDeliveryEvent(evt, route)
	if err != nil {
		t.Fatalf("NewDeliveryEvent: %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		passthrough, deferred, err := pc.InterceptDeliveryRoute(ctx, delivery, route)
		if err == nil || !strings.Contains(err.Error(), "multiple connected input events") {
			t.Fatalf("attempt %d InterceptDeliveryRoute error = %v, want explicit receiver-pin ambiguity", attempt, err)
		}
		if passthrough {
			t.Fatalf("attempt %d passthrough = true, want fail-closed interception", attempt)
		}
		if len(deferred) != 0 {
			t.Fatalf("attempt %d deferred events = %#v, want none", attempt, deferred)
		}
	}
	assertDeliveryAuthorityOutcomeCount(t, db, eventID, route.SubscriberID, 0)
	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2
	`, eventID, route.SubscriberID).Scan(&status); err != nil {
		t.Fatalf("load ambiguous connected-input delivery: %v", err)
	}
	if status != "pending" {
		t.Fatalf("delivery status = %q, want pending after rejected replay", status)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("published handler events = %d, want zero", got)
	}
}

func TestPipelineCoordinatorInterceptTerminalNodeDeliveryDoesNotAuthorizeExecution(t *testing.T) {
	for _, name := range []string{"dead_letter"} {
		t.Run(name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			ctx := testAuthorActivityContext(t, context.Background())
			pc, bus := newDeliveryAuthorityCoordinator(t, db)
			runCtx := testPipelineCoordinatorRunContext(t, pc)
			evt := seedDeliveryAuthorityEvent(t, db, runCtx)
			seedDeliveryAuthorityWorkflowInstance(t, pc, runCtx, evt.EntityID())
			seedDeliveryAuthorityTerminalNodeDelivery(t, db, evt.ID(), "node-a")

			postCommit := make([]OwnerAction, 0, 1)
			ictx := WithPipelinePostCommitActions(ctx, &postCommit)
			passthrough, _, err := pc.Intercept(ictx, evt)
			if err != nil {
				t.Fatalf("Intercept: %v", err)
			}
			if passthrough {
				t.Fatal("Intercept passthrough = true, want consumed event with terminal node execution skipped")
			}
			if got := bus.publishedCount(); got != 0 {
				t.Fatalf("published events = %d, want 0 for terminal node delivery", got)
			}
			assertDeliveryAuthorityOutcomeCount(t, db, evt.ID(), "node-a", 1)
			assertDeliveryAuthorityDeliveryCount(t, db, evt.ID(), "node-a", 1)
		})
	}
}

func TestPipelineCoordinatorInterceptSettlesAuthorizedNodeDelivery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(t, context.Background())
	pc, _ := newDeliveryAuthorityCoordinator(t, db)
	runCtx := testPipelineCoordinatorRunContext(t, pc)
	evt := seedDeliveryAuthorityEvent(t, db, runCtx)
	seedDeliveryAuthorityWorkflowInstance(t, pc, runCtx, evt.EntityID())
	route := seedDeliveryAuthorityNodeDelivery(t, db, evt.ID(), "node-a")

	handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(runCtx, route), evt)
	if err != nil {
		t.Fatalf("dispatchWorkflowNodeEventResult: %v", err)
	}
	if !handled {
		t.Fatal("dispatchWorkflowNodeEventResult handled = false, want true for authorized node delivery")
	}
	assertDeliveryAuthorityOutcomeCount(t, db, evt.ID(), "node-a", 1)
	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'node-a'
	`, evt.ID()).Scan(&status); err != nil {
		t.Fatalf("load authorized node delivery: %v", err)
	}
	if status != "delivered" {
		t.Fatalf("authorized node delivery status = %q, want delivered", status)
	}
}

func newDeliveryAuthorityCoordinator(t *testing.T, db *sql.DB) (*PipelineCoordinator, *recordingPipelineBus) {
	t.Helper()
	bus := &recordingPipelineBus{}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"node-a": {
					"source.evt": {
						Rules: []runtimecontracts.HandlerRuleEntry{{
							ID:         "complete",
							Condition:  "true",
							AdvancesTo: "done",
						}},
					},
				},
			},
		},
	}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		DeliveryStore: newPipelineTestDeliveryOwnerForDB(t, db),
		Module: &previewWorkflowModule{
			bundle: bundle,
			workflow: NewWorkflowDefinition("delivery-authority", []WorkflowStage{
				{Name: "queued"},
				{Name: "done", Terminal: true},
			}, []WorkflowTransition{{
				Name: "complete",
				From: []WorkflowStateID{"queued"},
				To:   "done",
				Node: "node-a",
			}}),
			workflowNodes: []WorkflowNode{{
				ID:            "node-a",
				Subscriptions: []events.EventType{"source.evt"},
				Policies: map[string]WorkflowEventPolicy{
					"source.evt": {Consume: true},
				},
			}},
		},
	})
	return pc, bus
}

func seedDeliveryAuthorityEvent(t *testing.T, db *sql.DB, ctx context.Context) events.Event {
	t.Helper()
	entityID := uuid.NewString()
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("source.evt"),
		"src",
		"",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)

	seedPipelineEventRecord(t, ctx, db, evt)
	return evt
}

func seedDeliveryAuthorityWorkflowInstance(t *testing.T, pc *PipelineCoordinator, ctx context.Context, entityID string) {
	t.Helper()
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "delivery-authority",
		WorkflowVersion: "v-test",
		CurrentState:    "queued",
	}); err != nil {
		t.Fatalf("seed delivery authority workflow instance: %v", err)
	}
}

func seedDeliveryAuthorityNodeDelivery(t *testing.T, db *sql.DB, eventID, nodeID string) events.DeliveryRoute {
	t.Helper()
	evt, err := newPipelineTestDeliveryOwnerForDB(t, db).loadEvent(testAuthorActivityContext(t, context.Background()), eventID)
	if err != nil {
		t.Fatalf("load delivery authority event: %v", err)
	}
	return seedDeliveryAuthorityNodeDeliveryForTarget(t, db, eventID, nodeID, events.RouteIdentity{EntityID: evt.EntityID()})
}

func seedDeliveryAuthorityNodeDeliveryForTarget(t *testing.T, db *sql.DB, eventID, nodeID string, target events.RouteIdentity) events.DeliveryRoute {
	t.Helper()
	owner := newPipelineTestDeliveryOwnerForDB(t, db)
	evt, err := owner.loadEvent(testAuthorActivityContext(t, context.Background()), eventID)
	if err != nil {
		t.Fatalf("load delivery authority event: %v", err)
	}
	route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: nodeID, Target: target}
	if err := owner.commitInitial(testAuthorActivityContext(t, context.Background()), evt, route); err != nil {
		t.Fatalf("seed target delivery authority node delivery: %v", err)
	}
	return route
}

func seedDeliveryAuthorityTerminalNodeDelivery(t *testing.T, db *sql.DB, eventID, nodeID string) {
	t.Helper()
	ctx := testAuthorActivityContext(t, context.Background())
	owner := newPipelineTestDeliveryOwnerForDB(t, db)
	evt, err := owner.loadEvent(ctx, eventID)
	if err != nil {
		t.Fatalf("load terminal delivery authority event: %v", err)
	}
	route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: nodeID, Target: events.RouteIdentity{EntityID: evt.EntityID()}}
	if err := owner.commitInitial(ctx, evt, route); err != nil {
		t.Fatalf("commit terminal delivery authority: %v", err)
	}
	claimed, err := owner.ClaimNodeDelivery(ctx, evt, route)
	if err != nil {
		t.Fatalf("claim terminal delivery authority: %v", err)
	}
	failure := runtimefailures.FromError(errors.New("terminal delivery fixture"), "pipeline-test", "settle").Failure
	if _, err := owner.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
		Disposition: runtimedelivery.FailureDeadLetter,
		ReasonCode:  "terminal_delivery_fixture",
		Failure:     &failure,
	}); err != nil {
		t.Fatalf("settle terminal delivery authority: %v", err)
	}
}

func assertDeliveryAuthorityOutcomeCount(t *testing.T, db *sql.DB, eventID, nodeID string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT COUNT(*)
		FROM event_delivery_outcomes o
		JOIN event_deliveries d ON d.delivery_id = o.delivery_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = $2
	`, eventID, nodeID).Scan(&got); err != nil {
		t.Fatalf("count delivery authority node outcomes: %v", err)
	}
	if got != want {
		t.Fatalf("delivery authority node outcomes = %d, want %d", got, want)
	}
}

func assertDeliveryAuthorityDeliveryCount(t *testing.T, db *sql.DB, eventID, nodeID string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodeID).Scan(&got); err != nil {
		t.Fatalf("count delivery authority node deliveries: %v", err)
	}
	if got != want {
		t.Fatalf("delivery authority node deliveries = %d, want %d", got, want)
	}
}

func deliveryAuthorityLogCount(logs []RuntimeLogEntry) int {
	count := 0
	for _, log := range logs {
		if log.Action == "delivery_authority_missing" {
			count++
		}
	}
	return count
}
