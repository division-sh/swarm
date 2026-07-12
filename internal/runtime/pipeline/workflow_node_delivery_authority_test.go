package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPipelineCoordinatorInterceptSkipsNodeWithoutPersistedDeliveryAuthority(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	ctx := context.Background()
	pc, bus := newDeliveryAuthorityCoordinator(t, db)
	runCtx := testPipelineCoordinatorRunContext(t, pc)
	evt := seedDeliveryAuthorityEvent(t, db, runCtx)
	seedDeliveryAuthorityWorkflowInstance(t, pc, runCtx, evt.EntityID())

	postCommit := make([]func(), 0, 1)
	ictx := WithPipelinePostCommitActions(ctx, &postCommit)
	passthrough, _, err := pc.Intercept(ictx, evt)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passthrough {
		t.Fatal("Intercept passthrough = false, want true when node delivery authority is missing")
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("published events = %d, want 0 without node delivery authority", got)
	}
	assertDeliveryAuthorityReceiptCount(t, db, evt.ID(), "node-a", 0)
	assertDeliveryAuthorityDeliveryCount(t, db, evt.ID(), "node-a", 0)
	logs := bus.runtimeLogEntries()
	if len(logs) != 1 || logs[0].Action != "delivery_authority_missing" {
		t.Fatalf("runtime logs = %#v, want one delivery_authority_missing entry", logs)
	}
}

func TestPipelineCoordinatorInterceptDeliveryRouteConsumesTargetWithoutGenericAuthorityLog(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	ctx := context.Background()
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
	targetPostCommit := make([]func(), 0, 1)
	targetCtx := WithPipelinePostCommitActions(ctx, &targetPostCommit)
	passthrough, _, err := pc.InterceptDeliveryRoute(targetCtx, targetEvt, route)
	if err != nil {
		t.Fatalf("target InterceptDeliveryRoute: %v", err)
	}
	if passthrough {
		t.Fatal("target InterceptDeliveryRoute passthrough = true, want false for consumed target-routed node event")
	}
	if deliveryAuthorityLogCount(bus.runtimeLogEntries()) != 0 {
		t.Fatalf("target runtime logs = %#v, want no false delivery_authority_missing log", bus.runtimeLogEntries())
	}
	assertDeliveryAuthorityReceiptCount(t, db, evt.ID(), route.SubscriberID, 1)
}

func TestPipelineCoordinatorInterceptReplayScopeMarkerDoesNotAuthorizeConcreteNode(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	ctx := context.Background()
	pc, bus := newDeliveryAuthorityCoordinator(t, db)
	runCtx := testPipelineCoordinatorRunContext(t, pc)
	evt := seedDeliveryAuthorityEvent(t, db, runCtx)
	seedDeliveryAuthorityWorkflowInstance(t, pc, runCtx, evt.EntityID())
	seedDeliveryAuthorityNodeDelivery(t, db, evt.ID(), "__runtime_replay_scope__")

	postCommit := make([]func(), 0, 1)
	ictx := WithPipelinePostCommitActions(ctx, &postCommit)
	passthrough, _, err := pc.Intercept(ictx, evt)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passthrough {
		t.Fatal("Intercept passthrough = false, want true when replay scope marker lacks concrete node authority")
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("published events = %d, want 0 with replay scope marker only", got)
	}
	assertDeliveryAuthorityReceiptCount(t, db, evt.ID(), "node-a", 0)
	assertDeliveryAuthorityDeliveryCount(t, db, evt.ID(), "node-a", 0)
	assertDeliveryAuthorityDeliveryCount(t, db, evt.ID(), "__runtime_replay_scope__", 1)
	logs := bus.runtimeLogEntries()
	if len(logs) != 1 || logs[0].Action != "delivery_authority_missing" {
		t.Fatalf("runtime logs = %#v, want one delivery_authority_missing entry", logs)
	}
}

func TestPipelineCoordinatorInterceptTerminalNodeDeliveryDoesNotAuthorizeExecution(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     string
		retryCount int
	}{
		{name: "dead_letter", status: "dead_letter", retryCount: 2},
		{name: "retry_exhausted_failed", status: "failed", retryCount: DefaultSystemNodeRetryLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
			ctx := context.Background()
			pc, bus := newDeliveryAuthorityCoordinator(t, db)
			runCtx := testPipelineCoordinatorRunContext(t, pc)
			evt := seedDeliveryAuthorityEvent(t, db, runCtx)
			seedDeliveryAuthorityWorkflowInstance(t, pc, runCtx, evt.EntityID())
			seedDeliveryAuthorityNodeDeliveryStatus(t, db, evt.ID(), "node-a", tc.status, tc.retryCount)

			postCommit := make([]func(), 0, 1)
			ictx := WithPipelinePostCommitActions(ctx, &postCommit)
			passthrough, _, err := pc.Intercept(ictx, evt)
			if err != nil {
				t.Fatalf("Intercept: %v", err)
			}
			if !passthrough {
				t.Fatal("Intercept passthrough = false, want true when terminal node delivery is non-executable")
			}
			if got := bus.publishedCount(); got != 0 {
				t.Fatalf("published events = %d, want 0 for terminal node delivery", got)
			}
			assertDeliveryAuthorityReceiptCount(t, db, evt.ID(), "node-a", 0)
			assertDeliveryAuthorityDeliveryCount(t, db, evt.ID(), "node-a", 1)
			logs := bus.runtimeLogEntries()
			if len(logs) != 1 || logs[0].Action != "delivery_authority_missing" {
				t.Fatalf("runtime logs = %#v, want one delivery_authority_missing entry", logs)
			}
		})
	}
}

func TestPipelineCoordinatorInterceptSkipsWhenCanonicalDeliveryAuthorityUnavailable(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	ctx := context.Background()
	pc, bus := newDeliveryAuthorityCoordinatorWithReceipts(t, db, false)
	runCtx := testPipelineCoordinatorRunContext(t, pc)
	evt := seedDeliveryAuthorityEvent(t, db, runCtx)
	seedDeliveryAuthorityWorkflowInstance(t, pc, runCtx, evt.EntityID())
	seedDeliveryAuthorityNodeDelivery(t, db, evt.ID(), "node-a")

	postCommit := make([]func(), 0, 1)
	ictx := WithPipelinePostCommitActions(ctx, &postCommit)
	passthrough, _, err := pc.Intercept(ictx, evt)
	if err != nil {
		t.Fatalf("Intercept: %v", err)
	}
	if !passthrough {
		t.Fatal("Intercept passthrough = false, want true when delivery authority owner is unavailable")
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("published events = %d, want 0 when delivery authority owner is unavailable", got)
	}
	assertDeliveryAuthorityReceiptCount(t, db, evt.ID(), "node-a", 0)
}

func TestPipelineCoordinatorInterceptSettlesAuthorizedNodeDelivery(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	ctx := context.Background()
	pc, _ := newDeliveryAuthorityCoordinator(t, db)
	runCtx := testPipelineCoordinatorRunContext(t, pc)
	evt := seedDeliveryAuthorityEvent(t, db, runCtx)
	seedDeliveryAuthorityWorkflowInstance(t, pc, runCtx, evt.EntityID())
	seedDeliveryAuthorityNodeDelivery(t, db, evt.ID(), "node-a")

	handled, err := pc.dispatchWorkflowNodeEventResult(runCtx, evt)
	if err != nil {
		t.Fatalf("dispatchWorkflowNodeEventResult: %v", err)
	}
	if !handled {
		t.Fatal("dispatchWorkflowNodeEventResult handled = false, want true for authorized node delivery")
	}
	assertDeliveryAuthorityReceiptCount(t, db, evt.ID(), "node-a", 1)
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
	return newDeliveryAuthorityCoordinatorWithReceipts(t, db, true)
}

func newDeliveryAuthorityCoordinatorWithReceipts(t *testing.T, db *sql.DB, receiptsEnabled bool) (*PipelineCoordinator, *recordingPipelineBus) {
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
		EventReceiptsCapability: eventReceiptsCapabilityStub{enabled: receiptsEnabled}.resolve,
	})
	return pc, bus
}

func seedDeliveryAuthorityEvent(t *testing.T, db *sql.DB, ctx context.Context) events.Event {
	t.Helper()
	entityID := uuid.NewString()
	evt := eventtest.RootIngress(
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

	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, entity_id, flow_instance, scope, payload,
			produced_by, produced_by_type, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3, $4::uuid, 'runtime', 'entity', $5::jsonb,
			'src', 'agent', now()
		)
	`, evt.ID(), evt.RunID(), string(evt.Type()), entityID, string(evt.Payload())); err != nil {
		t.Fatalf("seed delivery authority event: %v", err)
	}
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

func seedDeliveryAuthorityNodeDelivery(t *testing.T, db *sql.DB, eventID, nodeID string) {
	t.Helper()
	seedDeliveryAuthorityNodeDeliveryStatus(t, db, eventID, nodeID, "pending", 0)
}

func seedDeliveryAuthorityNodeDeliveryForTarget(t *testing.T, db *sql.DB, eventID, nodeID string, target events.RouteIdentity) {
	t.Helper()
	raw, err := json.Marshal(target.Normalized())
	if err != nil {
		t.Fatalf("marshal delivery authority target: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, delivery_target_route, status, retry_count, created_at)
		VALUES ($1::uuid, $2::uuid, 'node', $3, $4::jsonb, 'pending', 0, now())
	`, testPipelineRunID, eventID, nodeID, string(raw)); err != nil {
		t.Fatalf("seed target delivery authority node delivery: %v", err)
	}
}

func seedDeliveryAuthorityNodeDeliveryStatus(t *testing.T, db *sql.DB, eventID, nodeID, status string, retryCount int) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, status, retry_count, created_at)
		VALUES ($1::uuid, $2::uuid, 'node', $3, $4, $5, now())
	`, testPipelineRunID, eventID, nodeID, status, retryCount); err != nil {
		t.Fatalf("seed delivery authority node delivery: %v", err)
	}
}

func assertDeliveryAuthorityReceiptCount(t *testing.T, db *sql.DB, eventID, nodeID string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodeID).Scan(&got); err != nil {
		t.Fatalf("count delivery authority node receipts: %v", err)
	}
	if got != want {
		t.Fatalf("delivery authority node receipts = %d, want %d", got, want)
	}
}

func assertDeliveryAuthorityDeliveryCount(t *testing.T, db *sql.DB, eventID, nodeID string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(), `
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
