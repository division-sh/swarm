package pipeline_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const (
	gateRecoveryBundle = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherGateBundle    = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type gateRecoveryModule struct {
	source semanticview.Source
}

func (m gateRecoveryModule) SemanticSource() semanticview.Source                   { return m.source }
func (gateRecoveryModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition { return nil }
func (gateRecoveryModule) WorkflowNodes() []runtimepipeline.WorkflowNode           { return nil }
func (gateRecoveryModule) GuardRegistry() runtimepipeline.GuardRegistry            { return nil }
func (gateRecoveryModule) ActionRegistry() runtimepipeline.ActionRegistry          { return nil }

type gateRecoveryStoreCase struct {
	name          string
	postgres      bool
	db            *sql.DB
	events        runtimebus.EventStore
	cards         decisioncard.Store
	workflowStore *runtimepipeline.WorkflowInstanceStore
}

func TestWorkflowGateUnavailablePinRecoversThroughPersistedEventBusOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testWorkflowGateUnavailablePinRecovery(t, tc.open(t))
		})
	}
}

func testWorkflowGateUnavailablePinRecovery(t *testing.T, tc gateRecoveryStoreCase) {
	t.Helper()
	ctx := context.Background()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	insertGateRecoveryRun(t, tc, runID)
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	bus, err := runtimebus.NewEventBus(tc.events)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	outcomeAgent := "gate-outcome-recorder"
	bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{AgentID: outcomeAgent})
	outcomeEvents := bus.Subscribe(outcomeAgent, events.EventType("launch.approved"))
	t.Cleanup(func() { bus.Unsubscribe(outcomeAgent) })

	bundle := gateRecoveryContractBundle()
	newCoordinator := func(bundleHash string) *runtimepipeline.PipelineCoordinator {
		return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, tc.db, runtimepipeline.PipelineCoordinatorOptions{
			Module:            gateRecoveryModule{source: semanticview.Wrap(bundle)},
			WorkflowStore:     tc.workflowStore,
			DecisionCards:     tc.cards,
			BundleFingerprint: bundleHash,
		})
	}
	matching := newCoordinator(gateRecoveryBundle)

	if err := tc.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: "launch/review-1", StorageRef: entityID, WorkflowName: "launch", WorkflowVersion: "1",
		CurrentState: "awaiting_review", EnteredStageAt: time.Now().UTC(),
		Metadata: map[string]any{"entity_id": entityID, "run_id": runID},
	}); err != nil {
		t.Fatalf("Upsert workflow instance: %v", err)
	}
	if err := tc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		return matching.ArmFlowInstanceInitialStageLifecycle(txctx, entityID)
	}); err != nil {
		t.Fatalf("arm initial gate: %v", err)
	}
	items, _, err := tc.cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("decision cards = %#v, %v", items, err)
	}
	card, err := tc.cards.GetDecisionCard(ctx, items[0].CardID)
	if err != nil {
		t.Fatalf("GetDecisionCard: %v", err)
	}
	decisionEventID := uuid.NewString()
	decidedAt := time.Now().UTC()
	if err := tc.workflowStore.CommitGateDecision(ctx, card, decisionEventID, decidedAt); err != nil {
		t.Fatalf("CommitGateDecision: %v", err)
	}
	if _, err := tc.cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
		CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator-1",
		ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: decidedAt,
	}); err != nil {
		t.Fatalf("DecideDecisionCard: %v", err)
	}

	bus.SetInterceptors(newCoordinator(otherGateBundle))
	payload, _ := json.Marshal(map[string]any{"card_id": card.CardID})
	decisionEvent := eventtest.RuntimeControl(
		decisionEventID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), card.FlowInstance), decidedAt,
	)
	if err := bus.PublishAcknowledged(ctx, decisionEvent); err != nil {
		t.Fatalf("PublishAcknowledged: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("wait for unavailable-pin dispatch: %v", err)
	}
	assertGateRecoveryActivation(t, tc.workflowStore, ctx, entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
	if got := gateRecoveryPipelineReceiptCount(t, tc, decisionEventID); got != 0 {
		t.Fatalf("unavailable pin pipeline receipt count = %d, want 0", got)
	}
	recovery := runtimepipeline.NewRecoveryManagerWith(tc.events, bus)
	if err := recovery.Recover(ctx); err != nil {
		t.Fatalf("Recover while pin unavailable: %v", err)
	}
	if got := gateRecoveryPipelineReceiptCount(t, tc, decisionEventID); got != 0 {
		t.Fatalf("unavailable pin recovery wrote terminal receipt count = %d, want 0", got)
	}

	bus.SetInterceptors(matching)
	if err := recovery.Recover(ctx); err != nil {
		t.Fatalf("Recover after pin restore: %v", err)
	}
	assertGateRecoveryActivation(t, tc.workflowStore, ctx, entityID, "operating", gateruntime.StatusRouted)
	outcomeEventID := gateRecoveryOutcomeEventID(t, tc, decisionEventID)
	if got := gateRecoveryDeliveryCount(t, tc, outcomeEventID, outcomeAgent); got != 1 {
		t.Fatalf("outcome delivery manifest count = %d, want 1", got)
	}
	select {
	case got := <-outcomeEvents:
		if got.ID() != outcomeEventID {
			t.Fatalf("delivered outcome id = %s, want %s", got.ID(), outcomeEventID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for authored gate outcome delivery")
	}
	assertGateRecoveryProcessedReceipt(t, tc, decisionEventID)

	if err := recovery.Recover(ctx); err != nil {
		t.Fatalf("idempotent second Recover: %v", err)
	}
	if got := gateRecoveryOutcomeEventCount(t, tc, decisionEventID); got != 1 {
		t.Fatalf("authored outcome count after idempotent recovery = %d, want 1", got)
	}
}

func openSQLiteGateRecoveryStore(t *testing.T) gateRecoveryStoreCase {
	selected := storetest.StartSQLiteRuntimeStore(t)
	return gateRecoveryStoreCase{
		name: "sqlite", db: selected.DB, events: selected, cards: selected,
		workflowStore: runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(selected.DB, selected),
	}
}

func openPostgresGateRecoveryStore(t *testing.T) gateRecoveryStoreCase {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := &store.PostgresStore{DB: db}
	return gateRecoveryStoreCase{
		name: "postgres", postgres: true, db: db, events: selected, cards: selected,
		workflowStore: runtimepipeline.NewWorkflowInstanceStore(db),
	}
}

func gateRecoveryContractBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "launch", Version: "1", InitialStage: "awaiting_review",
			Gates: []runtimecontracts.WorkflowGatePlan{{
				Stage: "awaiting_review", Decision: "launch_review",
				Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"approve": {Verdict: "approve", AdvancesTo: "operating", Emit: runtimecontracts.EmitSpec{Event: "launch.approved"}},
				},
			}},
		},
	}
}

func insertGateRecoveryRun(t *testing.T, tc gateRecoveryStoreCase, runID string) {
	t.Helper()
	query := `INSERT INTO runs (run_id, status) VALUES (?, 'running')`
	if tc.postgres {
		query = `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`
	}
	if _, err := tc.db.ExecContext(context.Background(), query, runID); err != nil {
		t.Fatalf("insert run: %v", err)
	}
}

func assertGateRecoveryActivation(t *testing.T, workflowStore *runtimepipeline.WorkflowInstanceStore, ctx context.Context, entityID, stage string, status gateruntime.Status) {
	t.Helper()
	instance, ok, err := workflowStore.Load(ctx, entityID)
	if err != nil || !ok {
		t.Fatalf("Load workflow instance = %#v, %v, %v", instance, ok, err)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		t.Fatalf("StateCarrierFromPersisted: %v", err)
	}
	activation, found, err := gateruntime.Load(carrier.StateBuckets, "", "launch_review")
	if err != nil || !found || instance.CurrentState != stage || activation.Status != status {
		t.Fatalf("gate state = stage:%s activation:%#v found:%v err:%v, want %s/%s", instance.CurrentState, activation, found, err, stage, status)
	}
}

func gateRecoveryPipelineReceiptCount(t *testing.T, tc gateRecoveryStoreCase, eventID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if tc.postgres {
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var count int
	if err := tc.db.QueryRowContext(context.Background(), query, eventID).Scan(&count); err != nil {
		t.Fatalf("count pipeline receipts: %v", err)
	}
	return count
}

func assertGateRecoveryProcessedReceipt(t *testing.T, tc gateRecoveryStoreCase, eventID string) {
	t.Helper()
	query := `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if tc.postgres {
		query = `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var outcome, reason string
	if err := tc.db.QueryRowContext(context.Background(), query, eventID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load final pipeline receipt: %v", err)
	}
	if outcome != "success" || reason != "pipeline_persisted" {
		t.Fatalf("final pipeline receipt = %s/%s, want success/pipeline_persisted", outcome, reason)
	}
}

func gateRecoveryOutcomeEventID(t *testing.T, tc gateRecoveryStoreCase, parentEventID string) string {
	t.Helper()
	query := `SELECT event_id FROM events WHERE event_name = 'launch.approved' AND source_event_id = ?`
	if tc.postgres {
		query = `SELECT event_id::text FROM events WHERE event_name = 'launch.approved' AND source_event_id = $1::uuid`
	}
	var eventID string
	if err := tc.db.QueryRowContext(context.Background(), query, parentEventID).Scan(&eventID); err != nil {
		t.Fatalf("load authored outcome event: %v", err)
	}
	return eventID
}

func gateRecoveryOutcomeEventCount(t *testing.T, tc gateRecoveryStoreCase, parentEventID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE event_name = 'launch.approved' AND source_event_id = ?`
	if tc.postgres {
		query = `SELECT COUNT(*) FROM events WHERE event_name = 'launch.approved' AND source_event_id = $1::uuid`
	}
	var count int
	if err := tc.db.QueryRowContext(context.Background(), query, parentEventID).Scan(&count); err != nil {
		t.Fatalf("count authored outcome events: %v", err)
	}
	return count
}

func gateRecoveryDeliveryCount(t *testing.T, tc gateRecoveryStoreCase, eventID, recipient string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ? AND subscriber_id = ?`
	args := []any{eventID, recipient}
	if tc.postgres {
		query = `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_id = $2`
	}
	var count int
	if err := tc.db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("count authored outcome deliveries: %v", err)
	}
	return count
}
