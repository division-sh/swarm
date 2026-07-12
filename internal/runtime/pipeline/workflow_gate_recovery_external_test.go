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
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
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

type gateRecoveryFairnessInterceptor struct {
	deferred map[string]struct{}
}

type gateRecoveryPoisonInterceptor struct {
	poisonEventID string
}

func (i gateRecoveryPoisonInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	if evt.ID() != i.poisonEventID {
		return true, nil, nil
	}
	return false, nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "decision_route_fixture_invalid", "test", "poison_route", nil)
}

func (i gateRecoveryFairnessInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	if _, ok := i.deferred[evt.ID()]; !ok {
		return true, nil, nil
	}
	return true, nil, runtimepipeline.DeferPipelineReceipt(runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "decision_card_bundle_unavailable", "test", "fairness", nil))
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

func TestWorkflowGateStartupRecoverySettlesTerminalNoEmitOutcomeOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testWorkflowGateStartupTerminalRecovery(t, tc.open(t))
		})
	}
}

func TestDecisionRouteObligationFairnessAdmitsNewWorkBehindFullDeferredPageOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			ctx := context.Background()
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			deferred := map[string]struct{}{}
			oldAt := time.Now().UTC().Add(-25 * time.Hour)
			for i := 0; i < 200; i++ {
				eventID := seedGateRecoveryRouteObligation(t, selected, runID, oldAt.Add(time.Duration(i)*time.Millisecond))
				deferred[eventID] = struct{}{}
				setGateRecoveryRouteAttempt(t, selected, eventID, 1)
			}
			newEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC())
			bus, err := runtimebus.NewEventBusWithOptions(selected.events, runtimebus.EventBusOptions{Interceptors: []runtimebus.EventInterceptor{gateRecoveryFairnessInterceptor{deferred: deferred}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := bus.SweepUndispatched(ctx, 24*time.Hour, 200); err != nil {
				t.Fatal(err)
			}
			assertGateRecoveryProcessedReceipt(t, selected, newEventID)
			if got := gateRecoveryPipelineReceiptCount(t, selected, firstGateRecoveryEventID(deferred)); got != 0 {
				t.Fatalf("deferred retry receipt count = %d, want 0", got)
			}
		})
	}
}

func TestDecisionRouteObligationQuarantinesPoisonAndContinuesOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			poisonEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC().Add(-time.Minute))
			validEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC())
			bus, err := runtimebus.NewEventBusWithOptions(selected.events, runtimebus.EventBusOptions{
				Interceptors: []runtimebus.EventInterceptor{gateRecoveryPoisonInterceptor{poisonEventID: poisonEventID}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got, err := bus.SweepUndispatched(context.Background(), time.Hour, 10); err != nil || got != 1 {
				t.Fatalf("poison route sweep recovered = %d, %v; want 1, nil", got, err)
			}
			assertGateRecoveryObligationStatus(t, selected, poisonEventID, "quarantined")
			assertGateRecoveryErrorReceipt(t, selected, poisonEventID, "event_interceptor_failed")
			assertGateRecoveryProcessedReceipt(t, selected, validEventID)
			if got, err := bus.SweepUndispatched(context.Background(), time.Hour, 10); err != nil || got != 0 {
				t.Fatalf("second poison route sweep recovered = %d, %v; want 0, nil", got, err)
			}
		})
	}
}

func TestDecisionRouteStartupRecoveryQuarantinesPoisonAndContinuesOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			poisonEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC().Add(-time.Minute))
			validEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC())
			bus, err := runtimebus.NewEventBusWithOptions(selected.events, runtimebus.EventBusOptions{
				Interceptors: []runtimebus.EventInterceptor{gateRecoveryPoisonInterceptor{poisonEventID: poisonEventID}},
			})
			if err != nil {
				t.Fatal(err)
			}
			recovery := runtimepipeline.NewRecoveryManagerWith(selected.events, bus)
			if err := recovery.Recover(context.Background()); err != nil {
				t.Fatalf("startup poison route recovery: %v", err)
			}
			assertGateRecoveryObligationStatus(t, selected, poisonEventID, "quarantined")
			assertGateRecoveryErrorReceipt(t, selected, poisonEventID, "event_interceptor_failed")
			assertGateRecoveryProcessedReceipt(t, selected, validEventID)
			if err := recovery.Recover(context.Background()); err != nil {
				t.Fatalf("second startup poison route recovery: %v", err)
			}
		})
	}
}

func seedGateRecoveryRouteObligation(t *testing.T, tc gateRecoveryStoreCase, runID string, at time.Time) string {
	t.Helper()
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: runID, FlowInstance: "launch/recovery", FlowID: "launch", EntityID: uuid.NewString(),
		Stage: "awaiting_review", StageActivationID: uuid.NewString(), DecisionID: "launch_review",
		Snapshot:   decisioncard.Snapshot{Decision: "launch_review", Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "operating"}}},
		BundleHash: gateRecoveryBundle, EffectiveCadence: decisioncard.Cadence{ReminderInterval: "24h", InputDraftTTL: "15m"}, CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.cards.CreateDecisionCard(context.Background(), card); err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	if _, err := tc.cards.DecideDecisionCard(context.Background(), decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: eventID, Now: at}); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"card_id": card.CardID})
	evt := eventtest.RuntimeControl(eventID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, card.EntityID), card.FlowInstance), at)
	if err := tc.events.AppendEvent(context.Background(), evt); err != nil {
		t.Fatal(err)
	}
	scopeWriter, ok := tc.events.(interface {
		UpsertCommittedReplayScope(context.Context, string, runtimereplayclaim.CommittedReplayScope) error
	})
	if !ok {
		t.Fatalf("event store %T lacks replay scope writer", tc.events)
	}
	if err := scopeWriter.UpsertCommittedReplayScope(context.Background(), eventID, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
		t.Fatal(err)
	}
	return eventID
}

func setGateRecoveryRouteAttempt(t *testing.T, tc gateRecoveryStoreCase, eventID string, attempt int) {
	t.Helper()
	query := `UPDATE decision_card_route_obligations SET attempt_count = ?, next_attempt_at = ? WHERE event_id = ?`
	args := []any{attempt, time.Now().UTC().Add(-time.Second), eventID}
	if tc.postgres {
		query = `UPDATE decision_card_route_obligations SET attempt_count = $1, next_attempt_at = $2 WHERE event_id = $3::uuid`
	}
	if _, err := tc.db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func firstGateRecoveryEventID(ids map[string]struct{}) string {
	for id := range ids {
		return id
	}
	return ""
}

func testWorkflowGateStartupTerminalRecovery(t *testing.T, tc gateRecoveryStoreCase) {
	t.Helper()
	ctx := context.Background()
	runID, entityID := uuid.NewString(), uuid.NewString()
	insertGateRecoveryRun(t, tc, runID)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	bundle := gateRecoveryTerminalContractBundle()
	bus, err := runtimebus.NewEventBusWithOptions(tc.events, runtimebus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatal(err)
	}
	newCoordinator := func(bundleHash string) *runtimepipeline.PipelineCoordinator {
		return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, tc.db, runtimepipeline.PipelineCoordinatorOptions{
			Module: gateRecoveryModule{source: semanticview.Wrap(bundle)}, WorkflowStore: tc.workflowStore,
			DecisionCards: tc.cards, BundleFingerprint: bundleHash,
		})
	}
	matching := newCoordinator(gateRecoveryBundle)
	enteredAt := time.Now().UTC().Add(-25 * time.Hour)
	if err := tc.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: "launch/review-terminal", StorageRef: entityID, WorkflowName: "launch", WorkflowVersion: "1",
		CurrentState: "awaiting_review", EnteredStageAt: enteredAt,
		Metadata: map[string]any{"entity_id": entityID, "run_id": runID},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		return matching.ArmFlowInstanceInitialStageLifecycle(txctx, entityID)
	}); err != nil {
		t.Fatal(err)
	}
	items, _, err := tc.cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("decision cards = %#v, %v", items, err)
	}
	card, err := tc.cards.GetDecisionCard(ctx, items[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	decidedAt := enteredAt.Add(time.Minute)
	if err := tc.workflowStore.CommitGateDecision(ctx, card, eventID, decidedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: eventID, Now: decidedAt}); err != nil {
		t.Fatal(err)
	}
	bus.SetInterceptors(newCoordinator(otherGateBundle))
	payload, _ := json.Marshal(map[string]any{"card_id": card.CardID})
	evt := eventtest.RuntimeControl(eventID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), card.FlowInstance), decidedAt)
	if err := bus.PublishAcknowledged(ctx, evt); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatal(err)
	}
	makeGateRecoveryRouteDue(t, tc, eventID, time.Now().Add(-time.Second))
	bus.SetInterceptors(matching)
	if err := runtimepipeline.NewRecoveryManagerWith(tc.events, bus).Recover(ctx); err != nil {
		t.Fatal(err)
	}
	assertGateRecoveryActivation(t, tc.workflowStore, ctx, entityID, "completed", gateruntime.StatusRouted)
	var status string
	query := `SELECT status FROM runs WHERE run_id = ?`
	if tc.postgres {
		query = `SELECT status FROM runs WHERE run_id = $1::uuid`
	}
	if err := tc.db.QueryRowContext(ctx, query, runID).Scan(&status); err != nil || status != "completed" {
		t.Fatalf("terminal no-emit recovered run status = %q, %v", status, err)
	}
	assertGateRecoveryProcessedReceipt(t, tc, eventID)
}

func testWorkflowGateUnavailablePinRecovery(t *testing.T, tc gateRecoveryStoreCase) {
	t.Helper()
	ctx := context.Background()
	runID := uuid.NewString()
	entityID := uuid.NewString()
	insertGateRecoveryRun(t, tc, runID)
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	bundle := gateRecoveryContractBundle()
	bus, err := runtimebus.NewEventBusWithOptions(tc.events, runtimebus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	outcomeAgent := "gate-outcome-recorder"
	bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{AgentID: outcomeAgent})
	outcomeEvents := bus.Subscribe(outcomeAgent, events.EventType("launch.approved"))
	t.Cleanup(func() { bus.Unsubscribe(outcomeAgent) })

	newCoordinator := func(bundleHash string) *runtimepipeline.PipelineCoordinator {
		return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, tc.db, runtimepipeline.PipelineCoordinatorOptions{
			Module:            gateRecoveryModule{source: semanticview.Wrap(bundle)},
			WorkflowStore:     tc.workflowStore,
			DecisionCards:     tc.cards,
			BundleFingerprint: bundleHash,
		})
	}
	matching := newCoordinator(gateRecoveryBundle)

	scenarioAt := time.Now().UTC().Add(-25 * time.Hour)
	if err := tc.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: "launch/review-1", StorageRef: entityID, WorkflowName: "launch", WorkflowVersion: "1",
		CurrentState: "awaiting_review", EnteredStageAt: scenarioAt,
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
	decidedAt := scenarioAt.Add(time.Minute)
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
	makeGateRecoveryRouteDue(t, tc, decisionEventID, time.Now().Add(-time.Second))
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

func makeGateRecoveryRouteDue(t *testing.T, tc gateRecoveryStoreCase, eventID string, due time.Time) {
	t.Helper()
	query := `UPDATE decision_card_route_obligations SET next_attempt_at = ? WHERE event_id = ?`
	if tc.postgres {
		query = `UPDATE decision_card_route_obligations SET next_attempt_at = $1 WHERE event_id = $2::uuid`
	}
	if _, err := tc.db.ExecContext(context.Background(), query, due.UTC(), eventID); err != nil {
		t.Fatalf("make decision route obligation due: %v", err)
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
		RootSchema: nil,
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

func gateRecoveryTerminalContractBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: nil,
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "launch", Version: "1", InitialStage: "awaiting_review", TerminalStages: []string{"completed"},
			Gates: []runtimecontracts.WorkflowGatePlan{{
				Stage: "awaiting_review", Decision: "launch_review",
				Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "completed"}},
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

func assertGateRecoveryObligationStatus(t *testing.T, tc gateRecoveryStoreCase, eventID, want string) {
	t.Helper()
	query := `SELECT status FROM decision_card_route_obligations WHERE event_id = ?`
	if tc.postgres {
		query = `SELECT status FROM decision_card_route_obligations WHERE event_id = $1::uuid`
	}
	var got string
	if err := tc.db.QueryRowContext(context.Background(), query, eventID).Scan(&got); err != nil || got != want {
		t.Fatalf("decision route obligation status = %q, %v; want %q", got, err, want)
	}
}

func assertGateRecoveryErrorReceipt(t *testing.T, tc gateRecoveryStoreCase, eventID, wantReason string) {
	t.Helper()
	query := `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if tc.postgres {
		query = `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var outcome, reason string
	if err := tc.db.QueryRowContext(context.Background(), query, eventID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load quarantined pipeline receipt: %v", err)
	}
	if outcome != "dead_letter" || reason != wantReason {
		t.Fatalf("quarantined pipeline receipt = %s/%s, want dead_letter/%s", outcome, reason, wantReason)
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
