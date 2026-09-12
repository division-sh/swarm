package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type gateConsumerOutcomeStore struct {
	workflowTestSelectedStore
	injected error
	calls    int
}

func (s *gateConsumerOutcomeStore) CommitWorkflowEngineMutation(ctx context.Context, command runtimepipeline.WorkflowEngineMutationCommand) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	s.calls++
	result, err := s.workflowTestSelectedStore.CommitWorkflowEngineMutation(ctx, command)
	if result.Committed && len(command.Publications) > 0 {
		err = errors.Join(err, s.injected)
	}
	return result, err
}

func TestWorkflowGateConsumesCommittedErrorWithoutRouteReplayOnBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		for _, fail := range []bool{false, true} {
			phase := "healthy"
			if fail {
				phase = "postcommit_error"
			}
			t.Run(backend.name+"/"+phase, func(t *testing.T) {
				selected, db, seedCtx := backend.open(t)
				runID := runtimecorrelation.RunIDFromContext(seedCtx)
				ctx := authorGenericScheduleConsumerContext(runID)
				ctx, bindErr := eventreceiver.NormalExecution().Bind(ctx, executionmode.Live)
				if bindErr != nil {
					t.Fatal(bindErr)
				}
				registerTestAuthorActivityCatalogForContext(t, selected.(testAuthorActivityCatalogRegistrar), testAuthorActivityContext())
				store := &gateConsumerOutcomeStore{workflowTestSelectedStore: selected.(workflowTestSelectedStore)}
				injected := errors.New("injected gate cleanup after selected COMMIT")
				if fail {
					store.injected = injected
				}
				bundle := runControlTimerBundle()
				bundle.Semantics.Timers = nil
				bundle.Semantics.InitialStage = "awaiting_review"
				bundle.RootEntities = runtimecontracts.EntityContractsDocument{"default": {Fields: map[string]runtimecontracts.EntityFieldDecl{}}}
				bundle.Events = map[string]runtimecontracts.EventCatalogEntry{"test.node_emitted": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{}}}}
				source := semanticview.Wrap(bundle)
				bus, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore), runtimebus.EventBusOptions{ContractBundle: source})
				if err != nil {
					t.Fatal(err)
				}
				opts := completeWorkflowTestCoordinatorOptions(runtimepipeline.NewWorkflowPersistence(store), store)
				opts.Module = runForkGateWorkflowModule{source: source}
				opts.SourceArtifactFact = mustStoreTestSourceArtifactFact(authorActivityTestBundleHash)
				coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, opts)
				if coordinator == nil {
					t.Fatal("construct gate coordinator")
				}
				now := time.Now().UTC().Add(-time.Minute)
				routes := map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "done", Emit: runtimecontracts.EmitSpec{Event: "test.node_emitted"}, EmitSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}}}
				frozen, err := gateruntime.FreezeRoutes(routes)
				if err != nil {
					t.Fatal(err)
				}
				activation, err := gateruntime.New(runID, runID, runID, ".", "awaiting_review", "root_review", authorActivityTestBundleHash, frozen, "state:awaiting_review", now)
				if err != nil {
					t.Fatal(err)
				}
				buckets := map[string]map[string]any{}
				if err := gateruntime.Store(buckets, activation); err != nil {
					t.Fatal(err)
				}
				scope := runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: runtimeflowidentity.RouteForInstancePath(runID)}
				_, err = coordinator.MaterializeInitialEntry(ctx, scope, runtimepipeline.WorkflowInstance{InstanceID: runID, StorageRef: runID, EntityID: runID, EntityType: "default", WorkflowName: ".", WorkflowVersion: "1", CurrentState: "awaiting_review", EnteredStageAt: now, CreatedAt: now, StateBuckets: runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets()}, now)
				if err != nil {
					t.Fatal(err)
				}
				anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{Route: scope.Route, FlowID: ".", EntityID: runID, Source: eventtest.RootRoutingSource(runID), Stage: activation.Stage, StageActivationID: activation.ActivationID})
				if err != nil {
					t.Fatal(err)
				}
				card, err := decisioncard.New(decisioncard.Card{CardID: activation.CardID, RunID: runID, ExecutionMode: "live", Anchor: anchor, Snapshot: freezeDecisionCardTestSnapshot(t, activation.DecisionID, nil, routes), BundleHash: authorActivityTestBundleHash, WorkflowVersion: "1", CreatedAt: now})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.CreateDecisionCard(ctx, card); err != nil {
					t.Fatal(err)
				}
				eventID, decidedAt := uuid.NewString(), now.Add(time.Second)
				if err := coordinator.CommitDecision(ctx, card, eventID, decidedAt); err != nil {
					t.Fatal(err)
				}
				if _, err := store.DecideDecisionCard(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", Fields: admitDecisionCardTestObject(t, map[string]any{}), ActorTokenID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: eventID, Now: decidedAt}); err != nil {
					t.Fatal(err)
				}
				evt := eventtest.RuntimeControl(eventID, "mailbox.card_decided", "platform", "", []byte(`{"card_id":"`+card.CardID+`"}`), 0, runID, "", events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, runID), runID), decidedAt)
				if err := commitSemanticEventFixture(ctx, selected, evt); err != nil {
					t.Fatal(err)
				}
				store.calls = 0
				pass, emitted, outcome, err := coordinator.Intercept(ctx, evt)
				if pass || len(emitted) != 0 || !outcome.Committed || (fail && !errors.Is(err, injected)) || (!fail && err != nil) {
					t.Fatalf("pass=%t emitted=%d outcome=%+v err=%v", pass, len(emitted), outcome, err)
				}
				if store.calls != 1 {
					t.Fatalf("route mutations=%d", store.calls)
				}
				routed := loadDecisionCardGateActivation(t, db, backend.name == "postgres", runID, runID)
				if routed.Status != gateruntime.StatusRouted || routed.DecisionEventID != eventID {
					t.Fatalf("route=%+v", routed)
				}
				if _, _, _, err := coordinator.Intercept(ctx, evt); err != nil || store.calls != 1 {
					t.Fatalf("exact routed retry reissued mutation: calls=%d err=%v", store.calls, err)
				}
				query := `SELECT COUNT(*) FROM events WHERE run_id=? AND event_name='test.node_emitted'`
				if backend.name == "postgres" {
					query = `SELECT COUNT(*) FROM events WHERE run_id=$1::uuid AND event_name='test.node_emitted'`
				}
				var count int
				if err := db.QueryRowContext(ctx, query, runID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("emission rows=%d err=%v", count, err)
				}
			})
		}
	}
}
