package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
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
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend.name, false, map[string]string{
					"schema.yaml": "name: gate-consumer\nstages:\n  awaiting_review:\n    gate:\n      decision: root_review\n      outcomes:\n        approve:\n          advances_to: done\n          emit: test.node_emitted\n  done: {final: true}\n",
					"events.yaml": "test.node_emitted:\n",
				}, nil)
				selected, db := f.store, f.db
				runID := runtimecorrelation.RunIDFromContext(f.ctx)
				ctx := f.ctx
				ctx, bindErr := eventreceiver.NormalExecution().Bind(ctx, executionmode.Live)
				if bindErr != nil {
					t.Fatal(bindErr)
				}
				registerTestAuthorActivityCatalogForContext(t, selected.(testAuthorActivityCatalogRegistrar), ctx)
				store := &gateConsumerOutcomeStore{workflowTestSelectedStore: selected.(workflowTestSelectedStore)}
				injected := errors.New("injected gate cleanup after selected COMMIT")
				if fail {
					store.injected = injected
				}
				bundle := f.bundle
				source := semanticview.Wrap(bundle)
				fact := mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash())
				bus := f.newRuntimeEventBus(t, runtimebus.EventBusOptions{})
				opts := completeWorkflowTestCoordinatorOptions(runtimepipeline.NewWorkflowPersistence(store), store)
				opts.Module = runForkGateWorkflowModule{source: source}
				opts.SourceArtifactFact = fact
				coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, opts)
				if coordinator == nil {
					t.Fatal("construct gate coordinator")
				}
				now := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				identity := runtimeflowidentity.Instance{TemplateID: ".", ScopeKey: ".", InstanceID: runID, InstancePath: runID, EntityID: runID, HasStoredPath: true}
				plan, err := f.manager.PrepareFlowInstanceActivation(ctx, runtimepipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: identity, OccurredAt: now})
				if err != nil {
					t.Fatal(err)
				}
				committed, err := (agentFixtureFlowActivationCommitter{store: selected}).CommitFlowInstanceActivation(ctx, plan)
				if err != nil || !committed.Acknowledged || !committed.Created {
					t.Fatalf("canonical gate construction: %+v %v", committed, err)
				}
				scope := runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()}
				loadActivation := func() gateruntime.Activation {
					instance, found, err := coordinator.Load(ctx, scope)
					if err != nil || !found {
						t.Fatalf("load constructed gate: found=%t err=%v", found, err)
					}
					carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
					if err != nil {
						t.Fatal(err)
					}
					activations, err := gateruntime.List(carrier.StateBuckets)
					if err != nil || len(activations) != 1 {
						t.Fatalf("constructed gate activations: %+v %v", activations, err)
					}
					return activations[0]
				}
				activation := loadActivation()
				card, err := store.GetDecisionCard(ctx, activation.CardID)
				if err != nil {
					t.Fatal(err)
				}
				eventID, decidedAt := uuid.NewString(), now.Add(time.Second)
				if err := coordinator.CommitDecision(ctx, card, eventID, decidedAt); err != nil {
					t.Fatal(err)
				}
				if _, err := DecisionCardDomainForTest(selected).ApplyDecisionForTest(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", Fields: admitDecisionCardTestObject(t, map[string]any{}), PrincipalID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: eventID, Now: decidedAt}); err != nil {
					t.Fatal(err)
				}
				evt := eventtest.RuntimeControl(eventID, "mailbox.card_decided", "platform", "", []byte(`{"card_id":"`+card.CardID+`"}`), 0, runID, "", events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, runID), identity.InstancePath), decidedAt)
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
				routed := loadActivation()
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
