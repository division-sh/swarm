package runtimepersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestSelectedProviderTurnRetainsActualBusinessOriginBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, selected := newAgentFixtureAuthorityStore(t, backend)
			source := grantReceiverEntitySource(t)
			bundle, _ := semanticview.Bundle(source)
			actor, grant := selectedReceiverClaimGrant(t, ctx, selected, source)
			evidence, err := grant.Evidence()
			if err != nil || evidence.SelectedFork == nil {
				t.Fatalf("actual selected grant: %+v err=%v", evidence, err)
			}
			fork := evidence.SelectedFork
			fact := mustStoreTestSourceArtifactFact(evidence.BundleHash)
			ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, fact), actor.RunID)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.grant_receiver", "operator", "", []byte(`{}`), 0, actor.RunID, events.EventEnvelope{}, time.Now().UTC())
			req := sqliteFlowActivationRequest(bundle, "global", "global", "", actor.FlowInstance())
			root := flowidentity.Stored(source, ".", actor.RunID, actor.RunID, actor.RunID, "")
			req.Instance, err = flowidentity.KeylessChild(source, root, "global")
			if err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor,
				Target: events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "global", FlowInstance: actor.FlowInstance(), EntityID: req.Instance.EntityID})}
			event = eventtest.TargetRouted(event, route.Target.Route())
			req.TriggerEvent, req.OccurredAt = event, event.CreatedAt()
			constructHistoricalSourceFixture(t, ctx, selected, req)
			route.Initialization, err = events.AdmitFlowReceiverInitialization(event, route.Target)
			if err != nil {
				t.Fatal(err)
			}
			deliveryAuthority, err := deliverylifecycle.NewSelectedExecutionAuthority(fact, fork.ExecutionID, actor.RunID, fork.ExecutionGeneration)
			if err != nil {
				t.Fatal(err)
			}
			commitSelectedReceiverClaimEvent(t, ctx, selected, event, route, deliveryAuthority)
			claimResult, err := selected.ClaimDelivery(ctx, deliveryAuthority, event, route)
			claimed, acquired := claimResult.Acquired()
			if err != nil || !acquired {
				t.Fatalf("actual selected receiver claim: %+v err=%v", claimResult, err)
			}
			authority := effects.Authority{
				Kind: effects.AuthoritySelectedContractFork, ID: fork.ExecutionID, ExecutionMode: executionmode.Live,
				ExecutionOwner: fork.ExecutionOwner, FenceGeneration: fork.FenceGeneration, LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
				SelectedFork: effects.SelectedContractForkAuthority{ExecutionID: fork.ExecutionID, ForkRunID: actor.RunID, Generation: fork.ExecutionGeneration,
					AdmissionFingerprint: fork.AdmissionFingerprint, ContainerPlanFingerprint: fork.ContainerPlanFingerprint,
					ActorCensusFingerprint: fork.ActorCensusFingerprint, EffectiveConfigFingerprint: fork.EffectiveConfigFingerprint},
				Target: effects.UsageTarget{Kind: effects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: actor.RunID, AgentID: actor.AgentID(),
					AgentIdentity: actor, FlowInstance: actor.FlowInstance(), EntityID: req.Instance.EntityID, SessionID: uuid.NewString()},
			}
			store := selected.(completionSettlementTestStore)
			ctx = effects.WithController(effects.WithAuthority(ctx, authority), newCompletionControllerForTest(store))
			admission, err := managedexecution.New(managedexecution.KindSelectedContractFork, fork.ExecutionID, fork.ExecutionGeneration, actor.RunID, fork.ActorCensusFingerprint, evidence.BundleHash, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx = withManagedCompletionTestSurface(t, ctx, authority, "anthropic_api")
			ctx = managedexecution.WithAdmission(ctx, admission)
			ctx = effects.WithTurnTimeout(deliverylifecycle.WithClaim(correlation.WithInboundEvent(ctx, event), claimed.Claim), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.grant_receiver"})
			ctx, observe := effects.WithCompletionSettlementObserver(ctx)
			frame := managedCompletionTestFrameForSource(t, authority, "anthropic_api", event, evidence.BundleHash)
			handle, err := effects.BeginManagedCompletion(ctx, "anthropic_api", []byte("selected-real-origin"), frame, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !handle.Attempt().Origin.Delivery.Same(claimed.Claim) {
				t.Fatal("selected provider authorization omitted its exact business claim")
			}
			if err := handle.MarkLaunched(ctx); err != nil {
				t.Fatal(err)
			}
			clock, found := handle.LogicalTurnClock()
			if !found || !clock.Origin.Delivery.Same(claimed.Claim) {
				t.Fatal("selected provider launch omitted its exact logical turn clock")
			}
			before, err := selected.Snapshot(ctx, claimed.Claim.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			heartbeatAt := time.Now().UTC()
			if err := store.HeartbeatCompletionAttempt(ctx, handle.Attempt(), heartbeatAt, before.ClaimExpiresAt.Sub(heartbeatAt)+time.Minute); err != nil {
				t.Fatal(err)
			}
			after, err := selected.Snapshot(ctx, claimed.Claim.DeliveryID())
			if err != nil || !after.ClaimExpiresAt.After(before.ClaimExpiresAt) {
				t.Fatalf("selected physical heartbeat omitted its exact origin lease: before=%+v after=%+v err=%v", before, after, err)
			}
			intent, err := store.(effects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
			if err != nil || intent.ValidateIntent() != nil || !intent.Origin.Delivery.Same(claimed.Claim) || intent.Reason != deliverylifecycle.CancellationTurnTimeout {
				t.Fatalf("selected timeout lost actual turn ownership: %+v err=%v", intent, err)
			}
			failure := failures.FromError(&effects.AuthoredTurnCancellationError{Cancellation: intent}, "selected-turn-test", "physical_cleanup").Failure
			settlement := completionSettlementForTest(t, authority.Target, completionSettlementFixture{authority: authority, agentID: actor.AgentID(), sessionID: authority.Target.SessionID}, "anthropic_api", "", "")
			settlement.ProviderHead = nil
			settlement.Settlement = effects.Settlement{State: effects.StateOutcomeUncertain, Failure: &failure, Evidence: map[string]any{"physical_joined": true}}
			settlement.AgentTurn.Identity, settlement.AgentTurn.Failure = actor, &failure
			settlement.AgentTurn.TriggerEventID, settlement.AgentTurn.TriggerEventType = event.ID(), string(event.Type())
			physical, err := handle.SettleCompletion(ctx, settlement)
			if err != nil || !physical.Committed {
				t.Fatalf("selected physical completion failed: %+v err=%v", physical, err)
			}
			observed := observe()
			if observed.Cancellation == nil || observed.Cancellation.ValidateIntent() != nil || !observed.Cancellation.Origin.Same(intent.Origin) {
				t.Fatalf("selected physical settlement dropped authored intent: %+v", observed)
			}
			// This component has no publication owner. A timeout must not settle
			// its business origin without committing the required reaction.
			if committed, err := store.(effects.CanceledTurnStore).CommitCanceledTurn(ctx, effects.CanceledTurnCommandForAttempt(handle.Attempt(), nil)); err == nil || committed.Acknowledged {
				t.Fatalf("selected timeout omitted its atomic reaction: %+v err=%v", committed, err)
			}
		})
	}
}
