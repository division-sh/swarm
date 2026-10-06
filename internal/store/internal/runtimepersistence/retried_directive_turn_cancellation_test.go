package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

func TestLogicalTurnDirectiveRetryKeepsExactExecutionOwnerBothStores(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		store := requireProviderDirectiveStore(t, fixture)
		origin, _, event := admitProviderDirectiveOrigin(t, fixture, store, "directive-retry-cancel")
		deliveries := providerDirectiveDeliveryCount(t, fixture)
		ctx := providerDirectiveContext(t, fixture, origin, event, "directive-retry-cancel")
		ctx = effects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
		ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "claude_cli")
		first, err := beginManagedCompletionForTest(t, ctx, "claude_cli", []byte("directive-retry-cancel"))
		if err != nil {
			t.Fatal(err)
		}
		failure := retryableTurnPrelaunchFailure()
		if err := first.Settle(ctx, effects.StateTerminalFailure, &failure, map[string]any{"launch_rejected": true}); err != nil {
			t.Fatal(err)
		}
		history := snapshotForkHistoricalExecutionTables(t, fixture.db, !fixture.sqlite)
		firstPhysical := historicalEvidenceRow(t, history, "runtime_external_effect_attempts", "attempt_id", first.Attempt().AttemptID)
		foreign := origin
		foreign.ExecutionOwnerID = uuid.NewString()
		foreignCtx := effects.WithDirectiveCompletionOrigin(ctx, foreign)
		if _, err := beginManagedCompletionForTest(t, foreignCtx, "claude_cli", []byte("directive-retry-cancel")); err == nil {
			t.Fatal("physical retry transferred directive execution ownership")
		}
		requireProviderAttemptCount(t, fixture, 1)
		retry, err := beginManagedCompletionForTest(t, ctx, "claude_cli", []byte("directive-retry-cancel"))
		if err != nil {
			t.Fatal(err)
		}
		launch, err := store.MarkExternalAttemptLaunched(ctx, retry.Attempt(), time.Now().UTC().Truncate(time.Microsecond))
		if err != nil || launch.Turn == nil || launch.Turn.FirstAttempt != retry.Attempt().AttemptID || !launch.Turn.Origin.Same(first.Attempt().Origin) {
			t.Fatalf("same-owner directive retry launch: %+v %v", launch, err)
		}
		intent, err := fixture.store.(effects.TurnLifetimeStore).RequestTurnTimeout(ctx, retry.Attempt(), launch.Turn.DeadlineAt)
		if err != nil || !intent.Requested || !intent.Committed {
			t.Fatalf("same-owner directive timeout: %+v %v", intent, err)
		}
		if err := retry.Settle(ctx, effects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
			t.Fatal(err)
		}
		turns, err := fixture.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
		if err != nil || len(turns) != 1 || !turns[0].Attempt.Origin.Same(retry.Attempt().Origin) || turns[0].Clock == nil || !turns[0].Clock.DeadlineAt.Equal(launch.Turn.DeadlineAt) {
			t.Fatalf("directive retry recovery changed authority/clock: %+v %v", turns, err)
		}
		command := effects.CanceledTurnCommandForAttempt(turns[0].Attempt, prepareCanceledReactionForTest(t, ctx, fixture, *turns[0].Clock, intent.RequestedAt))
		defer func() {
			plan := command.Publication.(bus.EnginePublicationPlan)
			if err := fixture.store.(storeTestDurableEventBusStore).PipelineObligations().Release(context.WithoutCancel(ctx), plan.PublicationCommand().Commit.PipelineClaim); err != nil {
				t.Error(err)
			}
		}()
		result, err := fixture.store.(effects.CanceledTurnStore).CommitCanceledTurn(ctx, command)
		if err != nil || result.Validate() != nil || result.Directive.State != agentcontrol.DirectiveOperationCanceled || result.Directive.ExecutionOwnerID != origin.ExecutionOwnerID {
			t.Fatalf("directive retry cancellation changed owner: %+v %v", result, err)
		}
		again, err := fixture.store.(effects.CanceledTurnStore).CommitCanceledTurn(ctx, command)
		if err != nil || again.Validate() != nil || !again.Directive.CompletedAt.Equal(result.Directive.CompletedAt) {
			t.Fatalf("directive repeat changed cancellation: %+v %v", again, err)
		}
		assertRetriedTurnHistory(t, fixture, firstPhysical, first.Attempt().AttemptID, retry.Attempt().AttemptID, retry.Attempt().AttemptID)
		requireProviderAttemptCount(t, fixture, 2)
		requireProviderDirectiveDeliveryCount(t, fixture, deliveries)
		assertCanceledReactionCount(t, ctx, fixture, launch.Turn.TimeoutEvent, 1)
	})
}
