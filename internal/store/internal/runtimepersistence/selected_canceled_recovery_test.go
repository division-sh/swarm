package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedCanceledOriginRecoveryRetainsPendingIntentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			f := newSelectedProviderCompletionFixture(t, selected, db, sqlite)
			ctx := testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash)
			issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, f.request)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "selected-canceled-recovery", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			admitSelectedProviderFixture(t, ctx, f, issued, authority)
			authority.Target = selectedProviderTarget(f)
			ctx = effects.WithAuthority(effects.WithController(ctx, newCompletionControllerForTest(selected)), authority)
			ctx = selectedProviderClaimContext(t, ctx, f, authority)
			ctx = withManagedCompletionTestSurface(t, managedSelectedExecutionStoreTestContext(t, ctx, authority), authority, "anthropic_api")
			ctx = effects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.grant_receiver"})
			handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "selected-canceled-recovery")
			clock, found := handle.LogicalTurnClock()
			if !found {
				t.Fatal("selected launch lacks its exact clock")
			}
			intent, err := selected.(effects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
			if err != nil || intent.ValidateIntent() != nil {
				t.Fatalf("record exact timeout: %+v %v", intent, err)
			}
			if err := f.process.Release(ctx); err != nil {
				t.Fatal(err)
			}
			process, err := selectedPreparationProcessForTest(t, selected).Evidence()
			if err != nil {
				t.Fatal(err)
			}
			recovery := selected.(interface {
				ListSelectedForkRecoveryEntries(context.Context) ([]runfork.SelectedForkRecoveryEntry, error)
				RecoverSelectedFork(context.Context, runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error)
			})
			entries, err := recovery.ListSelectedForkRecoveryEntries(ctx)
			if err != nil || len(entries) != 1 {
				t.Fatalf("selected recovery inventory: %+v %v", entries, err)
			}
			request := runcontrol.SelectedForkRecoveryRequest{Entry: entries[0], Process: process, Effects: liveExternalEffectRecoveryRequest(time.Now().UTC())}
			result, err := recovery.RecoverSelectedFork(ctx, request)
			if err != nil || result.Effects.OutcomeUncertain != 1 {
				t.Fatalf("recover selected physical tail: %+v %v", result, err)
			}
			snapshot, err := selected.(deliverylifecycle.Store).Snapshot(ctx, handle.Attempt().Origin.Delivery.DeliveryID())
			if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.ClaimVersion != handle.Attempt().Origin.Delivery.Version() {
				t.Fatalf("recovery overwrote unresolved authored cancellation: %+v %v", snapshot, err)
			}
			if len(result.PendingCancellations) != 1 || result.PendingCancellations[0].Cancellation.ValidateIntent() != nil ||
				!result.PendingCancellations[0].Attempt.Origin.Same(handle.Attempt().Origin) || result.PendingCancellations[0].Clock == nil ||
				!reflect.DeepEqual(*result.PendingCancellations[0].Clock, clock) {
				t.Fatalf("selected recovery omitted exact cancellation/clock handoff: %+v", result)
			}
			normal, err := selected.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, request.Effects)
			if err != nil || len(normal) != 0 {
				t.Fatalf("normal recovery acquired selected cancellation: %+v %v", normal, err)
			}
			repeated, err := recovery.RecoverSelectedFork(ctx, request)
			if err != nil || repeated.Effects != (effects.RecoverySummary{}) || !reflect.DeepEqual(repeated.PendingCancellations, result.PendingCancellations) {
				t.Fatalf("selected acknowledgment retry changed pending evidence: %+v %v", repeated, err)
			}
		})
	}
}
