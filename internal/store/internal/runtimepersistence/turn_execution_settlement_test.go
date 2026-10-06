package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

func TestLogicalTurnExecutionCancelsAndSettlesRealDeliveryOriginBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		store := selected.selected.(completionSettlementTestStore)
		if selected.postgres {
			store = admitTestPostgresStore(t, selected.db)
		}
		fixture := newCompletionSettlementFixture(t, store, selected.db, !selected.postgres)
		authority := fixture.authority
		authority.BudgetScopes = nil
		parent := runtimeeffects.WithTurnTimeout(fixture.contextFor(authority), &timeridentity.TurnTimeout{After: time.Nanosecond, Emit: "investigation.aborted"})
		parent = withManagedCompletionTestSurface(t, parent, authority, "claude_cli")
		ctx, owner := runtimeeffects.WithTurnExecution(parent)
		defer func() { _, _ = owner.Finish() }()
		handle := beginLogicalClockCompletion(t, ctx, "owned-delivery-timeout")
		if err := handle.MarkLaunched(ctx); err != nil {
			t.Fatal(err)
		}
		requireAuthoredTurnTimeout(t, ctx)
		joined, err := owner.Finish()
		if err != nil || joined.Cancellation.ValidateIntent() != nil || !joined.Cancellation.Origin.Same(handle.Attempt().Origin) {
			t.Fatalf("owned cancellation join: %+v err=%v", joined, err)
		}
		canceled := store.(runtimeeffects.CanceledDeliveryTurnStore)
		if result, err := canceled.SettleCanceledDeliveryTurn(parent, joined.Attempt); err == nil || result.Acknowledged {
			t.Fatalf("live physical tail was skipped: %+v err=%v", result, err)
		}
		failure := runtimefailures.FromError(context.Canceled, "provider-test", "physical_join").Failure
		if err := handle.Settle(parent, runtimeeffects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
			t.Fatal(err)
		}
		result, err := canceled.SettleCanceledDeliveryTurn(parent, joined.Attempt)
		if err != nil || !result.Acknowledged || result.Snapshot.Status != deliverylifecycle.StatusCanceled || result.Snapshot.ReasonCode != "turn_timeout" {
			t.Fatalf("real canceled delivery receipt: %+v err=%v", result, err)
		}
	})
}

func TestLogicalTurnExecutionCancelsRealDirectiveWithoutDeliveryBothStores(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		store := requireProviderDirectiveStore(t, fixture)
		origin, _, event := admitProviderDirectiveOrigin(t, fixture, store, "owned-directive-timeout")
		before := providerDirectiveDeliveryCount(t, fixture)
		parent := providerDirectiveContext(t, fixture, origin, event, "owned-directive-timeout")
		parent = runtimeeffects.WithTurnTimeout(parent, &timeridentity.TurnTimeout{After: time.Nanosecond, Emit: "investigation.aborted"})
		ctx, owner := runtimeeffects.WithTurnExecution(parent)
		defer func() { _, _ = owner.Finish() }()
		handle, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("owned-directive-timeout"))
		if err != nil {
			t.Fatal(err)
		}
		if err := handle.MarkLaunched(ctx); err != nil {
			t.Fatal(err)
		}
		requireAuthoredTurnTimeout(t, ctx)
		joined, err := owner.Finish()
		if err != nil || !joined.Cancellation.Origin.Same(handle.Attempt().Origin) {
			t.Fatalf("directive cancellation join: %+v err=%v", joined, err)
		}
		canceled := store.(runtimeeffects.CanceledDirectiveTurnStore)
		if result, err := store.RecordDirectiveExecuted(parent, origin.OperationID, origin.ExecutionOwnerID, []byte(`{"reply":"too late"}`), time.Now().UTC()); err == nil || result.Acknowledged {
			t.Fatalf("ordinary directive response bypassed canceled intent: %+v err=%v", result, err)
		}
		if result, err := canceled.SettleCanceledDirectiveTurn(parent, joined.Attempt); err == nil || result.Acknowledged {
			t.Fatalf("directive skipped live physical tail: %+v err=%v", result, err)
		}
		failure := runtimefailures.FromError(context.Canceled, "provider-test", "physical_join").Failure
		if result, err := store.FinalizeDirectiveFailure(parent, origin.OperationID, origin.ExecutionOwnerID, failure, time.Now().UTC(), time.Hour); err == nil || result.Acknowledged {
			t.Fatalf("ordinary failure bypassed authored cancellation: %+v err=%v", result, err)
		}
		if err := handle.Settle(parent, runtimeeffects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
			t.Fatal(err)
		}
		result, err := canceled.SettleCanceledDirectiveTurn(parent, joined.Attempt)
		if err != nil || !result.Acknowledged || result.State != agentcontrol.DirectiveOperationCanceled || result.CancellationReason != deliverylifecycle.CancellationTurnTimeout || result.Failure != nil || len(result.Response) != 0 {
			t.Fatalf("real canceled directive receipt: %+v err=%v", result, err)
		}
		if err := agentcontrol.ValidateDirectiveOperationEvidence(result); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(agentcontrol.ErrorForDirectiveOperation(result), agentcontrol.ErrDirectiveCanceled) {
			t.Fatal("canceled directive was reinterpreted as a generic failure")
		}
		repeat, err := canceled.SettleCanceledDirectiveTurn(parent, joined.Attempt)
		if err != nil || !repeat.Acknowledged || !repeat.CompletedAt.Equal(result.CompletedAt) {
			t.Fatalf("canceled directive retry changed evidence: %+v err=%v", repeat, err)
		}
		requireProviderDirectiveDeliveryCount(t, fixture, before)
	})
}

func requireAuthoredTurnTimeout(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("acknowledged provider launch did not drive owned timeout")
	}
	var cause *runtimeeffects.AuthoredTurnCancellationError
	if !errors.As(context.Cause(ctx), &cause) || cause.Cancellation.Reason != deliverylifecycle.CancellationTurnTimeout {
		t.Fatalf("cancellation did not come from exact authored intent: %v", context.Cause(ctx))
	}
}

func TestCanceledTurnPreservesCapturedProviderDrainBothStores(t *testing.T) {
	for _, kind := range []string{"delivery", "directive"} {
		t.Run(kind, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				ctx := providerDrainContext(t, fixture, "canceled-drain")
				var directiveEvent events.Event
				if kind == "directive" {
					store := requireProviderDirectiveStore(t, fixture)
					origin, _, event := admitProviderDirectiveOrigin(t, fixture, store, "canceled-drain")
					ctx = providerDirectiveContext(t, fixture, origin, event, "canceled-drain")
					directiveEvent = event
				}
				ctx = runtimeeffects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "investigation.aborted"})
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "canceled-drain")
				clock, found := handle.LogicalTurnClock()
				if !found {
					t.Fatal("launched origin lost its clock")
				}
				intent, err := fixture.store.(runtimeeffects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
				if err != nil || !intent.Requested {
					t.Fatalf("timeout: %+v err=%v", intent, err)
				}
				transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
				if transition.ProviderDrainCount != 1 {
					t.Fatalf("accepted provider tail was not retained: %+v", transition)
				}
				settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "", "")
				if kind == "directive" {
					settlement = completionDirectiveSettlementForTest(t, handle.Attempt().Authority.Target, fixture, directiveEvent, "anthropic_api", "", "")
				}
				settlement.ProviderHead = nil
				failure := runtimefailures.FromError(context.Canceled, "provider-test", "physical_join").Failure
				settlement.Settlement = runtimeeffects.Settlement{State: runtimeeffects.StateOutcomeUncertain, Failure: &failure, Evidence: map[string]any{"physical_joined": true}}
				settlement.AgentTurn.Failure = &failure
				result, err := handle.SettleCompletion(ctx, settlement)
				if err != nil || !result.Committed || !result.OriginSettled {
					t.Fatalf("authored cancellation blocked accepted captured-tail settlement: %+v err=%v", result, err)
				}
				requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
				requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateOutcomeUncertain, 1, 0)
				if kind == "delivery" {
					snapshot, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, fixture.origin.DeliveryID())
					if err != nil || snapshot.Status != deliverylifecycle.StatusCanceled || snapshot.ReasonCode != "turn_timeout" {
						t.Fatalf("captured tail lost canceled delivery: %+v err=%v", snapshot, err)
					}
				} else {
					op, found, err := requireProviderDirectiveStore(t, fixture).LoadDirectiveOperation(ctx, handle.Attempt().Origin.Directive.OperationID)
					if err != nil || !found || op.State != agentcontrol.DirectiveOperationCanceled {
						t.Fatalf("captured tail lost canceled directive: %+v found=%t err=%v", op, found, err)
					}
				}
			})
		})
	}
}

func TestCanceledOriginWaitsForEveryCapturedProviderTailBothStores(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		ctx := runtimeeffects.WithTurnTimeout(providerDrainContext(t, fixture, "canceled-two-tails"), &timeridentity.TurnTimeout{After: time.Minute, Emit: "investigation.aborted"})
		first := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "canceled-two-tails-first")
		authority := fixture.authority
		authority.Target.ID = uuid.NewString()
		authority.BudgetScopes = nil
		secondCtx := runtimeeffects.WithTurnTimeout(fixture.contextFor(authority), &timeridentity.TurnTimeout{After: time.Minute, Emit: "investigation.aborted"})
		secondCtx = runtimeeffects.WithLogicalOperationIdentity(secondCtx, "canceled-two-tails-second")
		second := beginObservedCompletionForSettlementTest(t, secondCtx, "anthropic_api", "canceled-two-tails-second")
		clock, _ := first.LogicalTurnClock()
		intent, err := fixture.store.(runtimeeffects.TurnLifetimeStore).RequestTurnTimeout(ctx, first.Attempt(), clock.DeadlineAt)
		if err != nil || !intent.Requested {
			t.Fatalf("timeout: %+v err=%v", intent, err)
		}
		transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
		if transition.ProviderDrainCount != 2 {
			t.Fatalf("two accepted tails were not retained: %+v", transition)
		}
		for index, handle := range []*runtimeeffects.Handle{first, second} {
			settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "", "")
			settlement.ProviderHead = nil
			failure := runtimefailures.FromError(context.Canceled, "provider-test", "physical_join").Failure
			settlement.Settlement = runtimeeffects.Settlement{State: runtimeeffects.StateOutcomeUncertain, Failure: &failure, Evidence: map[string]any{"physical_joined": true}}
			settlement.AgentTurn.Failure = &failure
			result, err := handle.SettleCompletion(ctx, settlement)
			if err != nil || !result.Committed || result.Cancellation == nil || result.Cancellation.ValidateIntent() != nil || result.OriginSettled != (index == 1) {
				t.Fatalf("tail %d origin ownership: %+v err=%v", index, result, err)
			}
			if index == 0 && result.Finalization != nil {
				t.Fatal("first physical tail prematurely finalized the captured set")
			}
			snapshot, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, fixture.origin.DeliveryID())
			want := deliverylifecycle.StatusInProgress
			if index == 1 {
				want = deliverylifecycle.StatusCanceled
			}
			if err != nil || snapshot.Status != want {
				t.Fatalf("tail %d snapshot: %+v err=%v", index, snapshot, err)
			}
			requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
			requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateOutcomeUncertain, 1, 0)
		}
	})
}

func TestCanceledCapturedTurnRecoveryNeverReadmitsWorkBothStores(t *testing.T) {
	for _, kind := range []string{"delivery", "directive"} {
		t.Run(kind, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				ctx := providerDrainContext(t, fixture, "canceled-recovery")
				if kind == "directive" {
					store := requireProviderDirectiveStore(t, fixture)
					origin, _, event := admitProviderDirectiveOrigin(t, fixture, store, "canceled-recovery")
					ctx = providerDirectiveContext(t, fixture, origin, event, "canceled-recovery")
				}
				before := providerDirectiveDeliveryCount(t, fixture)
				ctx = runtimeeffects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "investigation.aborted"})
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "canceled-recovery")
				clock, _ := handle.LogicalTurnClock()
				intent, err := fixture.store.(runtimeeffects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
				if err != nil || !intent.Requested {
					t.Fatalf("timeout: %+v err=%v", intent, err)
				}
				transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
				if transition.ProviderDrainCount != 1 {
					t.Fatalf("captured tail missing: %+v", transition)
				}
				summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(time.Now().UTC()))
				if err != nil || summary.OutcomeUncertain != 1 {
					t.Fatalf("canceled physical recovery: %+v err=%v", summary, err)
				}
				requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
				requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
				if kind == "delivery" {
					snapshot, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, fixture.origin.DeliveryID())
					if err != nil || snapshot.Status != deliverylifecycle.StatusCanceled {
						t.Fatalf("recovered canceled delivery: %+v err=%v", snapshot, err)
					}
				} else {
					op, found, err := requireProviderDirectiveStore(t, fixture).LoadDirectiveOperation(ctx, handle.Attempt().Origin.Directive.OperationID)
					if err != nil || !found || op.State != agentcontrol.DirectiveOperationCanceled {
						t.Fatalf("recovered canceled directive: %+v found=%t err=%v", op, found, err)
					}
				}
				again, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(time.Now().UTC()))
				if err != nil || again != (runtimeeffects.RecoverySummary{}) {
					t.Fatalf("repeated recovery admitted work: %+v err=%v", again, err)
				}
				requireProviderDirectiveDeliveryCount(t, fixture, before)
			})
		})
	}
}
