package runtimepersistence

import (
	"context"
	"encoding/json"
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
		fixture := newCompletionSettlementFixture(t, store, selected.db, !selected.postgres)
		authority := fixture.authority
		authority.BudgetScopes = nil
		parent := runtimeeffects.WithTurnTimeout(fixture.contextFor(authority), &timeridentity.TurnTimeout{After: time.Nanosecond, Emit: "test.node_emitted"})
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
		canceled := store.(runtimeeffects.CanceledTurnStore)
		command := runtimeeffects.CanceledTurnCommandForAttempt(joined.Attempt, prepareCanceledReactionForTest(t, parent, fixture, *joined.Clock, joined.Cancellation.RequestedAt))
		if result, err := canceled.CommitCanceledTurn(parent, command); err == nil || result.Acknowledged {
			t.Fatalf("live physical tail was skipped: %+v err=%v", result, err)
		}
		failure := runtimefailures.FromError(context.Canceled, "provider-test", "physical_join").Failure
		if err := handle.Settle(parent, runtimeeffects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
			t.Fatal(err)
		}
		result, err := canceled.CommitCanceledTurn(parent, command)
		if err != nil || !result.Acknowledged || result.Delivery.Status != deliverylifecycle.StatusCanceled || result.Delivery.ReasonCode != "turn_timeout" {
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
		parent = runtimeeffects.WithTurnTimeout(parent, &timeridentity.TurnTimeout{After: time.Nanosecond, Emit: "test.node_emitted"})
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
		canceled := store.(runtimeeffects.CanceledTurnStore)
		command := runtimeeffects.CanceledTurnCommandForAttempt(joined.Attempt, prepareCanceledReactionForTest(t, parent, fixture, *joined.Clock, joined.Cancellation.RequestedAt))
		if result, err := store.RecordDirectiveExecuted(parent, origin.OperationID, origin.ExecutionOwnerID, []byte(`{"reply":"too late"}`), time.Now().UTC()); err == nil || result.Acknowledged {
			t.Fatalf("ordinary directive response bypassed canceled intent: %+v err=%v", result, err)
		}
		if result, err := canceled.CommitCanceledTurn(parent, command); err == nil || result.Acknowledged {
			t.Fatalf("directive skipped live physical tail: %+v err=%v", result, err)
		}
		failure := runtimefailures.FromError(context.Canceled, "provider-test", "physical_join").Failure
		if result, err := store.FinalizeDirectiveFailure(parent, origin.OperationID, origin.ExecutionOwnerID, failure, time.Now().UTC(), time.Hour); err == nil || result.Acknowledged {
			t.Fatalf("ordinary failure bypassed authored cancellation: %+v err=%v", result, err)
		}
		if err := handle.Settle(parent, runtimeeffects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
			t.Fatal(err)
		}
		commit, err := canceled.CommitCanceledTurn(parent, command)
		result := commit.Directive
		if err != nil || !result.Acknowledged || result.State != agentcontrol.DirectiveOperationCanceled || result.CancellationReason != deliverylifecycle.CancellationTurnTimeout || result.Failure != nil || len(result.Response) != 0 {
			t.Fatalf("real canceled directive receipt: %+v err=%v", result, err)
		}
		if err := agentcontrol.ValidateDirectiveOperationEvidence(result); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(agentcontrol.ErrorForDirectiveOperation(result), agentcontrol.ErrDirectiveCanceled) {
			t.Fatal("canceled directive was reinterpreted as a generic failure")
		}
		repeated, err := canceled.CommitCanceledTurn(parent, command)
		repeat := repeated.Directive
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
				ctx = runtimeeffects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
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
				if err != nil || !result.Committed || result.OriginSettled {
					t.Fatalf("authored cancellation blocked accepted captured-tail settlement: %+v err=%v", result, err)
				}
				assertCanceledReactionCount(t, ctx, fixture, clock.TimeoutEvent, 0)
				command := runtimeeffects.CanceledTurnCommandForAttempt(handle.Attempt(), prepareCanceledReactionForTest(t, ctx, fixture, clock, intent.RequestedAt))
				commit, err := fixture.store.(runtimeeffects.CanceledTurnStore).CommitCanceledTurn(ctx, command)
				if err != nil || !commit.Acknowledged || commit.Validate() != nil {
					t.Fatalf("captured origin and reaction did not settle atomically: %+v err=%v", commit, err)
				}
				assertCanceledReactionCount(t, ctx, fixture, clock.TimeoutEvent, 1)
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
		ctx := runtimeeffects.WithTurnTimeout(providerDrainContext(t, fixture, "canceled-two-tails"), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
		first := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "canceled-two-tails-first")
		authority := fixture.authority
		authority.Target.ID = uuid.NewString()
		authority.BudgetScopes = nil
		secondCtx := runtimeeffects.WithTurnTimeout(fixture.contextFor(authority), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
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
			if err != nil || !result.Committed || result.Cancellation == nil || result.Cancellation.ValidateIntent() != nil || result.OriginSettled {
				t.Fatalf("tail %d origin ownership: %+v err=%v", index, result, err)
			}
			if index == 0 && result.Finalization != nil {
				t.Fatal("first physical tail prematurely finalized the captured set")
			}
			snapshot, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, fixture.origin.DeliveryID())
			if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress {
				t.Fatalf("tail %d snapshot: %+v err=%v", index, snapshot, err)
			}
			requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
			requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateOutcomeUncertain, 1, 0)
			pending, err := fixture.store.(runtimeeffects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
			if err != nil || len(pending) != index {
				t.Fatalf("tail %d recovery admitted an incomplete physical set: count=%d err=%v", index, len(pending), err)
			}
		}
		settleRecoveredCanceledTurnForTest(t, fixture, first.Attempt())
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
				ctx = runtimeeffects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
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
				settleRecoveredCanceledTurnForTest(t, fixture, handle.Attempt())
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

func settleRecoveredCanceledTurnForTest(t *testing.T, fixture completionSettlementFixture, original runtimeeffects.Attempt) {
	t.Helper()
	// No previous runtime/controller/handle is carried into this recovery read.
	ctx := testAuthorActivityContext()
	recovery := fixture.store.(runtimeeffects.CanceledTurnRecoveryStore)
	request := liveExternalEffectRecoveryRequest(time.Now().UTC())
	turns, err := recovery.ListCanceledTurnRecoveries(ctx, request)
	if err != nil || len(turns) != 1 {
		t.Fatalf("recover exact canceled origin: count=%d err=%v", len(turns), err)
	}
	turn := turns[0]
	if turn.Attempt.AttemptID != original.AttemptID || turn.Clock.FirstAttempt != original.AttemptID ||
		!turn.Attempt.Origin.Same(original.Origin) || turn.Attempt.Authority.Normal != original.Authority.Normal ||
		turn.Cancellation.ValidateIntent() != nil || turn.Cancellation.OriginSettled {
		t.Fatalf("recovery substituted original execution evidence: %+v", turn)
	}
	assertCanceledReactionCount(t, ctx, fixture, turn.Cancellation.CauseEvent, 0)
	command := runtimeeffects.CanceledTurnCommandForAttempt(turn.Attempt, prepareCanceledReactionForTest(t, ctx, fixture, *turn.Clock, turn.Cancellation.RequestedAt))
	store := fixture.store.(runtimeeffects.CanceledTurnStore)
	installCanceledReactionCut(t, ctx, fixture)
	if result, err := store.CommitCanceledTurn(ctx, command); err == nil || result.Acknowledged {
		t.Fatalf("recovered reaction failure committed partial origin: %+v err=%v", result, err)
	}
	stillPending, err := recovery.ListCanceledTurnRecoveries(ctx, request)
	if err != nil || len(stillPending) != 1 {
		t.Fatalf("reaction rollback lost recovery responsibility: count=%d err=%v", len(stillPending), err)
	}
	removeCanceledReactionCut(t, ctx, fixture)
	for i := 0; i < 2; i++ {
		result, err := store.CommitCanceledTurn(ctx, command)
		if err != nil || !result.Acknowledged || result.Validate() != nil {
			t.Fatalf("recovered atomic settlement/retry %d: %+v err=%v", i, result, err)
		}
	}
	assertCanceledReactionCount(t, ctx, fixture, turn.Cancellation.CauseEvent, 1)
	remaining, err := recovery.ListCanceledTurnRecoveries(ctx, request)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("settled cancellation remained executable: count=%d err=%v", len(remaining), err)
	}
}

func TestCanceledTurnRecoveryRejectsCorruptFirstOriginBothStores(t *testing.T) {
	for _, corruption := range []string{"missing_first_attempt", "foreign_run", "foreign_cause", "before_first_launch"} {
		t.Run(corruption, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				ctx := runtimeeffects.WithTurnTimeout(providerDrainContext(t, fixture, "corrupt-canceled-recovery"), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "corrupt-canceled-recovery")
				clock, _ := handle.LogicalTurnClock()
				intent, err := fixture.store.(runtimeeffects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
				if err != nil || intent.ValidateIntent() != nil {
					t.Fatalf("intent: %+v err=%v", intent, err)
				}
				failure := runtimefailures.FromError(context.Canceled, "provider-test", "physical_join").Failure
				if err := handle.Settle(ctx, runtimeeffects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
					t.Fatal(err)
				}
				column, value := "first_attempt_id", any(uuid.NewString())
				switch corruption {
				case "foreign_run":
					column = "run_id"
				case "foreign_cause":
					column = "cancel_cause_event_id"
				case "before_first_launch":
					column = "cancel_requested_at"
					value = clock.LaunchedAt.Add(-time.Second)
				}
				query := "UPDATE runtime_agent_turn_lifetimes SET " + column + "=$1 WHERE origin_id=$2"
				if _, err := fixture.db.ExecContext(ctx, query, value, handle.Attempt().Origin.Delivery.DeliveryID()); err != nil {
					t.Fatal(err)
				}
				turns, err := fixture.store.(runtimeeffects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
				if err == nil || len(turns) != 0 {
					t.Fatalf("corruption %s returned cancellation authority: count=%d err=%v", corruption, len(turns), err)
				}
				assertCanceledReactionCount(t, ctx, fixture, clock.TimeoutEvent, 0)
				snapshot, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, fixture.origin.DeliveryID())
				if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress {
					t.Fatalf("failed recovery mutated origin: %+v err=%v", snapshot, err)
				}
			})
		})
	}
}

func TestCompletionReportsCanceledOriginWithoutDroppingAcceptedResponseBothStores(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		parent := runtimeeffects.WithTurnTimeout(providerDrainContext(t, fixture, "late-canceled-response"), &timeridentity.TurnTimeout{After: time.Hour, Emit: "test.node_emitted"})
		ctx, owner := runtimeeffects.WithTurnExecution(parent)
		defer func() { _, _ = owner.Finish() }()
		handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "late-canceled-response")
		clock, _ := handle.LogicalTurnClock()
		intent, err := fixture.store.(runtimeeffects.TurnLifetimeStore).RequestTurnTimeout(parent, handle.Attempt(), clock.DeadlineAt)
		if err != nil || intent.ValidateIntent() != nil {
			t.Fatalf("intent: %+v err=%v", intent, err)
		}
		settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "", "")
		settlement.ProviderHead = nil
		if err := runtimeeffects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte("late-canceled-response"), json.RawMessage(`{"version":"late-result","response":"kept"}`)); err != nil {
			t.Fatal(err)
		}
		result, err := handle.SettleCompletion(ctx, settlement)
		if err != nil || !result.Committed || result.Cancellation == nil || result.Cancellation.ValidateIntent() != nil || result.OriginSettled {
			t.Fatalf("current completion lost cancellation intent or physical evidence: %+v err=%v", result, err)
		}
		if _, executable := handle.Attempt().CompletionContinuation(); executable {
			t.Fatal("canceled late response became executable continuation")
		}
		var active bool
		if err := fixture.db.QueryRowContext(parent, `SELECT completion_continuation_active FROM runtime_external_effect_attempts WHERE attempt_id=$1`, handle.Attempt().AttemptID).Scan(&active); err != nil || active {
			t.Fatalf("canceled response persisted executable continuation: active=%t err=%v", active, err)
		}
		requireAuthoredTurnTimeout(t, ctx)
		turn, err := owner.Finish()
		if err != nil || turn.Cancellation.ValidateIntent() != nil || !turn.Cancellation.Origin.Same(handle.Attempt().Origin) {
			t.Fatalf("logical owner missed committed cancellation: %+v err=%v", turn, err)
		}
		requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateSettled, 1, 0)
		settleRecoveredCanceledTurnForTest(t, fixture, handle.Attempt())
		requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateSettled, 1, 0)
	})
}
