package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestCanceledTurnRecoveryPreservesExactClaimBoundaryBothStores(t *testing.T) {
	for _, boundary := range []string{"expired", "reclaimed"} {
		t.Run(boundary, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, f completionSettlementFixture) {
				ctx := effects.WithTurnTimeout(providerDrainContext(t, f, boundary), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", boundary)
				clock, found := handle.LogicalTurnClock()
				if !found {
					t.Fatal("missing real launch clock")
				}
				intent, err := f.store.(effects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
				if err != nil {
					t.Fatal(err)
				}
				failure := failures.FromError(context.Canceled, "provider-test", "physical_join").Failure
				if err := handle.Settle(ctx, effects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
					t.Fatal(err)
				}
				actual := f.store.(deliverylifecycle.Store)
				original, err := actual.Snapshot(ctx, f.origin.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				persisted, found, err := f.store.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, original.EventID)
				if err != nil || !found {
					t.Fatalf("canonical event missing: %t %v", found, err)
				}
				expireProviderOriginLease(t, f, f.origin)
				command := effects.CanceledTurnCommandForAttempt(handle.Attempt(), prepareCanceledReactionForTest(t, ctx, f, effects.TurnExecutionResult{Attempt: handle.Attempt(), Cancellation: intent, Clock: &clock}))
				defer func() {
					plan := command.Publication.(bus.EnginePublicationPlan)
					if err := f.store.(storeTestDurableEventBusStore).PipelineObligations().Release(context.WithoutCancel(ctx), plan.PublicationCommand().Commit.PipelineClaim); err != nil {
						t.Error(err)
					}
				}()
				recovery := f.store.(effects.CanceledTurnRecoveryStore)
				request := liveExternalEffectRecoveryRequest(time.Now().UTC())
				if boundary == "expired" {
					t.Run("retained_ownership", func(t *testing.T) {
						proveCanceledTurnHistoricalOwnership(t, ctx, f, handle.Attempt(), request)
					})
					turns, err := recovery.ListCanceledTurnRecoveries(ctx, request)
					if err != nil || len(turns) != 1 || !turns[0].Attempt.Origin.Same(handle.Attempt().Origin) {
						t.Fatalf("expired recovery substituted origin: %+v %v", turns, err)
					}
					committed, err := f.store.(effects.CanceledTurnStore).CommitCanceledTurn(ctx, command)
					if err != nil || committed.Validate() != nil || committed.Delivery.Status != deliverylifecycle.StatusCanceled || committed.Delivery.ClaimVersion != f.origin.Version() || committed.Delivery.RetryCount != original.RetryCount {
						t.Fatalf("expired origin invented a new attempt: %+v %v", committed, err)
					}
					again, err := f.store.(effects.CanceledTurnStore).CommitCanceledTurn(ctx, command)
					if err != nil || again.Validate() != nil || !again.Delivery.SettledAt.Equal(committed.Delivery.SettledAt) {
						t.Fatalf("expired retry changed outcome: %+v %v", again, err)
					}
					claimed, err := actual.ClaimDelivery(ctx, original.Authority, persisted.Event.Event(), original.Route)
					if _, acquired := claimed.Acquired(); err != nil || acquired || claimed.Disposition != deliverylifecycle.ClaimTerminal {
						t.Fatalf("canceled origin reclaimed: %+v %v", claimed, err)
					}
					assertCanceledReactionCount(t, ctx, f, clock.TimeoutEvent, 1)
					return
				}
				claimed, err := actual.ClaimDelivery(ctx, original.Authority, persisted.Event.Event(), original.Route)
				successor, acquired := claimed.Acquired()
				if err != nil || !acquired || successor.Claim.Version() != f.origin.Version()+1 {
					t.Fatalf("native reclamation did not win: %+v %v", claimed, err)
				}
				before, err := actual.Snapshot(ctx, f.origin.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				history, err := actual.Outcomes(ctx, f.origin.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				turns, err := recovery.ListCanceledTurnRecoveries(ctx, request)
				if err != nil || len(turns) != 1 || !turns[0].Attempt.Origin.Same(handle.Attempt().Origin) || turns[0].Attempt.Origin.Delivery.Same(successor.Claim) {
					t.Fatalf("read-only recovery substituted successor claim: %+v %v", turns, err)
				}
				committed, err := f.store.(effects.CanceledTurnStore).CommitCanceledTurn(ctx, command)
				if !errors.Is(err, deliverylifecycle.ErrConflict) || committed.Acknowledged {
					t.Fatalf("predecessor settled successor: %+v %v", committed, err)
				}
				after, err := actual.Snapshot(ctx, f.origin.DeliveryID())
				if err != nil || !reflect.DeepEqual(after, before) {
					t.Fatalf("stale cancellation mutated successor: before=%+v after=%+v %v", before, after, err)
				}
				afterHistory, err := actual.Outcomes(ctx, f.origin.DeliveryID())
				if err != nil || !reflect.DeepEqual(afterHistory, history) {
					t.Fatalf("stale cancellation rewrote immutable attempts: %+v %v", afterHistory, err)
				}
				assertCanceledReactionCount(t, ctx, f, clock.TimeoutEvent, 0)
			})
		})
	}
}

func proveCanceledTurnHistoricalOwnership(t *testing.T, ctx context.Context, f completionSettlementFixture, attempt effects.Attempt, request effects.RecoveryRequest) {
	t.Helper()
	if !attempt.InstanceOwnership.RunOwned {
		t.Fatal("historical run-owned control lost its native nonmembership proof")
	}
	var original []byte
	if err := f.db.QueryRowContext(ctx, `SELECT authority_evidence FROM runtime_external_effect_attempts WHERE attempt_id=$1`, attempt.AttemptID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	var evidence map[string]json.RawMessage
	if err := json.Unmarshal(original, &evidence); err != nil {
		t.Fatal(err)
	}
	write := func(raw []byte) {
		t.Helper()
		if _, err := f.db.ExecContext(ctx, `UPDATE runtime_external_effect_attempts SET authority_evidence=$1 WHERE attempt_id=$2`, string(raw), attempt.AttemptID); err != nil {
			t.Fatal(err)
		}
	}
	defer write(original)
	for _, invalid := range []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`{}`), json.RawMessage(`{"run_owned":true,"instance_life":{"run_id":"invalid"}}`)} {
		evidence["instance_ownership"] = invalid
		raw, err := json.Marshal(evidence)
		if err != nil {
			t.Fatal(err)
		}
		write(raw)
		if rows, err := f.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, request); err == nil || len(rows) != 0 {
			t.Fatalf("malformed retained statement became absence: rows=%+v err=%v", rows, err)
		}
	}
	// Model only the ruled no-counter deployment population, not recovery of
	// candidate-written life-2 scratch data. This turn is native run-owned work.
	delete(evidence, "instance_ownership")
	historical, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	write(historical)
	before := snapshotForkHistoricalExecutionTables(t, f.db, !f.sqlite)
	rows, err := f.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, request)
	if err != nil || len(rows) != 1 || !rows[0].Attempt.InstanceOwnership.Equal(attempt.InstanceOwnership) {
		t.Fatalf("historical owner-proven run-owned recovery: rows=%+v err=%v", rows, err)
	}
	if after := snapshotForkHistoricalExecutionTables(t, f.db, !f.sqlite); !reflect.DeepEqual(before, after) {
		t.Fatal("locking inventory wrote historical evidence or a revision")
	}
}
