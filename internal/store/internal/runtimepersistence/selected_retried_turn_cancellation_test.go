package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedRetriedTurnCancellationBothStores(t *testing.T) {
	for _, mode := range []string{"unstarted", "authorized", "launched", "timeout_recovery"} {
		for _, backend := range []string{"sqlite", "postgres"} {
			t.Run(mode+"/"+backend, func(t *testing.T) {
				selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
				fixture := newSelectedProviderCompletionFixture(t, selected, db, sqlite)
				ctx := testAuthorActivityContextForBundle(fixture.request.DeclarationPlan.BundleHash)
				issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "selected-retry-cancel", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				plan := admitSelectedProviderFixture(t, ctx, fixture, issued, authority)
				authority.Target = selectedProviderTarget(fixture)
				ctx = effects.WithAuthority(effects.WithController(ctx, newCompletionControllerForTest(selected)), authority)
				ctx = selectedProviderClaimContext(t, ctx, fixture, authority)
				ctx = withManagedCompletionTestSurface(t, managedSelectedExecutionStoreTestContext(t, ctx, authority), authority, "claude_cli")
				ctx = effects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.grant_receiver"})
				first, err := beginManagedCompletionForTest(t, ctx, "claude_cli", []byte("selected-retry-cancel"))
				if err != nil {
					t.Fatal(err)
				}
				failure := retryableTurnPrelaunchFailure()
				if err := first.Settle(ctx, effects.StateTerminalFailure, &failure, map[string]any{"launch_rejected": true}); err != nil {
					t.Fatal(err)
				}
				actual := selected.(deliverylifecycle.Store)
				claim := first.Attempt().Origin.Delivery
				if _, err := actual.SettleFailure(ctx, claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: &failure, RetryBase: time.Nanosecond, RuleSelection: handlerselection.NotReached()}); err != nil {
					t.Fatal(err)
				}
				history := snapshotForkHistoricalExecutionTables(t, db, !sqlite)
				firstPhysical := historicalEvidenceRow(t, history, "runtime_external_effect_attempts", "attempt_id", first.Attempt().AttemptID)
				snapshot, err := actual.Snapshot(ctx, claim.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				event, found := correlation.InboundEventFromContext(ctx)
				if !found {
					t.Fatal("selected retry lacks its admitted event")
				}
				claimed, err := actual.ClaimDelivery(ctx, snapshot.Authority, event, snapshot.Route)
				current, acquired := claimed.Acquired()
				if err != nil || !acquired || current.Claim.Version() != claim.Version()+1 {
					t.Fatalf("selected reclaim: %+v err=%v", claimed, err)
				}
				ctx = deliverylifecycle.WithClaim(ctx, current.Claim)
				origin, err := effects.DeliveryCompletionOrigin(current.Claim)
				if err != nil {
					t.Fatal(err)
				}
				var retry *effects.Handle
				var clock *effects.LogicalTurnClock
				if mode != "unstarted" {
					retry, err = beginManagedCompletionForTest(t, ctx, "claude_cli", []byte("selected-retry-cancel"))
					if err != nil {
						t.Fatal(err)
					}
				}
				if mode == "launched" || mode == "timeout_recovery" {
					launch, err := selected.MarkExternalAttemptLaunched(ctx, retry.Attempt(), time.Now().UTC().Truncate(time.Microsecond))
					if err != nil || launch.Turn == nil {
						t.Fatalf("selected retry launch: %+v %v", launch, err)
					}
					clock = launch.Turn
					if mode == "timeout_recovery" {
						intent, err := selected.(effects.TurnLifetimeStore).RequestTurnTimeout(ctx, retry.Attempt(), launch.Turn.DeadlineAt)
						if err != nil || !intent.Committed || !intent.Requested {
							t.Fatalf("selected retry timeout: %+v %v", intent, err)
						}
					}
				}
				if mode != "timeout_recovery" {
					requestSelectedTurnTerminationForTest(t, ctx, selected, fixture, plan, origin)
				}
				canceled := selected.(effects.CanceledTurnStore)
				if result, err := canceled.CommitCanceledTurn(ctx, effects.CanceledTurnCommandForAttempt(first.Attempt(), nil)); !errors.Is(err, deliverylifecycle.ErrConflict) || result.Acknowledged {
					t.Fatalf("selected stale predecessor settled current claim: %+v %v", result, err)
				}
				if retry != nil {
					if mode != "timeout_recovery" {
						if result, err := canceled.CommitCanceledTurn(ctx, effects.CanceledTurnCommandForAttempt(retry.Attempt(), nil)); err == nil || result.Acknowledged {
							t.Fatalf("selected cancellation bypassed physical join: %+v %v", result, err)
						}
					}
					state := effects.StateTerminalFailure
					if clock != nil {
						state = effects.StateOutcomeUncertain
					}
					if err := retry.Settle(ctx, state, &failure, map[string]any{"physical_joined": true}); err != nil {
						t.Fatal(err)
					}
				}
				if err := fixture.process.Release(ctx); err != nil {
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
					t.Fatalf("selected retry recovery entry: %+v %v", entries, err)
				}
				request := runcontrol.SelectedForkRecoveryRequest{Entry: entries[0], Process: process, Effects: liveExternalEffectRecoveryRequest(time.Now().UTC())}
				pending, err := recovery.RecoverSelectedFork(ctx, request)
				turns := pending.PendingCancellations
				currentAttempt, firstLaunch := "", ""
				if retry != nil {
					currentAttempt = retry.Attempt().AttemptID
				}
				if clock != nil {
					firstLaunch = currentAttempt
				}
				if err != nil || len(turns) != 1 || !turns[0].Attempt.Origin.Same(origin) || turns[0].Attempt.AttemptID != currentAttempt || (turns[0].Clock != nil) != (clock != nil) {
					t.Fatalf("selected retry inventory: %+v %v", turns, err)
				}
				if clock != nil && (turns[0].Clock.FirstAttempt != firstLaunch || !turns[0].Clock.DeadlineAt.Equal(clock.DeadlineAt)) {
					t.Fatal("selected recovery restarted the first-launch clock")
				}
				normal, err := selected.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, request.Effects)
				if err != nil || len(normal) != 0 {
					t.Fatalf("normal inventory acquired selected work: %+v %v", normal, err)
				}
				command := effects.CanceledTurnCommand{Origin: origin}
				if retry != nil {
					command = effects.CanceledTurnCommandForAttempt(turns[0].Attempt, nil)
				}
				if mode == "timeout_recovery" {
					publication := prepareSelectedRetryReactionForTest(t, ctx, selected, fixture, authority)
					reaction, err := publication.PrepareRecoveredTurnTimeoutReaction(ctx, turns[0])
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := publication.ReleaseTurnTimeoutReaction(context.WithoutCancel(ctx), reaction); err != nil {
							t.Error(err)
						}
					}()
					command.Publication = reaction
				}
				request.Cancellations = []effects.CanceledTurnCommand{command}
				result, err := recovery.RecoverSelectedFork(ctx, request)
				if err != nil || len(result.CanceledTurns) != 1 || result.CanceledTurns[0].Validate() != nil || len(result.PendingCancellations) != 0 || !result.CanceledTurns[0].Origin.Same(origin) || result.CanceledTurns[0].Delivery.Status != deliverylifecycle.StatusCanceled || result.CanceledTurns[0].Delivery.ClaimVersion != current.Claim.Version() || result.CanceledTurns[0].Delivery.RetryCount != snapshot.RetryCount {
					t.Fatalf("selected retry cancellation commit: %+v %v", result, err)
				}
				inspection := completionSettlementFixture{store: selected.(completionSettlementTestStore), db: db, sqlite: sqlite, authority: authority}
				before := snapshotForkHistoricalExecutionTables(t, db, !sqlite)
				again, err := recovery.RecoverSelectedFork(ctx, request)
				if err != nil || len(again.CanceledTurns) != 1 || !again.CanceledTurns[0].Delivery.SettledAt.Equal(result.CanceledTurns[0].Delivery.SettledAt) {
					t.Fatalf("selected retry changed terminal evidence: %+v %v", again, err)
				}
				after := snapshotForkHistoricalExecutionTables(t, db, !sqlite)
				for _, table := range []string{"runtime_agent_turn_lifetimes", "runtime_external_effect_attempts", "event_delivery_attempts", "events"} {
					if !reflect.DeepEqual(before[table], after[table]) {
						t.Fatalf("selected repeat rewrote %s", table)
					}
				}
				assertRetriedTurnHistory(t, inspection, firstPhysical, first.Attempt().AttemptID, firstLaunch, currentAttempt)
				if mode == "timeout_recovery" {
					assertCanceledReactionCount(t, ctx, inspection, clock.TimeoutEvent, 1)
				}
			})
		}
	}
}

func prepareSelectedRetryReactionForTest(t *testing.T, ctx context.Context, selected selectedCompletionAuthorityStore, fixture selectedCompletionFixture, authority effects.Authority) *bus.EventBus {
	t.Helper()
	receiver, err := eventreceiver.SelectedRecoveryPublication(authority)
	if err != nil {
		t.Fatal(err)
	}
	deliveryAuthority, err := deliverylifecycle.NewSelectedExecutionAuthority(mustStoreTestSourceArtifactFact(fixture.request.DeclarationPlan.BundleHash), authority.ID, fixture.forkRun, authority.SelectedFork.Generation)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore), bus.EventBusOptions{SourceArtifactFact: mustStoreTestSourceArtifactFact(fixture.request.DeclarationPlan.BundleHash), ContractBundle: fixture.providerSource, ReceiverExecution: receiver, DeliveryAuthority: deliveryAuthority})
	if err != nil {
		t.Fatal(err)
	}
	descriptors, err := rootruntime.AuthorActivityEventDescriptors(fixture.providerSource)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := authoractivity.ScopeFromContext(ctx)
	catalog, err := selected.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { catalog.Release() })
	return publication
}

func retryableTurnPrelaunchFailure() failures.Envelope {
	return failures.Normalize(failures.New(failures.ClassDependencyUnavailable, "claude_cli_process_start_failed", "turn-retry-test", "start", map[string]any{"launch_rejected": true}), "turn-retry-test", "start")
}
