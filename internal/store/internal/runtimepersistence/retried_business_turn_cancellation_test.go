package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestRetriedBusinessTurnTerminationAndTimeoutRecoveryBothStores(t *testing.T) {
	for _, mode := range []string{"retry_queued", "unstarted", "authorized", "launched", "timeout-recovery"} {
		t.Run(mode, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				ctx := correlation.WithRunID(fixture.contextFor(fixture.authority), fixture.authority.Target.RunID)
				scope, instance, path, err := fixture.authority.BusinessTurnCoordinates()
				if err != nil {
					t.Fatal(err)
				}
				owner := flowidentity.RunScopedFlowInstance{RunID: fixture.authority.Target.RunID, Route: flowidentity.StoredRoute(scope, instance, path)}
				at, entity := time.Now().UTC().Truncate(time.Microsecond), uuid.NewString()
				record := seedTurnTerminationHeader(t, fixture, owner, entity, at)
				if _, err := fixture.store.(deliverylifecycle.Store).SettleSuccess(ctx, fixture.origin, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
					t.Fatalf("settle the fixture's unrelated initial claim: %v", err)
				}
				event := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(fixture.agentID), AgentIdentity: fixture.authority.Normal.Identity}
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				first, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), event, route)
				if err != nil {
					t.Fatal(err)
				}
				providerCtx := deliverylifecycle.WithClaim(correlation.WithInboundEvent(ctx, event), first.Claim)
				providerCtx = effects.WithTurnTimeout(providerCtx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
				providerCtx = withManagedCompletionTestSurface(t, providerCtx, fixture.authority, "claude_cli")
				handle, err := beginManagedCompletionForTest(t, providerCtx, "claude_cli", []byte("review-first-authorized"))
				if err != nil {
					t.Fatal(err)
				}
				failure := failures.Normalize(failures.New(failures.ClassDependencyUnavailable, "claude_cli_process_start_failed", "reviewer", "start", map[string]any{"launch_rejected": true}), "reviewer", "start")
				if err := handle.Settle(providerCtx, effects.StateTerminalFailure, &failure, map[string]any{"launch_rejected": true}); err != nil {
					t.Fatal(err)
				}
				_, err = fixture.store.(deliverylifecycle.Store).SettleFailure(ctx, first.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: &failure, RetryBase: time.Nanosecond, RuleSelection: handlerselection.NotReached()})
				if err != nil {
					t.Fatal(err)
				}
				firstHistory := snapshotForkHistoricalExecutionTables(t, fixture.db, !fixture.sqlite)
				firstPhysical := historicalEvidenceRow(t, firstHistory, "runtime_external_effect_attempts", "attempt_id", handle.Attempt().AttemptID)
				before, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, first.Claim.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				var current deliverylifecycle.Claim
				if mode != "retry_queued" {
					second, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), event, route)
					if err != nil {
						t.Fatal(err)
					}
					current = second.Claim
					if current.Version() != first.Claim.Version()+1 || current.Same(first.Claim) {
						t.Fatal("did not reclaim the exact origin")
					}
				}
				var retry *effects.Handle
				retryCtx := deliverylifecycle.WithClaim(correlation.WithInboundEvent(ctx, event), current)
				if mode != "unstarted" && mode != "retry_queued" {
					retryCtx = effects.WithTurnTimeout(retryCtx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
					retryCtx = withManagedCompletionTestSurface(t, retryCtx, fixture.authority, "claude_cli")
					retry, err = beginManagedCompletionForTest(t, retryCtx, "claude_cli", []byte("review-first-authorized"))
					if err != nil {
						t.Fatal(err)
					}
					if mode == "launched" || mode == "timeout-recovery" {
						launch, err := fixture.store.MarkExternalAttemptLaunched(retryCtx, retry.Attempt(), at.Add(time.Second))
						if err != nil {
							t.Fatal(err)
						}
						if mode == "timeout-recovery" {
							intent, err := fixture.store.(effects.TurnLifetimeStore).RequestTurnTimeout(retryCtx, retry.Attempt(), launch.Turn.DeadlineAt)
							if err != nil || !intent.Committed || !intent.Requested {
								t.Fatalf("current retry timeout: %+v %v", intent, err)
							}
							if result, err := fixture.store.(effects.TurnLifetimeStore).RequestTurnTimeout(providerCtx, handle.Attempt(), launch.Turn.DeadlineAt); !errors.Is(err, deliverylifecycle.ErrConflict) || result.Committed {
								t.Fatalf("stale predecessor admitted timeout: %+v %v", result, err)
							}
							plan := prepareCanceledReactionForTest(t, retryCtx, fixture, *launch.Turn, intent.RequestedAt)
							command := effects.CanceledTurnCommandForAttempt(retry.Attempt(), plan)
							defer func() {
								if err := fixture.store.(storeTestDurableEventBusStore).PipelineObligations().Release(context.WithoutCancel(ctx), plan.(bus.EnginePublicationPlan).PublicationCommand().Commit.PipelineClaim); err != nil {
									t.Error(err)
								}
							}()
							if result, err := fixture.store.(effects.CanceledTurnStore).CommitCanceledTurn(retryCtx, command); err == nil || result.Acknowledged {
								t.Fatalf("unjoined physical tail settled: %+v %v", result, err)
							}
							if err := retry.Settle(retryCtx, effects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
								t.Fatal(err)
							}
							turns, err := fixture.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
							if err != nil || len(turns) != 1 || !turns[0].Attempt.Origin.Delivery.Same(current) || turns[0].Clock == nil || turns[0].Clock.FirstAttempt != retry.Attempt().AttemptID || !turns[0].Clock.DeadlineAt.Equal(launch.Turn.DeadlineAt) {
								t.Fatalf("current retry timeout recovery: %+v %v", turns, err)
							}
							assertRetriedTurnSettlement(t, retryCtx, fixture, command, before.RetryCount, current.Version(), "turn_timeout")
							assertCanceledReactionCount(t, ctx, fixture, launch.Turn.TimeoutEvent, 1)
							assertRetriedTurnHistory(t, fixture, firstPhysical, handle.Attempt().AttemptID, retry.Attempt().AttemptID, retry.Attempt().AttemptID)
							return
						}
					}
				}
				cause := managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
				node, err := identity.AdmitExecutableNodeDeclaration(owner.Route.ScopeKey, "router")
				if err != nil {
					t.Fatal(err)
				}
				nodeRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: owner.Route.ScopeKey, FlowInstance: path, EntityID: entity})}
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, cause, []events.DeliveryRoute{nodeRoute}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), cause, nodeRoute)
				if err != nil {
					t.Fatal(err)
				}
				result, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, turnTerminationCommandForClaim(t, record, cause, claimed.Claim, at))
				if err != nil || !result.Committed {
					t.Fatalf("terminate current retried origin: committed=%t err=%v", result.Committed, err)
				}
				if mode == "retry_queued" {
					after, err := fixture.store.(deliverylifecycle.Store).Snapshot(ctx, first.Claim.DeliveryID())
					if err != nil || len(result.Lifecycle.QueuedCancellations) != 1 || len(result.Lifecycle.TurnCancellations) != 0 || after.Status != deliverylifecycle.StatusCanceled || after.RetryCount != before.RetryCount || after.ClaimVersion != before.ClaimVersion {
						t.Fatalf("queued retry changed history or invented a claim: %+v %+v %v", result.Lifecycle, after, err)
					}
					assertRetriedTurnHistory(t, fixture, firstPhysical, handle.Attempt().AttemptID, "", handle.Attempt().AttemptID)
					return
				}
				if len(result.Lifecycle.TurnCancellations) != 1 || !result.Lifecycle.TurnCancellations[0].Origin.Delivery.Same(current) {
					t.Fatalf("cancellation did not bind current claim: %+v", result.Lifecycle.TurnCancellations)
				}
				canceled := fixture.store.(effects.CanceledTurnStore)
				if stale, err := canceled.CommitCanceledTurn(providerCtx, effects.CanceledTurnCommandForAttempt(handle.Attempt(), nil)); !errors.Is(err, deliverylifecycle.ErrConflict) || stale.Acknowledged {
					t.Fatalf("stale physical predecessor settled current claim: %+v %v", stale, err)
				}
				origin, err := effects.DeliveryCompletionOrigin(current)
				if err != nil {
					t.Fatal(err)
				}
				command := effects.CanceledTurnCommand{Origin: origin}
				firstLaunch, currentAttempt := "", ""
				if retry != nil {
					command = effects.CanceledTurnCommandForAttempt(retry.Attempt(), nil)
					currentAttempt = retry.Attempt().AttemptID
					if mode == "launched" {
						firstLaunch = currentAttempt
					}
					if early, err := canceled.CommitCanceledTurn(retryCtx, command); err == nil || early.Acknowledged {
						t.Fatalf("termination bypassed physical join: %+v %v", early, err)
					}
					state := effects.StateTerminalFailure
					if mode == "launched" {
						state = effects.StateOutcomeUncertain
					}
					if err := retry.Settle(retryCtx, state, &failure, map[string]any{"physical_joined": true}); err != nil {
						t.Fatal(err)
					}
				}
				turns, err := fixture.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
				if err != nil || len(turns) != 1 || !turns[0].Attempt.Origin.Same(origin) || turns[0].Attempt.AttemptID != currentAttempt || (turns[0].Clock != nil) != (mode == "launched") {
					t.Fatalf("termination recovery changed current work: %+v %v", turns, err)
				}
				assertRetriedTurnSettlement(t, ctx, fixture, command, before.RetryCount, current.Version(), "terminate")
				assertRetriedTurnHistory(t, fixture, firstPhysical, handle.Attempt().AttemptID, firstLaunch, currentAttempt)
			})
		})
	}
}

func assertRetriedTurnSettlement(t *testing.T, ctx context.Context, fixture completionSettlementFixture, command effects.CanceledTurnCommand, retries int, version int64, reason string) {
	t.Helper()
	store := fixture.store.(effects.CanceledTurnStore)
	result, err := store.CommitCanceledTurn(ctx, command)
	if err != nil || result.Validate() != nil || result.Delivery.Status != deliverylifecycle.StatusCanceled || result.Delivery.ReasonCode != reason || result.Delivery.RetryCount != retries || result.Delivery.ClaimVersion != version {
		t.Fatalf("current origin did not settle exactly: %+v %v", result, err)
	}
	before := snapshotForkHistoricalExecutionTables(t, fixture.db, !fixture.sqlite)
	again, err := store.CommitCanceledTurn(ctx, command)
	if err != nil || again.Validate() != nil || !again.Delivery.SettledAt.Equal(result.Delivery.SettledAt) {
		t.Fatalf("repeat changed canceled outcome: %+v %v", again, err)
	}
	after := snapshotForkHistoricalExecutionTables(t, fixture.db, !fixture.sqlite)
	for _, table := range []string{"runtime_agent_turn_lifetimes", "runtime_external_effect_attempts", "event_delivery_attempts", "events"} {
		if !reflect.DeepEqual(before[table], after[table]) {
			t.Fatalf("repeat rewrote %s", table)
		}
	}
	turns, err := fixture.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
	if err != nil || len(turns) != 0 {
		t.Fatalf("settled cancellation remained executable: %+v %v", turns, err)
	}
}

func assertRetriedTurnHistory(t *testing.T, fixture completionSettlementFixture, firstPhysical map[string]json.RawMessage, admitted, firstLaunch, current string) {
	t.Helper()
	history := snapshotForkHistoricalExecutionTables(t, fixture.db, !fixture.sqlite)
	if !reflect.DeepEqual(firstPhysical, historicalEvidenceRow(t, history, "runtime_external_effect_attempts", "attempt_id", admitted)) {
		t.Fatal("retry cancellation rewrote its immutable predecessor")
	}
	row := historicalEvidenceRow(t, history, "runtime_agent_turn_lifetimes", "admitted_attempt_id", admitted)
	for field, want := range map[string]string{"admitted_attempt_id": admitted, "first_attempt_id": firstLaunch, "current_attempt_id": current} {
		var got string
		if err := json.Unmarshal(row[field], &got); err != nil || got != want {
			t.Fatalf("%s=%q want=%q err=%v", field, got, want, err)
		}
	}
}

func historicalEvidenceRow(t *testing.T, snapshot map[string][]string, table, key, id string) map[string]json.RawMessage {
	t.Helper()
	for _, raw := range snapshot[table] {
		var values []json.RawMessage
		if err := json.Unmarshal([]byte(raw), &values); err != nil {
			t.Fatal(err)
		}
		row := make(map[string]json.RawMessage)
		for i, column := range snapshot[table+"/columns"] {
			row[column] = values[i]
		}
		var actual string
		if err := json.Unmarshal(row[key], &actual); err == nil && actual == id {
			return row
		}
	}
	t.Fatalf("missing %s %s=%s", table, key, id)
	return nil
}
