package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestProviderAttemptDrainRejectsMissingOrForeignOriginClaimParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		for _, candidate := range []struct {
			name string
			ctx  func(context.Context) context.Context
		}{
			{name: "missing", ctx: runtimedelivery.WithoutClaim},
			{name: "foreign", ctx: func(ctx context.Context) context.Context {
				claim, err := runtimedelivery.AdmitPersistedClaim(
					uuid.NewString(), fixture.authority.Target.RunID, "foreign-provider-origin",
					uuid.NewString(), 1, runtimedelivery.SubscriberAgent, fixture.agentID,
				)
				if err != nil {
					t.Fatalf("admit foreign origin claim: %v", err)
				}
				return runtimedelivery.WithClaim(ctx, claim)
			}},
		} {
			t.Run(candidate.name, func(t *testing.T) {
				ctx := candidate.ctx(providerDrainContext(t, fixture, "origin-"+candidate.name))
				if _, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte(candidate.name), nil); err == nil {
					t.Fatalf("%s origin claim authorized provider completion", candidate.name)
				}
				requireProviderAttemptCount(t, fixture, 0)
			})
		}
	})
}

func TestProviderAttemptLaunchVersusLifecycleSupersessionParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		t.Run("transition_wins", func(t *testing.T) {
			ctx := providerDrainContext(t, fixture, "transition-wins")
			handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte("transition-wins"), nil)
			if err != nil {
				t.Fatalf("authorize completion: %v", err)
			}
			result := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
			if result.Phase != runtimemanager.AgentLifecycleTerminated || result.ProviderDrainCount != 0 {
				t.Fatalf("transition result=%+v, want terminal without drain", result)
			}
			requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateTerminalFailure)
			requireProviderDrainCount(t, fixture, handle.Attempt().AttemptID, 0)
			requireCompletionRecoveryRows(t, fixture, handle.Attempt().AttemptID, 0, 0, 0)
			requireOriginDeliveryOutcome(t, fixture, "dead_letter", "provider_attempt_superseded_before_launch")
			if err := handle.MarkLaunched(ctx); err == nil {
				t.Fatal("transition-wins attempt reached provider launch")
			}
		})

		t.Run("launch_wins", func(t *testing.T) {
			fixture = freshProviderDrainFixture(t, fixture.sqlite)
			ctx := providerDrainContext(t, fixture, "launch-wins")
			handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "launch-wins")
			result := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
			if result.Phase != runtimemanager.AgentLifecycleDraining || result.ProviderDrainCount != 1 {
				t.Fatalf("transition result=%+v, want one terminal drain", result)
			}
			requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "pending")
			requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleDraining, result.Generation)
		})
	})
}

func TestProviderAttemptDrainLaunchSupersessionRaceParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		ctx := providerDrainContext(t, fixture, "launch-race")
		handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte("launch-race"), nil)
		if err != nil {
			t.Fatalf("authorize raced completion: %v", err)
		}

		start := make(chan struct{})
		launchResult := make(chan error, 1)
		transitionResult := make(chan struct {
			result runtimemanager.AgentLifecycleTransitionResult
			err    error
		}, 1)
		var ready sync.WaitGroup
		ready.Add(2)
		go func() {
			ready.Done()
			<-start
			launchResult <- handle.MarkLaunched(ctx)
		}()
		go func() {
			ready.Done()
			<-start
			result, err := commitProviderDrainTransition(fixture, "teardown", runtimemanager.AgentLifecycleTerminated)
			transitionResult <- struct {
				result runtimemanager.AgentLifecycleTransitionResult
				err    error
			}{result: result, err: err}
		}()
		ready.Wait()
		close(start)
		launchErr := <-launchResult
		transition := <-transitionResult
		if transition.err != nil {
			t.Fatalf("commit raced lifecycle transition: %v", transition.err)
		}
		if launchErr == nil {
			if transition.result.ProviderDrainCount != 1 || transition.result.Phase != runtimemanager.AgentLifecycleDraining {
				t.Fatalf("launch-wins race transition=%+v, want one drain", transition.result)
			}
			requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "pending")
			requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateLaunched)
			return
		}
		if transition.result.ProviderDrainCount != 0 || transition.result.Phase != runtimemanager.AgentLifecycleTerminated {
			t.Fatalf("transition-wins race transition=%+v, want terminal without drain", transition.result)
		}
		requireProviderDrainCount(t, fixture, handle.Attempt().AttemptID, 0)
		requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateTerminalFailure)
	})
}

func TestProviderAttemptDrainLifecycleTransitionRollbackParity(t *testing.T) {
	cases := []struct {
		name       string
		observe    bool
		boundary   providerDrainFailureBoundary
		wantState  runtimeeffects.State
		wantDrains int
	}{
		{
			name: "authorized_terminalization", boundary: providerDrainFailureBoundary{
				name: "origin_outcome", table: "event_delivery_outcomes", operation: "INSERT",
			}, wantState: runtimeeffects.StateAuthorized,
		},
		{
			name: "launched_drain_capture", observe: true, boundary: providerDrainFailureBoundary{
				name: "drain_capture", table: "runtime_provider_attempt_drains", operation: "INSERT",
			}, wantState: runtimeeffects.StateResponseObserved,
		},
	}
	forEachProviderDrainStore(t, func(t *testing.T, base completionSettlementFixture) {
		for _, candidate := range cases {
			t.Run(candidate.name, func(t *testing.T) {
				fixture := freshProviderDrainFixture(t, base.sqlite)
				ctx := providerDrainContext(t, fixture, "transition-rollback-"+candidate.name)
				handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte(candidate.name), nil)
				if err != nil {
					t.Fatalf("authorize transition rollback completion: %v", err)
				}
				if candidate.observe {
					if err := handle.MarkLaunched(ctx); err != nil {
						t.Fatalf("launch transition rollback completion: %v", err)
					}
					if err := handle.MarkResponseObserved(ctx, map[string]any{"case": candidate.name}); err != nil {
						t.Fatalf("observe transition rollback completion: %v", err)
					}
				}
				installProviderDrainFailureBoundary(t, fixture, candidate.boundary)
				if _, err := commitProviderDrainTransition(fixture, "teardown", runtimemanager.AgentLifecycleTerminated); err == nil {
					t.Fatalf("%s transition did not roll back", candidate.name)
				}
				requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, candidate.wantState)
				requireProviderDrainCount(t, fixture, handle.Attempt().AttemptID, candidate.wantDrains)
				requireCompletionRecoveryRows(t, fixture, handle.Attempt().AttemptID, 0, 0, 1)
				requireDeliveryClaimPending(t, fixture, fixture.origin)
				requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleRunning, fixture.authority.Normal.Generation)
			})
		}
	})
}

func TestProviderAttemptDrainLifecycleSupersessionParity(t *testing.T) {
	targets := []struct {
		name       string
		phase      runtimemanager.AgentLifecyclePhase
		wantDuring runtimemanager.AgentLifecyclePhase
		wantAfter  runtimemanager.AgentLifecyclePhase
	}{
		{name: "restart", phase: runtimemanager.AgentLifecycleRunning, wantDuring: runtimemanager.AgentLifecycleRunning, wantAfter: runtimemanager.AgentLifecycleRunning},
		{name: "reconfigure", phase: runtimemanager.AgentLifecycleRunning, wantDuring: runtimemanager.AgentLifecycleRunning, wantAfter: runtimemanager.AgentLifecycleRunning},
		{name: "teardown", phase: runtimemanager.AgentLifecycleTerminated, wantDuring: runtimemanager.AgentLifecycleDraining, wantAfter: runtimemanager.AgentLifecycleTerminated},
		{name: "fail", phase: runtimemanager.AgentLifecycleFailed, wantDuring: runtimemanager.AgentLifecycleDraining, wantAfter: runtimemanager.AgentLifecycleFailed},
	}
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		for _, target := range targets {
			t.Run(target.name, func(t *testing.T) {
				fixture := freshProviderDrainFixture(t, fixture.sqlite)
				ctx := providerDrainContext(t, fixture, "supersession-"+target.name)
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", target.name)
				result := supersedeProviderDrainFixtureWithKind(t, fixture, target.name, target.phase)
				if result.ProviderDrainCount != 1 || result.Phase != target.wantDuring {
					t.Fatalf("transition result=%+v, want one drain in %s", result, target.wantDuring)
				}
				requireAgentLifecycleState(t, fixture, target.wantDuring, result.Generation)

				settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "provider-head-current", "forbidden-head")
				settled, err := handle.SettleCompletion(ctx, settlement)
				if err != nil {
					t.Fatalf("settle drained completion: %v", err)
				}
				if settled.Disposition != runtimeeffects.CompletionSettlementDrained {
					t.Fatalf("completion disposition=%q, want drained", settled.Disposition)
				}
				wantFinalization := target.phase != runtimemanager.AgentLifecycleRunning
				if !settled.Committed || !settled.SpendRecorded || !settled.OriginSettled || (settled.Finalization != nil) != wantFinalization {
					t.Fatalf("drained settlement result=%+v, want complete immutable commit with finalization=%t", settled, wantFinalization)
				}
				requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateSettled)
				requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
				requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateSettled, 1, 0)
				requireOriginDeliveryOutcome(t, fixture, "delivered", "")
				requireAgentLifecycleState(t, fixture, target.wantAfter, result.Generation)
				requireProviderHead(t, fixture.db, fixture.sqlite, fixture.sessionID, "provider-head-current")
			})
		}
	})
}

func TestProviderAttemptHeartbeatAuthorityParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, base completionSettlementFixture) {
		for _, candidate := range []struct {
			name    string
			drained bool
		}{
			{name: "current"},
			{name: "drained", drained: true},
		} {
			t.Run(candidate.name, func(t *testing.T) {
				fixture := freshProviderDrainFixture(t, base.sqlite)
				ctx := providerDrainContext(t, fixture, "heartbeat-"+candidate.name)
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", candidate.name)
				if candidate.drained {
					transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
					if transition.ProviderDrainCount != 1 {
						t.Fatalf("capture heartbeat drain: %+v", transition)
					}
				}
				now := time.Now().UTC()
				shortLease := now.Add(20 * time.Second)
				setCompletionAttemptLease(t, fixture, handle.Attempt().AttemptID, shortLease)
				setProviderOriginLease(t, fixture, fixture.origin, shortLease)
				if candidate.drained {
					setProviderDrainLease(t, fixture, handle.Attempt().AttemptID, shortLease)
				}
				if err := fixture.store.HeartbeatCompletionAttempt(ctx, handle.Attempt(), now, 2*time.Minute); err != nil {
					t.Fatalf("heartbeat exact provider authority: %v", err)
				}
				minimum := now.Add(time.Minute)
				if lease := completionAttemptLease(t, fixture, handle.Attempt().AttemptID); !lease.After(minimum) {
					t.Fatalf("effect lease=%s, want after %s", lease, minimum)
				}
				if lease := providerOriginLease(t, fixture, fixture.origin); !lease.After(minimum) {
					t.Fatalf("origin lease=%s, want after %s", lease, minimum)
				}
				if candidate.drained {
					if lease := providerDrainLease(t, fixture, handle.Attempt().AttemptID); !lease.After(minimum) {
						t.Fatalf("drain lease=%s, want after %s", lease, minimum)
					}
				}

				before := completionAttemptLease(t, fixture, handle.Attempt().AttemptID)
				stale := handle.Attempt()
				foreign, err := runtimedelivery.AdmitPersistedClaim(
					fixture.origin.DeliveryID(), fixture.origin.RunID(), fixture.origin.RouteIdentity(),
					uuid.NewString(), fixture.origin.Version()+1, fixture.origin.SubscriberClass(), fixture.origin.SubscriberID(),
				)
				if err != nil {
					t.Fatalf("admit foreign heartbeat claim: %v", err)
				}
				stale.Origin = runtimeeffects.CompletionOrigin{Kind: runtimeeffects.CompletionOriginDelivery, Delivery: foreign}
				if err := fixture.store.HeartbeatCompletionAttempt(ctx, stale, now.Add(time.Second), 3*time.Minute); err == nil {
					t.Fatal("foreign provider origin renewed authority")
				}
				if after := completionAttemptLease(t, fixture, handle.Attempt().AttemptID); !after.Equal(before) {
					t.Fatalf("failed heartbeat changed effect lease from %s to %s", before, after)
				}
			})
		}
	})
}

func TestProviderAttemptCompletionAfterOriginalOriginExpiryParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, base completionSettlementFixture) {
		for _, candidate := range []struct {
			name    string
			drained bool
		}{
			{name: "current"},
			{name: "drained", drained: true},
		} {
			t.Run(candidate.name, func(t *testing.T) {
				fixture := freshProviderDrainFixture(t, base.sqlite)
				ctx := providerDrainContext(t, fixture, "completion-after-origin-expiry-"+candidate.name)
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", candidate.name)
				if candidate.drained {
					transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
					if transition.ProviderDrainCount != 1 {
						t.Fatalf("capture delayed-completion drain: %+v", transition)
					}
				}
				now := time.Now().UTC()
				// Keep the synthetic deadline beyond PostgreSQL's persisted start and
				// SQLite's second-granularity expiry comparisons.
				originalExpiry := now.Add(2 * time.Second)
				setCompletionAttemptLease(t, fixture, handle.Attempt().AttemptID, originalExpiry)
				setProviderOriginLease(t, fixture, fixture.origin, originalExpiry)
				if candidate.drained {
					setProviderDrainLease(t, fixture, handle.Attempt().AttemptID, originalExpiry)
				}
				if err := fixture.store.HeartbeatCompletionAttempt(ctx, handle.Attempt(), now, 2*time.Minute); err != nil {
					t.Fatalf("retain delayed provider authority: %v", err)
				}
				if delay := time.Until(originalExpiry.Add(20 * time.Millisecond)); delay > 0 {
					timer := time.NewTimer(delay)
					<-timer.C
				}
				settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "provider-head-current", "provider-head-delayed")
				if candidate.drained {
					settlement.ProviderHead = nil
				}
				if _, err := handle.SettleCompletion(ctx, settlement); err != nil {
					t.Fatalf("settle after original origin expiry: %v", err)
				}
				if !candidate.drained {
					deliveryStore := fixture.store.(runtimedelivery.Store)
					if _, err := deliveryStore.SettleSuccess(testAuthorActivityContext(), fixture.origin, nil, 0); err != nil {
						t.Fatalf("settle renewed current origin: %v", err)
					}
				}
				requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateSettled)
				requireOriginDeliveryOutcome(t, fixture, "delivered", "")
				if candidate.drained {
					requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
				}
			})
		}
	})
}

func TestProviderAttemptDrainAdapterParity(t *testing.T) {
	adapters := []string{"anthropic_api", "openai_compatible", "openai_responses", "claude_cli", "mock_python"}
	forEachProviderDrainStore(t, func(t *testing.T, base completionSettlementFixture) {
		for _, adapter := range adapters {
			t.Run(adapter, func(t *testing.T) {
				fixture := freshProviderDrainFixture(t, base.sqlite)
				authority := fixture.authority
				if adapter == "mock_python" {
					authority.ExecutionMode = runtimeeffects.ExecutionModeMock
				}
				ctx := providerDrainAttemptContext(t, fixture, authority, fixture.origin, adapter, "adapter-"+adapter)
				handle := beginObservedCompletionForSettlementTest(t, ctx, adapter, adapter)
				transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
				if transition.ProviderDrainCount != 1 || transition.Phase != runtimemanager.AgentLifecycleDraining {
					t.Fatalf("%s transition=%+v, want one draining attempt", adapter, transition)
				}

				settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, adapter, "provider-head-current", "")
				settlement.ProviderHead = nil
				settlement.Spend.Provider = adapter
				if registration, ok := runtimeeffects.RegistrationFor(adapter); ok {
					settlement.Spend.Transport = registration.Transport
				}
				applyManagedCompletionTestSurface(t, settlement.AgentTurn, handle.Attempt().Authority, adapter)
				if _, err := handle.SettleCompletion(ctx, settlement); err != nil {
					t.Fatalf("settle drained %s completion: %v", adapter, err)
				}
				requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateSettled, 1, 0)
				requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
				requireOriginDeliveryOutcome(t, fixture, "delivered", "")
				requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleTerminated, transition.Generation)
			})
		}
	})
}

func TestProviderAttemptDrainSameSlugSiblingIsolationParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		fixture.authority.Target.EntityID = uuid.NewString()
		fixture.authority.BudgetScopes = []runtimeeffects.BudgetAdmissionScope{{Kind: "entity", Key: fixture.authority.Target.EntityID, CapUSD: 1}}
		sibling := newProviderDrainSiblingFixture(t, fixture)

		primaryCtx := providerDrainAttemptContext(t, fixture, fixture.authority, fixture.origin, "anthropic_api", "same-slug-primary")
		primary := beginObservedCompletionForSettlementTest(t, primaryCtx, "anthropic_api", "same-slug-primary")
		siblingCtx := providerDrainAttemptContext(t, sibling, sibling.authority, sibling.origin, "anthropic_api", "same-slug-sibling")
		siblingHandle := beginObservedCompletionForSettlementTest(t, siblingCtx, "anthropic_api", "same-slug-sibling")

		transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
		if transition.ProviderDrainCount != 1 || transition.Phase != runtimemanager.AgentLifecycleDraining {
			t.Fatalf("primary transition=%+v, want exactly one primary drain", transition)
		}
		requireProviderDrainState(t, fixture, primary.Attempt().AttemptID, "pending")
		requireProviderDrainCount(t, sibling, siblingHandle.Attempt().AttemptID, 0)
		requireExternalAttemptState(t, sibling.db, sibling.sqlite, siblingHandle.Attempt().AttemptID, runtimeeffects.StateResponseObserved)
		requireAgentLifecycleState(t, sibling, runtimemanager.AgentLifecycleRunning, sibling.authority.Normal.Generation)
	})
}

func TestProviderAttemptDrainTenAttemptExactnessParity(t *testing.T) {
	const attemptCount = 10
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		handles := make([]*runtimeeffects.Handle, 0, attemptCount)
		contexts := make([]context.Context, 0, attemptCount)
		claims := make([]runtimedelivery.Claim, 0, attemptCount)
		for index := 0; index < attemptCount; index++ {
			claim := fixture.origin
			if index > 0 {
				claim = newProviderDrainOriginClaim(t, fixture, index)
			}
			authority := fixture.authority
			authority.Target.ID = uuid.NewString()
			authority.Target.EntityID = uuid.NewString()
			authority.BudgetScopes = []runtimeeffects.BudgetAdmissionScope{{Kind: "entity", Key: authority.Target.EntityID, CapUSD: 1}}
			ctx := providerDrainAttemptContext(t, fixture, authority, claim, "openai_responses", fmt.Sprintf("batch-%02d", index))
			handle := beginObservedCompletionForSettlementTest(t, ctx, "openai_responses", fmt.Sprintf("batch-%02d", index))
			handles = append(handles, handle)
			contexts = append(contexts, ctx)
			claims = append(claims, claim)
		}

		transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
		if transition.ProviderDrainCount != attemptCount || transition.Phase != runtimemanager.AgentLifecycleDraining {
			t.Fatalf("batch transition=%+v, want %d drains", transition, attemptCount)
		}

		for index, handle := range handles {
			settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "openai_responses", "provider-head-current", "")
			settlement.ProviderHead = nil
			settlement.AgentTurn.ToolCalls = json.RawMessage(fmt.Sprintf(`[{"id":"tool-%02d","name":"lookup"}]`, index))
			settlement.AgentTurn.EmittedEvents = json.RawMessage(fmt.Sprintf(`[{"type":"batch.completed","index":%d}]`, index))
			if _, err := handle.SettleCompletion(contexts[index], settlement); err != nil {
				t.Fatalf("settle batch attempt %d: %v", index, err)
			}
			requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
			requireDeliveryClaimOutcome(t, fixture, claims[index], "delivered", "")
			wantPhase := runtimemanager.AgentLifecycleDraining
			if index == attemptCount-1 {
				wantPhase = runtimemanager.AgentLifecycleTerminated
			}
			requireAgentLifecycleState(t, fixture, wantPhase, transition.Generation)
		}

		requireProviderDrainAggregate(t, fixture, attemptCount, 2.50)
		requireProviderDrainImmutableSnapshots(t, fixture, attemptCount)
	})
}

func TestProviderAttemptDrainTerminalFailureParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		ctx := providerDrainContext(t, fixture, "terminal-failure")
		handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "terminal-failure")
		transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleFailed)
		settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "provider-head-current", "")
		settlement.ProviderHead = nil
		failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "provider_terminal_failure", nil)
		settlement.Settlement.State = runtimeeffects.StateTerminalFailure
		settlement.Settlement.Failure = &failure
		settlement.AgentTurn.Failure = &failure
		if _, err := handle.SettleCompletion(ctx, settlement); err != nil {
			t.Fatalf("settle drained terminal failure: %v", err)
		}
		requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateTerminalFailure)
		requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateTerminalFailure, 1, 0)
		requireOriginDeliveryOutcome(t, fixture, "dead_letter", "provider_terminal_failure")
		requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleFailed, transition.Generation)
	})
}

func TestProviderAttemptDrainSettlementRollbackParity(t *testing.T) {
	boundaries := []providerDrainFailureBoundary{
		{name: "agent_turn", table: "agent_turns", operation: "INSERT"},
		{name: "spend", table: "spend_ledger", operation: "INSERT"},
		{name: "origin_outcome", table: "event_delivery_outcomes", operation: "INSERT"},
		{name: "drain_settlement", table: "runtime_provider_attempt_drains", operation: "UPDATE", predicate: "NEW.state='settled'"},
		{name: "lifecycle_finalization", table: "agents", operation: "UPDATE", predicate: "OLD.lifecycle_phase='draining' AND NEW.lifecycle_phase='terminated'"},
	}
	forEachProviderDrainStore(t, func(t *testing.T, base completionSettlementFixture) {
		for _, boundary := range boundaries {
			t.Run(boundary.name, func(t *testing.T) {
				fixture := freshProviderDrainFixture(t, base.sqlite)
				ctx := providerDrainContext(t, fixture, "rollback-"+boundary.name)
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", boundary.name)
				transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
				installProviderDrainFailureBoundary(t, fixture, boundary)

				settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "provider-head-current", "")
				settlement.ProviderHead = nil
				if _, err := handle.SettleCompletion(ctx, settlement); err == nil {
					t.Fatalf("%s boundary did not fail", boundary.name)
				}
				requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateResponseObserved)
				requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "pending")
				requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateResponseObserved, 0, 1)
				requireDeliveryClaimPending(t, fixture, fixture.origin)
				requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleDraining, transition.Generation)
			})
		}
	})
}

func TestProviderAttemptDrainStartupRecoveryParity(t *testing.T) {
	cases := []struct {
		name          string
		observe       bool
		expire        bool
		capture       bool
		originExpired bool
		wantCode      string
	}{
		{name: "after_launch", capture: false, wantCode: "effect_recovery_outcome_unconfirmed"},
		{name: "after_capture", capture: true, wantCode: "effect_recovery_outcome_unconfirmed"},
		{name: "after_response", observe: true, capture: true, wantCode: "effect_recovery_outcome_unconfirmed"},
		{name: "after_launch_expired_origin", capture: false, originExpired: true, wantCode: "effect_recovery_outcome_unconfirmed"},
		{name: "after_capture_expired_origin", capture: true, originExpired: true, wantCode: "effect_recovery_outcome_unconfirmed"},
		{name: "drain_expiry", observe: true, capture: true, expire: true, wantCode: "provider_attempt_drain_expired"},
	}
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		for _, candidate := range cases {
			t.Run(candidate.name, func(t *testing.T) {
				fixture := freshProviderDrainFixture(t, fixture.sqlite)
				ctx := providerDrainContext(t, fixture, "recovery-"+candidate.name)
				handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte(candidate.name), nil)
				if err != nil {
					t.Fatalf("authorize completion: %v", err)
				}
				if err := handle.MarkLaunched(ctx); err != nil {
					t.Fatalf("launch completion: %v", err)
				}
				if candidate.observe {
					if err := handle.MarkResponseObserved(ctx, map[string]any{"case": candidate.name}); err != nil {
						t.Fatalf("observe completion: %v", err)
					}
				}
				var transition runtimemanager.AgentLifecycleTransitionResult
				if candidate.capture {
					transition = supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
				} else {
					setCompletionAttemptLease(t, fixture, handle.Attempt().AttemptID, time.Now().UTC().Add(-time.Minute))
				}
				if candidate.originExpired {
					expireProviderOriginLease(t, fixture, fixture.origin)
				}
				now := time.Now().UTC()
				if candidate.expire {
					now = now.Add(10 * time.Minute)
				}
				summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now))
				if err != nil {
					t.Fatalf("recover completion: %v", err)
				}
				if summary.OutcomeUncertain != 1 || summary.PrelaunchTerminal != 0 {
					t.Fatalf("recovery summary=%+v, want one uncertain", summary)
				}
				requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
				requireCompletionRecoveryRows(t, fixture, handle.Attempt().AttemptID, 1, 1, 0)
				requireOriginDeliveryOutcome(t, fixture, "dead_letter", candidate.wantCode)
				if candidate.capture {
					wantDrain := "settled"
					if candidate.expire {
						wantDrain = "expired"
					}
					requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, wantDrain)
					requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleTerminated, transition.Generation)
				}
				again, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now.Add(time.Second)))
				if err != nil || again != (runtimeeffects.RecoverySummary{}) {
					t.Fatalf("idempotent recovery summary=%+v err=%v", again, err)
				}
			})
		}
	})
}

func TestProviderAttemptDrainPreTopologyRecoveryParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		ctx := providerDrainContext(t, fixture, "pre-topology-recovery")
		handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "pre-topology-recovery")
		transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)

		requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleDraining, transition.Generation)
		requireAgentCoarseStatus(t, fixture, "terminated")

		now := time.Now().UTC()
		summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now))
		if err != nil {
			t.Fatalf("recover terminal drain before topology readback: %v", err)
		}
		if summary.OutcomeUncertain != 1 || summary.PrelaunchTerminal != 0 {
			t.Fatalf("pre-topology recovery summary=%+v, want one uncertain", summary)
		}
		requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
		requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
		requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleTerminated, transition.Generation)
		requireAgentCoarseStatus(t, fixture, "terminated")

		again, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now.Add(time.Second)))
		if err != nil || again != (runtimeeffects.RecoverySummary{}) {
			t.Fatalf("idempotent pre-topology recovery summary=%+v err=%v", again, err)
		}
	})
}

func TestProviderAttemptDrainRejectsTransitionWhilePendingParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		ctx := providerDrainContext(t, fixture, "pending-drain-transition-fence")
		handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "pending-drain-transition-fence")
		transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
		selected, ok := fixture.store.(agentfixture.Store)
		if !ok {
			t.Fatalf("completion store %T does not support lifecycle transitions", fixture.store)
		}
		for _, operation := range []string{"restart", "reconfigure"} {
			_, err := agentfixture.Commit(testAuthorActivityContext(), selected, runtimemanager.AgentLifecycleTransition{
				OperationID: uuid.NewString(), OperationKind: operation, RequestHash: "pending-drain-" + operation,
				Identity: fixture.authority.Normal.Identity, AgentID: fixture.agentID, Trigger: "provider_drain_test",
				ExpectedEpoch: fixture.authority.Normal.RuntimeEpoch, ExpectedGeneration: transition.Generation,
				ExpectedPhase: runtimemanager.AgentLifecycleDraining,
				TargetEpoch:   fixture.authority.Normal.RuntimeEpoch, TargetGeneration: transition.Generation + 1,
				TargetPhase: runtimemanager.AgentLifecycleRunning, ConfigRevision: "pending-drain-reintroduction",
				RunMode: runtimemanager.AgentRunModeStandard, Now: time.Now().UTC(),
			})
			failure, matched := runtimefailures.As(err)
			if err == nil || !matched || failure.Failure.Detail.Code != "provider_attempt_drain_transition_blocked" {
				t.Fatalf("%s pending-drain transition error=%v, want provider_attempt_drain_transition_blocked", operation, err)
			}
		}
		requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "pending")
		requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleDraining, transition.Generation)
	})
}

func TestProviderAttemptDrainTerminalRunRecoveryParity(t *testing.T) {
	type runQuiescenceStore interface {
		ApplyActiveRunQuiescence(context.Context, runtimerunquiescence.Request) (runtimerunquiescence.Result, error)
	}
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		ctx := providerDrainContext(t, fixture, "terminal-run-recovery")
		handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "terminal-run-recovery")
		transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleRunning)
		selected, ok := fixture.store.(runQuiescenceStore)
		if !ok {
			t.Fatalf("completion store %T does not expose run quiescence", fixture.store)
		}
		quiesced, err := selected.ApplyActiveRunQuiescence(testAuthorActivityContext(), runtimerunquiescence.Request{
			OperationName: "provider_attempt_terminal_run_recovery", RequestedAt: time.Now().UTC(),
			RunIDs: []string{fixture.authority.Target.RunID}, ReasonCode: runtimerunquiescence.ServeAbandonReasonCode,
			ControlledBy: "provider-drain-test", DeliveryNote: "terminal provider drain recovery proof",
		})
		if err != nil || len(quiesced.Runs) != 1 || !quiesced.Runs[0].Changed || len(quiesced.Deliveries) != 1 {
			t.Fatalf("terminalize provider run: result=%+v err=%v, want one run and one origin delivery", quiesced, err)
		}
		requireTerminalizedOriginDelivery(t, fixture, runtimerunquiescence.ServeAbandonReasonCode)

		now := time.Now().UTC()
		summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now))
		if err != nil || summary.OutcomeUncertain != 1 {
			t.Fatalf("recover terminal-run provider drain: summary=%+v err=%v", summary, err)
		}
		requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
		requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
		requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleRunning, transition.Generation)
		requireTerminalizedOriginDelivery(t, fixture, runtimerunquiescence.ServeAbandonReasonCode)
		again, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now.Add(time.Second)))
		if err != nil || again != (runtimeeffects.RecoverySummary{}) {
			t.Fatalf("repeat terminal-run recovery: summary=%+v err=%v", again, err)
		}
	})
}

func TestProviderAttemptDrainRecoveryRejectsNewerOriginClaimParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		ctx := providerDrainContext(t, fixture, "recovery-newer-origin")
		handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "newer-origin")
		transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
		expireProviderOriginLease(t, fixture, fixture.origin)
		replaceProviderOriginClaim(t, fixture, fixture.origin)
		if _, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(time.Now().UTC())); err == nil {
			t.Fatal("provider recovery accepted a newer origin claim")
		}
		requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateResponseObserved)
		requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "pending")
		requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleDraining, transition.Generation)
	})
}

func forEachProviderDrainStore(t *testing.T, proof func(*testing.T, completionSettlementFixture)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		proof(t, newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true))
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		proof(t, newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false))
	})
}

func freshProviderDrainFixture(t *testing.T, sqlite bool) completionSettlementFixture {
	t.Helper()
	if sqlite {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		return newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true)
	}
	_, db, _ := testutil.StartPostgres(t)
	return newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false)
}

func providerDrainContext(t *testing.T, fixture completionSettlementFixture, operation string) context.Context {
	t.Helper()
	return providerDrainAttemptContext(t, fixture, fixture.authority, fixture.origin, "anthropic_api", operation)
}

func providerDrainAttemptContext(
	t *testing.T,
	fixture completionSettlementFixture,
	authority runtimeeffects.Authority,
	claim runtimedelivery.Claim,
	adapter string,
	operation string,
) context.Context {
	t.Helper()
	ctx := runtimeeffects.WithExecutionMode(testAuthorActivityContext(), authority.ExecutionMode)
	controller := runtimeeffects.NewCompletionController(fixture.store, fixture.store, fixture.store, nil)
	posture := executionposture.Live
	if authority.ExecutionMode == runtimeeffects.ExecutionModeMock {
		posture = executionposture.MockOnly
	}
	ctx = runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), controller.WithExecutionPosture(posture))
	ctx = runtimedelivery.WithClaim(ctx, claim)
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, "provider-drain:"+operation)
	return withManagedCompletionTestSurface(t, ctx, authority, adapter)
}

func supersedeProviderDrainFixture(t *testing.T, fixture completionSettlementFixture, phase runtimemanager.AgentLifecyclePhase) runtimemanager.AgentLifecycleTransitionResult {
	t.Helper()
	kind := "restart"
	switch phase {
	case runtimemanager.AgentLifecycleTerminated:
		kind = "teardown"
	case runtimemanager.AgentLifecycleFailed:
		kind = "fail"
	}
	return supersedeProviderDrainFixtureWithKind(t, fixture, kind, phase)
}

func supersedeProviderDrainFixtureWithKind(t *testing.T, fixture completionSettlementFixture, kind string, phase runtimemanager.AgentLifecyclePhase) runtimemanager.AgentLifecycleTransitionResult {
	t.Helper()
	result, err := commitProviderDrainTransition(fixture, kind, phase)
	if err != nil {
		t.Fatalf("supersede provider attempt (%s): %v", kind, err)
	}
	return result
}

func commitProviderDrainTransition(fixture completionSettlementFixture, kind string, phase runtimemanager.AgentLifecyclePhase) (runtimemanager.AgentLifecycleTransitionResult, error) {
	selected, ok := fixture.store.(agentfixture.Store)
	if !ok {
		return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("completion store %T does not support lifecycle transitions", fixture.store)
	}
	runMode := runtimemanager.AgentRunModeStopped
	if phase == runtimemanager.AgentLifecycleRunning {
		runMode = runtimemanager.AgentRunModeStandard
	}
	return agentfixture.Commit(testAuthorActivityContext(), selected, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: kind, RequestHash: "provider-drain-" + kind,
		Identity: fixture.authority.Normal.Identity, AgentID: fixture.agentID, Trigger: "provider_drain_test",
		ExpectedEpoch: fixture.authority.Normal.RuntimeEpoch, ExpectedGeneration: fixture.authority.Normal.Generation,
		ExpectedPhase: runtimemanager.AgentLifecycleRunning,
		TargetEpoch:   fixture.authority.Normal.RuntimeEpoch, TargetGeneration: fixture.authority.Normal.Generation + 1,
		TargetPhase: phase, ConfigRevision: "provider-drain-revision", RunMode: runMode, Now: time.Now().UTC(),
	})
}

func requireProviderDrainCount(t *testing.T, fixture completionSettlementFixture, attemptID string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM runtime_provider_attempt_drains WHERE attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT COUNT(*) FROM runtime_provider_attempt_drains WHERE attempt_id=$1::uuid`
	}
	var count int
	if err := fixture.db.QueryRow(query, attemptID).Scan(&count); err != nil || count != want {
		t.Fatalf("provider drain count=%d err=%v, want %d", count, err, want)
	}
}

func requireProviderAttemptCount(t *testing.T, fixture completionSettlementFixture, want int) {
	t.Helper()
	var count int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM runtime_external_effect_attempts`).Scan(&count); err != nil || count != want {
		t.Fatalf("provider attempt count=%d err=%v, want %d", count, err, want)
	}
}

func requireProviderDrainState(t *testing.T, fixture completionSettlementFixture, attemptID, want string) {
	t.Helper()
	query := `SELECT state FROM runtime_provider_attempt_drains WHERE attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT state FROM runtime_provider_attempt_drains WHERE attempt_id=$1::uuid`
	}
	var state string
	if err := fixture.db.QueryRow(query, attemptID).Scan(&state); err != nil || state != want {
		t.Fatalf("provider drain state=%q err=%v, want %q", state, err, want)
	}
}

func setProviderOriginLease(t *testing.T, fixture completionSettlementFixture, claim runtimedelivery.Claim, lease time.Time) {
	t.Helper()
	query := `UPDATE event_delivery_attempts SET lease_expires_at=? WHERE delivery_id=? AND claim_version=? AND claim_token=? AND open_marker=TRUE`
	if !fixture.sqlite {
		query = `UPDATE event_delivery_attempts SET lease_expires_at=$1 WHERE delivery_id=$2::uuid AND claim_version=$3 AND claim_token=$4::uuid AND open_marker=TRUE`
	}
	result, err := fixture.db.Exec(query, lease.UTC(), claim.DeliveryID(), claim.Version(), claim.PersistenceToken())
	if err != nil {
		t.Fatalf("set provider origin lease: %v", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		t.Fatalf("set provider origin lease rows=%d, want 1", rows)
	}
}

func expireProviderOriginLease(t *testing.T, fixture completionSettlementFixture, claim runtimedelivery.Claim) {
	t.Helper()
	expiresAt := time.Now().UTC().Add(-time.Second)
	startedAt := expiresAt.Add(-time.Minute)
	query := `UPDATE event_delivery_attempts SET started_at=?,lease_expires_at=? WHERE delivery_id=? AND claim_version=? AND claim_token=? AND open_marker=TRUE`
	if !fixture.sqlite {
		query = `UPDATE event_delivery_attempts SET started_at=$1,lease_expires_at=$2 WHERE delivery_id=$3::uuid AND claim_version=$4 AND claim_token=$5::uuid AND open_marker=TRUE`
	}
	result, err := fixture.db.Exec(query, startedAt, expiresAt, claim.DeliveryID(), claim.Version(), claim.PersistenceToken())
	if err != nil {
		t.Fatalf("expire provider origin lease: %v", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		t.Fatalf("expire provider origin lease rows=%d, want 1", rows)
	}
	deliveryQuery := `UPDATE event_deliveries SET started_at=? WHERE delivery_id=? AND status='in_progress' AND claim_version=?`
	if !fixture.sqlite {
		deliveryQuery = `UPDATE event_deliveries SET started_at=$1 WHERE delivery_id=$2::uuid AND status='in_progress' AND claim_version=$3`
	}
	result, err = fixture.db.Exec(deliveryQuery, startedAt, claim.DeliveryID(), claim.Version())
	if err != nil {
		t.Fatalf("align expired provider origin lifecycle start: %v", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		t.Fatalf("align expired provider origin lifecycle rows=%d, want 1", rows)
	}
}

func providerOriginLease(t *testing.T, fixture completionSettlementFixture, claim runtimedelivery.Claim) time.Time {
	t.Helper()
	query := `SELECT lease_expires_at FROM event_delivery_attempts WHERE delivery_id=? AND claim_version=? AND claim_token=?`
	if !fixture.sqlite {
		query = `SELECT lease_expires_at FROM event_delivery_attempts WHERE delivery_id=$1::uuid AND claim_version=$2 AND claim_token=$3::uuid`
	}
	return providerLeaseTime(t, fixture, query, claim.DeliveryID(), claim.Version(), claim.PersistenceToken())
}

func setProviderDrainLease(t *testing.T, fixture completionSettlementFixture, attemptID string, lease time.Time) {
	t.Helper()
	query := `UPDATE runtime_provider_attempt_drains SET expires_at=? WHERE attempt_id=? AND state='pending'`
	if !fixture.sqlite {
		query = `UPDATE runtime_provider_attempt_drains SET expires_at=$1 WHERE attempt_id=$2::uuid AND state='pending'`
	}
	result, err := fixture.db.Exec(query, lease.UTC(), attemptID)
	if err != nil {
		t.Fatalf("set provider drain lease: %v", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		t.Fatalf("set provider drain lease rows=%d, want 1", rows)
	}
}

func providerDrainLease(t *testing.T, fixture completionSettlementFixture, attemptID string) time.Time {
	t.Helper()
	query := `SELECT expires_at FROM runtime_provider_attempt_drains WHERE attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT expires_at FROM runtime_provider_attempt_drains WHERE attempt_id=$1::uuid`
	}
	return providerLeaseTime(t, fixture, query, attemptID)
}

func providerLeaseTime(t *testing.T, fixture completionSettlementFixture, query string, args ...any) time.Time {
	t.Helper()
	if fixture.sqlite {
		var lease conversationForkTimeValue
		if err := fixture.db.QueryRow(query, args...).Scan(&lease); err != nil || !lease.Valid {
			t.Fatalf("load SQLite provider lease: valid=%v err=%v", lease.Valid, err)
		}
		return lease.Time.UTC()
	}
	var lease time.Time
	if err := fixture.db.QueryRow(query, args...).Scan(&lease); err != nil {
		t.Fatalf("load PostgreSQL provider lease: %v", err)
	}
	return lease.UTC()
}

func replaceProviderOriginClaim(t *testing.T, fixture completionSettlementFixture, claim runtimedelivery.Claim) {
	t.Helper()
	deliveryStore, ok := fixture.store.(runtimedelivery.Store)
	if !ok {
		t.Fatalf("completion store %T does not expose delivery reclaim", fixture.store)
	}
	ctx := testAuthorActivityContext()
	snapshot, err := deliveryStore.Snapshot(ctx, claim.DeliveryID())
	if err != nil {
		t.Fatalf("load provider origin before reclaim: %v", err)
	}
	event := eventtest.ExistingRunRootIngress(
		snapshot.EventID, "completion.origin", "gateway", "", []byte(`{}`), 0,
		snapshot.RunID, events.EventEnvelope{}, snapshot.CreatedAt,
	)
	result, err := deliveryStore.ClaimDelivery(ctx, snapshot.Authority, event, snapshot.Route)
	if err != nil {
		t.Fatalf("reclaim provider origin: %v", err)
	}
	claimed, ok := result.Acquired()
	if !ok || claimed.Claim.Version() != claim.Version()+1 || claimed.Claim.Same(claim) {
		t.Fatalf("newer provider origin claim=%+v disposition=%s, want version %d", claimed.Claim, result.Disposition, claim.Version()+1)
	}
}

func requireAgentLifecycleState(t *testing.T, fixture completionSettlementFixture, wantPhase runtimemanager.AgentLifecyclePhase, wantGeneration uint64) {
	t.Helper()
	reader, ok := fixture.store.(runtimemanager.AgentLifecycleStateReader)
	if !ok {
		t.Fatalf("completion store %T does not expose lifecycle state", fixture.store)
	}
	state, found, err := reader.LoadAgentLifecycleState(testAuthorActivityContext(), fixture.authority.Normal.Identity)
	if err != nil || !found || state.Phase != wantPhase || state.Generation != wantGeneration {
		t.Fatalf("lifecycle state=%+v found=%v err=%v, want %s generation %d", state, found, err, wantPhase, wantGeneration)
	}
}

func requireAgentCoarseStatus(t *testing.T, fixture completionSettlementFixture, want string) {
	t.Helper()
	fields, err := fixture.authority.Normal.Identity.StorageFields()
	if err != nil {
		t.Fatalf("provider-drain identity storage fields: %v", err)
	}
	query := `SELECT status FROM agents WHERE agent_id=? AND agent_name_owner=? AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=? AND flow_instance_id=? AND flow_instance=?`
	args := []any{fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath}
	if !fixture.sqlite {
		query = `SELECT status FROM agents WHERE agent_id=$1 AND agent_name_owner=$2 AND agent_name_source=$3 AND agent_route_presence=$4 AND flow_scope_key=$5 AND flow_instance_id=$6 AND flow_instance=$7`
	}
	var status string
	if err := fixture.db.QueryRow(query, args...).Scan(&status); err != nil || status != want {
		t.Fatalf("agent coarse status=%q err=%v, want %q", status, err, want)
	}
}

func requireOriginDeliveryOutcome(t *testing.T, fixture completionSettlementFixture, wantState, wantReason string) {
	t.Helper()
	requireDeliveryClaimOutcome(t, fixture, fixture.origin, wantState, wantReason)
}

func requireTerminalizedOriginDelivery(t *testing.T, fixture completionSettlementFixture, wantReason string) {
	t.Helper()
	query := `
		SELECT d.status,d.claim_version,COALESCE(o.reason_code,''),
		       (SELECT COUNT(*) FROM event_delivery_outcomes all_o WHERE all_o.delivery_id=d.delivery_id),
		       (SELECT COUNT(*) FROM event_delivery_attempts a
		        WHERE a.delivery_id=d.delivery_id AND a.claim_version=? AND a.claim_token=?
		          AND a.open_marker=FALSE AND a.completed_at IS NOT NULL AND a.outcome='terminalized')
		FROM event_deliveries d
		JOIN event_delivery_outcomes o ON o.delivery_id=d.delivery_id AND o.claim_version=d.claim_version
		WHERE d.delivery_id=?`
	args := []any{fixture.origin.Version(), fixture.origin.PersistenceToken(), fixture.origin.DeliveryID()}
	if !fixture.sqlite {
		query = `
			SELECT d.status,d.claim_version,COALESCE(o.reason_code,''),
			       (SELECT COUNT(*) FROM event_delivery_outcomes all_o WHERE all_o.delivery_id=d.delivery_id),
			       (SELECT COUNT(*) FROM event_delivery_attempts a
			        WHERE a.delivery_id=d.delivery_id AND a.claim_version=$1 AND a.claim_token=$2::uuid
			          AND a.open_marker=FALSE AND a.completed_at IS NOT NULL AND a.outcome='terminalized')
			FROM event_deliveries d
			JOIN event_delivery_outcomes o ON o.delivery_id=d.delivery_id AND o.claim_version=d.claim_version
			WHERE d.delivery_id=$3::uuid`
	}
	var state, reason string
	var version int64
	var outcomes, interrupted int
	if err := fixture.db.QueryRow(query, args...).Scan(&state, &version, &reason, &outcomes, &interrupted); err != nil ||
		state != "dead_letter" || version != fixture.origin.Version()+1 || reason != wantReason || outcomes != 1 || interrupted != 1 {
		t.Fatalf("terminalized origin state=%q version=%d reason=%q outcomes=%d interrupted=%d err=%v", state, version, reason, outcomes, interrupted, err)
	}
}

func requireDeliveryClaimOutcome(t *testing.T, fixture completionSettlementFixture, claim runtimedelivery.Claim, wantState, wantReason string) {
	t.Helper()
	query := `SELECT d.status,COALESCE(o.reason_code,'') FROM event_deliveries d JOIN event_delivery_outcomes o ON o.delivery_id=d.delivery_id AND o.claim_version=? WHERE d.delivery_id=?`
	args := []any{claim.Version(), claim.DeliveryID()}
	if !fixture.sqlite {
		query = `SELECT d.status,COALESCE(o.reason_code,'') FROM event_deliveries d JOIN event_delivery_outcomes o ON o.delivery_id=d.delivery_id AND o.claim_version=$1 WHERE d.delivery_id=$2::uuid`
	}
	var state, reason string
	if err := fixture.db.QueryRow(query, args...).Scan(&state, &reason); err != nil || state != wantState || reason != wantReason {
		t.Fatalf("origin delivery state/reason=%q/%q err=%v, want %q/%q", state, reason, err, wantState, wantReason)
	}
}

func newProviderDrainOriginClaim(t *testing.T, fixture completionSettlementFixture, ordinal int) runtimedelivery.Claim {
	t.Helper()
	event := eventtest.ExistingRunRootIngress(
		uuid.NewString(), "completion.origin", "gateway", "", []byte(fmt.Sprintf(`{"ordinal":%d}`, ordinal)), 0,
		fixture.authority.Target.RunID, events.EventEnvelope{}, time.Now().UTC(),
	)
	route := events.DeliveryRoute{
		Recipient:     events.MustAgentDeliveryRecipient(fixture.agentID),
		AgentIdentity: fixture.authority.Normal.Identity,
	}
	if err := commitSemanticEventFixtureWithRoutes(testAuthorActivityContext(), fixture.store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("commit provider-drain origin %d: %v", ordinal, err)
	}
	claimed, err := claimDeliveryFixture(testAuthorActivityContext(), fixture.store.(deliveryFixtureStore), event, route)
	if err != nil {
		t.Fatalf("claim provider-drain origin %d: %v", ordinal, err)
	}
	return claimed.Claim
}

func newProviderDrainSiblingFixture(t *testing.T, fixture completionSettlementFixture) completionSettlementFixture {
	t.Helper()
	sibling := fixture
	sibling.sessionID = uuid.NewString()
	sibling.authority.Normal.Identity = testAgentIdentity(t, fixture.agentID, "sibling")
	sibling.authority.Normal.Generation = fixture.authority.Normal.Generation
	sibling.authority.Normal.RuntimeEpoch = fixture.authority.Normal.RuntimeEpoch
	sibling.authority.FenceGeneration = sibling.authority.Normal.Generation
	sibling.authority.Target.ID = uuid.NewString()
	sibling.authority.Target.AgentIdentity = sibling.authority.Normal.Identity
	sibling.authority.Target.FlowInstance = sibling.authority.Normal.Identity.FlowInstance()
	sibling.authority.Target.SessionID = sibling.sessionID
	sibling.authority.Target.EntityID = uuid.NewString()
	sibling.authority.BudgetScopes = []runtimeeffects.BudgetAdmissionScope{{Kind: "entity", Key: sibling.authority.Target.EntityID, CapUSD: 1}}
	fields, err := sibling.authority.Normal.Identity.StorageFields()
	if err != nil {
		t.Fatalf("same-slug sibling identity: %v", err)
	}
	now := time.Now().UTC()
	if fixture.sqlite {
		if _, err := fixture.db.Exec(`INSERT INTO agents (agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,role,model,llm_backend,memory_enabled,memory_source,status,lifecycle_runtime_epoch,lifecycle_generation,lifecycle_phase,created_at,topology_authority_kind,topology_admission,execution_lifetime) VALUES (?,?,?,?,?,?,?,'worker','regular','claude_cli',1,'authored','active',1,1,'running',?,'static_declaration_plan',?,'durable_managed')`,
			fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
			fields.FlowInstanceID, fields.FlowInstancePath, now, testAgentTopologyJSON(t)); err != nil {
			t.Fatalf("seed SQLite provider-drain sibling agent: %v", err)
		}
		if _, err := fixture.db.Exec(`INSERT INTO agent_sessions (session_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,memory_enabled,memory_source,conversation,turn_count,runtime_state,lease_holder,lease_expires_at,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,1,'authored','[]',0,?,?,?,'active',?,?)`,
			sibling.sessionID, sibling.authority.Target.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
			fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
			`{"provider_session_id":"provider-head-current"}`, sibling.leaseHolder, now.Add(10*time.Minute), now, now); err != nil {
			t.Fatalf("seed SQLite provider-drain sibling session: %v", err)
		}
	} else {
		if _, err := fixture.db.Exec(`INSERT INTO agents (agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,role,model,llm_backend,memory_enabled,memory_source,status,lifecycle_runtime_epoch,lifecycle_generation,lifecycle_phase,created_at,topology_authority_kind,topology_admission,execution_lifetime) VALUES ($1,$2,$3,$4,$5,$6,$7,'worker','regular','claude_cli',TRUE,'authored','active',1,1,'running',$8,'static_declaration_plan',$9::jsonb,'durable_managed')`,
			fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
			fields.FlowInstanceID, fields.FlowInstancePath, now, testAgentTopologyJSON(t)); err != nil {
			t.Fatalf("seed PostgreSQL provider-drain sibling agent: %v", err)
		}
		if _, err := fixture.db.Exec(`INSERT INTO agent_sessions (session_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,memory_enabled,memory_source,conversation,turn_count,runtime_state,lease_holder,lease_expires_at,status,created_at,updated_at) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,TRUE,'authored','[]'::jsonb,0,$10::jsonb,$11,$12,'active',$13,$13)`,
			sibling.sessionID, sibling.authority.Target.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
			fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
			`{"provider_session_id":"provider-head-current"}`, sibling.leaseHolder, now.Add(10*time.Minute), now); err != nil {
			t.Fatalf("seed PostgreSQL provider-drain sibling session: %v", err)
		}
	}
	sibling.origin = newProviderDrainOriginClaim(t, sibling, 100)
	return sibling
}

func requireProviderDrainAggregate(t *testing.T, fixture completionSettlementFixture, wantRows int, wantSpend float64) {
	t.Helper()
	var attempts, drains, turns, spendRows, reservations int
	var spend float64
	queries := []struct {
		query string
		dest  any
	}{
		{`SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE state='settled'`, &attempts},
		{`SELECT COUNT(*) FROM runtime_provider_attempt_drains WHERE state='settled'`, &drains},
		{`SELECT COUNT(*) FROM agent_turns`, &turns},
		{`SELECT COUNT(*) FROM spend_ledger`, &spendRows},
		{`SELECT COUNT(*) FROM runtime_effect_budget_reservations`, &reservations},
		{`SELECT COALESCE(SUM(cost_usd),0) FROM spend_ledger`, &spend},
	}
	for _, query := range queries {
		if err := fixture.db.QueryRow(query.query).Scan(query.dest); err != nil {
			t.Fatalf("provider-drain aggregate query %q: %v", query.query, err)
		}
	}
	if attempts != wantRows || drains != wantRows || turns != wantRows || spendRows != wantRows || reservations != 0 || spend != wantSpend {
		t.Fatalf("provider-drain aggregate attempts=%d drains=%d turns=%d spend_rows=%d reservations=%d spend=%.2f, want %d/%d/%d/%d/0/%.2f", attempts, drains, turns, spendRows, reservations, spend, wantRows, wantRows, wantRows, wantRows, wantSpend)
	}
}

func requireProviderDrainImmutableSnapshots(t *testing.T, fixture completionSettlementFixture, wantRows int) {
	t.Helper()
	var toolRows, eventRows int
	if fixture.sqlite {
		if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM agent_turns WHERE json_array_length(tool_calls)=1`).Scan(&toolRows); err != nil {
			t.Fatalf("count SQLite tool snapshots: %v", err)
		}
		if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM agent_turns WHERE json_array_length(emitted_events)=1`).Scan(&eventRows); err != nil {
			t.Fatalf("count SQLite event snapshots: %v", err)
		}
	} else {
		if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM agent_turns WHERE jsonb_array_length(tool_calls)=1`).Scan(&toolRows); err != nil {
			t.Fatalf("count PostgreSQL tool snapshots: %v", err)
		}
		if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM agent_turns WHERE jsonb_array_length(emitted_events)=1`).Scan(&eventRows); err != nil {
			t.Fatalf("count PostgreSQL event snapshots: %v", err)
		}
	}
	if toolRows != wantRows || eventRows != wantRows {
		t.Fatalf("immutable snapshot rows tools=%d events=%d, want %d/%d", toolRows, eventRows, wantRows, wantRows)
	}
}

func requireDeliveryClaimPending(t *testing.T, fixture completionSettlementFixture, claim runtimedelivery.Claim) {
	t.Helper()
	statusQuery := `SELECT status FROM event_deliveries WHERE delivery_id=?`
	outcomeQuery := `SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id=? AND claim_version=?`
	statusArgs := []any{claim.DeliveryID()}
	outcomeArgs := []any{claim.DeliveryID(), claim.Version()}
	if !fixture.sqlite {
		statusQuery = `SELECT status FROM event_deliveries WHERE delivery_id=$1::uuid`
		outcomeQuery = `SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id=$1::uuid AND claim_version=$2`
	}
	var status string
	var outcomes int
	if err := fixture.db.QueryRow(statusQuery, statusArgs...).Scan(&status); err != nil {
		t.Fatalf("load pending origin status: %v", err)
	}
	if err := fixture.db.QueryRow(outcomeQuery, outcomeArgs...).Scan(&outcomes); err != nil {
		t.Fatalf("count pending origin outcomes: %v", err)
	}
	if status != "in_progress" || outcomes != 0 {
		t.Fatalf("origin after rollback status=%q outcomes=%d, want in_progress/0", status, outcomes)
	}
}

type providerDrainFailureBoundary struct {
	name      string
	table     string
	operation string
	predicate string
}

func installProviderDrainFailureBoundary(t *testing.T, fixture completionSettlementFixture, boundary providerDrainFailureBoundary) {
	t.Helper()
	const triggerName = "provider_drain_rollback_proof"
	if fixture.sqlite {
		when := ""
		if boundary.predicate != "" {
			when = " WHEN " + boundary.predicate
		}
		statement := fmt.Sprintf(
			`CREATE TRIGGER %s BEFORE %s ON %s%s BEGIN SELECT RAISE(ABORT, 'provider drain rollback proof'); END`,
			triggerName, boundary.operation, boundary.table, when,
		)
		if _, err := fixture.db.Exec(statement); err != nil {
			t.Fatalf("install SQLite %s failure boundary: %v", boundary.name, err)
		}
		return
	}
	if _, err := fixture.db.Exec(`CREATE FUNCTION provider_drain_rollback_proof() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'provider drain rollback proof'; END $$`); err != nil {
		t.Fatalf("install PostgreSQL %s failure function: %v", boundary.name, err)
	}
	when := ""
	if boundary.predicate != "" {
		when = " WHEN (" + boundary.predicate + ")"
	}
	statement := fmt.Sprintf(
		`CREATE TRIGGER %s BEFORE %s ON %s FOR EACH ROW%s EXECUTE FUNCTION provider_drain_rollback_proof()`,
		triggerName, boundary.operation, boundary.table, when,
	)
	if _, err := fixture.db.Exec(statement); err != nil {
		t.Fatalf("install PostgreSQL %s failure boundary: %v", boundary.name, err)
	}
}
