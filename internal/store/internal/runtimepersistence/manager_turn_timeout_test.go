package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

// This executes the real Manager and selected-store directive owners with a
// controlled physical primitive, not a paid provider or public-launcher journey.
func TestManagerOwnsDirectiveLaunchTimeoutAndReactionBothStores(t *testing.T) {
	forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
		store := backend.store.(completionControllerTestStore)
		if backend.name == "postgres" {
			registerTestAuthorActivityCatalog(t, backend.store.(testAuthorActivityCatalogRegistrar))
		}
		agent := &directiveAmbiguityAgent{id: "manager-timeout-agent", effects: store}
		var timeoutID string
		agent.onBoard = func(ctx context.Context, directive agentcontrol.BoardDirective) (string, error) {
			token, found := runtimeeffects.LifecycleTokenFromContext(ctx)
			if !found {
				t.Fatal("Manager did not retain exact execution authority")
			}
			authority := runtimeeffects.NormalAgentAuthority(token, "manager-timeout-provider", time.Now().UTC().Add(time.Minute))
			authority.Target = runtimeeffects.UsageTarget{
				Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: directive.Event.RunID(), AgentID: agent.id,
				AgentIdentity: token.Identity, FlowInstance: token.Identity.FlowInstance(), SessionID: uuid.NewString(),
			}
			ctx = runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), runtimeeffects.NewCompletionController(store, store, store, nil).WithExecutionPosture(executionposture.Live))
			ctx = runtimeeffects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Nanosecond, Emit: "test.node_emitted"})
			ctx = withManagedCompletionTestSurface(t, ctx, authority, "anthropic_api")
			handle, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("manager-owned-timeout"))
			if err != nil {
				t.Errorf("controlled provider admission: %v", err)
				return "", err
			}
			if err := handle.MarkLaunched(ctx); err != nil {
				t.Errorf("controlled provider launch: %v", err)
				return "", err
			}
			clock, found := handle.LogicalTurnClock()
			if !found {
				t.Fatal("actual Manager turn has no first-launch clock")
			}
			timeoutID = clock.TimeoutEvent
			requireAuthoredTurnTimeout(t, ctx)
			failure := runtimefailures.FromError(context.Canceled, "manager-timeout-test", "physical_join").Failure
			// Join/record the accepted physical primitive before returning to the
			// Manager's cancellation consumer. No workflow state is written.
			settleErr := handle.Settle(context.WithoutCancel(ctx), runtimeeffects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true})
			return "", errors.Join(context.Canceled, settleErr)
		}
		harness := newDirectiveAmbiguityHarness(t, backend, agent)
		_, err := harness.manager.SendDirective(harness.workContext(t), harness.request)
		if !errors.Is(err, agentcontrol.ErrDirectiveCanceled) {
			t.Fatalf("Manager did not return typed canceled evidence: %v", err)
		}
		operation := harness.loadOperation(t)
		if operation.State != agentcontrol.DirectiveOperationCanceled || operation.CancellationReason != deliverylifecycle.CancellationTurnTimeout ||
			len(operation.Response) != 0 || operation.Failure != nil || agent.calls.Load() != 1 {
			t.Fatalf("actual Manager cancellation evidence: %+v calls=%d", operation, agent.calls.Load())
		}
		var reactions int
		if err := backend.db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id=$1 AND event_name='test.node_emitted'`, timeoutID).Scan(&reactions); err != nil || reactions != 1 {
			t.Fatalf("actual timeout reaction count=%d err=%v", reactions, err)
		}
		_, err = harness.manager.SendDirective(harness.workContext(t), harness.request)
		if !errors.Is(err, agentcontrol.ErrDirectiveCanceled) || agent.calls.Load() != 1 {
			t.Fatalf("same-key canceled directive reexecuted: calls=%d err=%v", agent.calls.Load(), err)
		}
	})
}

func TestManagerConsumesCommittedTerminationAndJoinsRealDirectiveBothStores(t *testing.T) {
	for _, phase := range []string{"unstarted", "prelaunch", "launched"} {
		t.Run(phase, func(t *testing.T) {
			forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
				store := backend.store.(completionControllerTestStore)
				if backend.name == "postgres" {
					registerTestAuthorActivityCatalog(t, backend.store.(testAuthorActivityCatalogRegistrar))
				}
				agent := &directiveAmbiguityAgent{id: "manager-terminate-agent", effects: store}
				var harness *directiveAmbiguityHarness
				agent.onBoard = func(ctx context.Context, directive agentcontrol.BoardDirective) (string, error) {
					token, found := runtimeeffects.LifecycleTokenFromContext(ctx)
					if !found {
						t.Fatal("termination invocation lost exact Manager lease")
					}
					authority := runtimeeffects.NormalAgentAuthority(token, "manager-termination-provider", time.Now().UTC().Add(time.Minute))
					authority.Target = runtimeeffects.UsageTarget{Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: directive.Event.RunID(), AgentID: agent.id,
						AgentIdentity: token.Identity, FlowInstance: token.Identity.FlowInstance(), SessionID: uuid.NewString()}
					ctx = runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), runtimeeffects.NewCompletionController(store, store, store, nil).WithExecutionPosture(executionposture.Live))
					ctx = withManagedCompletionTestSurface(t, ctx, authority, "anthropic_api")
					var handle *runtimeeffects.Handle
					if phase != "unstarted" {
						var err error
						handle, err = beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("manager-owned-termination"))
						if err != nil {
							t.Fatal(err)
						}
					}
					if phase == "launched" {
						if err := handle.MarkLaunched(ctx); err != nil {
							t.Fatal(err)
						}
					}
					scope, instanceID, path, _ := token.Identity.Route.Fields()
					owner := flowidentity.RunScopedFlowInstance{RunID: directive.Event.RunID(), Route: flowidentity.StoredRoute(scope, instanceID, path)}
					entity, at := uuid.NewString(), time.Now().UTC().Truncate(time.Microsecond)
					record := seedTurnTerminationHeader(t, completionSettlementFixture{store: backend.store.(completionSettlementTestStore)}, owner, entity, at)
					node, err := identity.AdmitExecutableNodeDeclaration(scope, "termination-router")
					if err != nil {
						t.Fatal(err)
					}
					event := managedCompletionTestEventWithIdentity(authority, uuid.NewString(), "test.node_emitted")
					route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{
						FlowID: scope, FlowInstance: path, EntityID: entity,
					})}
					if err := commitSemanticEventFixtureWithRoutes(ctx, backend.store.(completionSettlementTestStore), event, []events.DeliveryRoute{route}); err != nil {
						t.Fatal(err)
					}
					claimed, err := claimDeliveryFixture(ctx, backend.store.(deliveryFixtureStore), event, route)
					if err != nil {
						t.Fatal(err)
					}
					committed, err := backend.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, turnTerminationCommandForClaim(t, record, event, claimed.Claim, at))
					if err != nil || !committed.Committed || len(committed.Lifecycle.TurnCancellations) != 1 {
						t.Fatalf("guarded termination commit: %+v err=%v", committed, err)
					}
					if err := harness.manager.ApplyCommittedTurnCancellations(ctx, committed.Lifecycle.TurnCancellations); err != nil || ctx.Err() == nil {
						t.Fatalf("actual Manager lease missed termination: cause=%v err=%v", context.Cause(ctx), err)
					}
					var authored *runtimeeffects.AuthoredTurnCancellationError
					if !errors.As(context.Cause(ctx), &authored) || authored.Cancellation.Reason != deliverylifecycle.CancellationTerminate || authored.Cancellation.CauseEvent != event.ID() {
						t.Fatal("Manager substituted termination reason or cause")
					}
					if phase == "unstarted" {
						if _, err := beginManagedCompletionForTest(t, context.WithoutCancel(ctx), "anthropic_api", []byte("after-unstarted-termination")); err == nil {
							t.Fatal("canceled unstarted directive admitted a provider attempt")
						}
						return "", context.Canceled
					}
					failure := runtimefailures.FromError(context.Canceled, "manager-termination-test", "physical_cleanup").Failure
					state := runtimeeffects.StateOutcomeUncertain
					if phase == "prelaunch" {
						state = runtimeeffects.StateTerminalFailure
					}
					settleErr := handle.Settle(context.WithoutCancel(ctx), state, &failure, map[string]any{"physical_joined": true, "launch_rejected": phase == "prelaunch"})
					return "", errors.Join(context.Canceled, settleErr)
				}
				harness = newDirectiveAmbiguityHarness(t, backend, agent)
				_, err := harness.manager.SendDirective(harness.workContext(t), harness.request)
				operation := harness.loadOperation(t)
				if !errors.Is(err, agentcontrol.ErrDirectiveCanceled) || operation.State != agentcontrol.DirectiveOperationCanceled || operation.CancellationReason != deliverylifecycle.CancellationTerminate || operation.Failure != nil {
					t.Fatalf("real Manager termination settlement: %+v err=%v", operation, err)
				}
				_, err = harness.manager.SendDirective(harness.workContext(t), harness.request)
				if !errors.Is(err, agentcontrol.ErrDirectiveCanceled) || agent.calls.Load() != 1 {
					t.Fatalf("same-key terminated directive repeated provider work: calls=%d err=%v", agent.calls.Load(), err)
				}
			})
		})
	}
}
