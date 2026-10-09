package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type emitReadCancellationFixture struct {
	completionSettlementFixture
	ctx       context.Context
	turnCtx   context.Context
	turn      *effects.TurnExecution
	handle    *effects.Handle
	publisher *bus.EventBus
	event     events.Event
	instance  flowidentity.RunScopedFlowInstance
	header    pipeline.WorkflowEngineStateRecord
	probe     *operatorSnapshotProbe
	consumer  worklifetime.InternalSubscription
}

func newEmitReadCancellationFixture(t *testing.T, backend string) emitReadCancellationFixture {
	t.Helper()
	probe := &operatorSnapshotProbe{readOnlyGate: true}
	selected, db, _ := newStopCommitStore(t, backend, probe)
	store := selected.(completionSettlementTestStore)
	fixture := newCompletionSettlementFixtureForFlow(t, store, db, backend == "sqlite", agentmemory.Plan{}, "")
	ctx := effects.WithTurnTimeout(fixture.contextFor(fixture.authority), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
	ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "anthropic_api")
	turnCtx, turn := effects.WithTurnExecution(ctx)
	t.Cleanup(func() {
		if _, err := turn.Finish(); err != nil {
			t.Error(err)
		}
	})
	handle := beginObservedCompletionForSettlementTest(t, turnCtx, "anthropic_api", "native-emit-read")
	runID := fixture.authority.Target.RunID
	instance := flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute(".", runID, runID)}
	header := seedTurnTerminationHeader(t, fixture, instance, runID, time.Now().UTC().Truncate(time.Microsecond))
	registerTestAuthorActivityCatalogForContext(t, selected.(testAuthorActivityCatalogRegistrar), turnCtx)
	publisher, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore))
	if err != nil {
		t.Fatal(err)
	}
	parent := managedCompletionTestEvent(fixture.authority)
	event := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "test.node_emitted", eventtest.Producer(events.EventProducerAgent, fixture.agentID), "store-test", json.RawMessage(`{ "request": "store-test" }`), 1, events.LineageFromEvent(parent), events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
	consumer, err := publisher.SubscribeInternal(ctx, "emit-read-witness", event.Type())
	if err != nil {
		t.Fatal(err)
	}
	consumer.MarkReady()
	t.Cleanup(func() {
		if err := consumer.Complete(false); err != nil {
			t.Error(err)
		}
	})
	return emitReadCancellationFixture{completionSettlementFixture: fixture, ctx: ctx, turnCtx: turnCtx, turn: turn, handle: handle, publisher: publisher, event: event, instance: instance, header: header, probe: probe, consumer: consumer}
}

func (f emitReadCancellationFixture) receiveDispatch(t *testing.T) {
	t.Helper()
	select {
	case delivery := <-f.consumer.Deliveries():
		if delivery == nil || delivery.ID() != f.event.ID() {
			t.Fatal("committed emit dispatched another occurrence")
		}
		if err := delivery.Complete(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("committed emit did not dispatch its owned internal carrier")
	}
}

func (f emitReadCancellationFixture) cancel(t *testing.T, reason string) effects.TurnCancellation {
	t.Helper()
	if reason == "turn_timeout" {
		clock, found := f.handle.LogicalTurnClock()
		if !found {
			t.Fatal("turn cancellation omitted its first-launch clock")
		}
		intent, err := f.store.(effects.TurnLifetimeStore).RequestTurnTimeout(f.ctx, f.handle.Attempt(), clock.DeadlineAt)
		if err != nil || intent.ValidateIntent() != nil {
			t.Fatalf("timeout intent was not acknowledged: %+v err=%v", intent, err)
		}
		return intent
	}
	cause := managedCompletionTestEventWithIdentity(f.authority, uuid.NewString(), "completion.test.requested")
	node, err := identity.AdmitExecutableNodeDeclaration(".", "router")
	if err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: f.instance.RunID, EntityID: f.header.EntityID})}
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, cause, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(f.ctx, f.store.(deliveryFixtureStore), cause, route)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, turnTerminationCommandForClaim(t, f.header, cause, claimed.Claim, time.Now().UTC().Truncate(time.Microsecond)))
	if err != nil || !committed.Committed || len(committed.Lifecycle.TurnCancellations) != 1 {
		t.Fatalf("termination intent was not acknowledged: %+v err=%v", committed, err)
	}
	return committed.Lifecycle.TurnCancellations[0]
}

func TestWorkflowEmitReadCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, reader := range []string{"emit-stage", "emit-feedback"} {
			for _, reason := range []string{"turn_timeout", "terminate"} {
				t.Run(fmt.Sprintf("%s/%s/%s", backend, reader, reason), func(t *testing.T) {
					f := newEmitReadCancellationFixture(t, backend)
					request := pipeline.WorkflowPublicationStageRequest{Instance: f.instance, EntityID: f.header.EntityID}
					var original pipeline.WorkflowEmitResult
					if reader == "emit-feedback" {
						var err error
						original, err = f.publisher.PublishEmit(f.turnCtx, f.event, request)
						if err != nil || !original.Accepted || !original.FeedbackCommitted {
							t.Fatalf("initial emit was not acknowledged: %+v err=%v", original, err)
						}
						f.receiveDispatch(t)
					}
					baseline := f.db.Stats().InUse
					workBaseline := storeTestWorkOwner(t).ActiveCount()
					entered, release := f.probe.arm(reader, 1, nil, nil)
					defer release()
					defer f.probe.disarm()
					response := make(chan struct {
						result pipeline.WorkflowEmitResult
						err    error
					}, 1)
					go func() {
						result, err := f.publisher.PublishEmit(f.turnCtx, f.event, request)
						response <- struct {
							result pipeline.WorkflowEmitResult
							err    error
						}{result, err}
					}()
					select {
					case <-entered:
					case <-time.After(3 * time.Second):
						t.Fatal("emit did not reach its native read boundary")
					}
					if reader == "emit-stage" {
						f.receiveDispatch(t)
					}
					intent := f.cancel(t, reason)
					matched, err := f.turn.RequestCancellation(intent)
					if err != nil || !matched {
						t.Fatalf("exact turn intent did not cancel its reader: %t %v", matched, err)
					}
					select {
					case returned := <-response:
						if !errors.Is(returned.err, context.Canceled) || !returned.result.Accepted || returned.result.FeedbackCommitted {
							t.Fatalf("canceled read changed acceptance or fabricated feedback: %+v %v", returned.result, returned.err)
						}
					case <-time.After(3 * time.Second):
						t.Fatal("canceled emit read did not join")
					}
					f.probe.disarm()
					f.probe.mu.Lock()
					active, outside, writes := f.probe.active, f.probe.outside, f.probe.writes
					f.probe.mu.Unlock()
					if active != 0 || outside != 0 || writes != 0 || f.db.Stats().InUse != baseline {
						t.Fatalf("canceled snapshot escaped or leaked: active=%d outside=%d writes=%d connections=%d baseline=%d", active, outside, writes, f.db.Stats().InUse, baseline)
					}
					if actual := storeTestWorkOwner(t).ActiveCount(); actual != workBaseline {
						t.Fatalf("canceled emit leaked runtime work: active=%d baseline=%d", actual, workBaseline)
					}
					select {
					case duplicate := <-f.consumer.Deliveries():
						if duplicate != nil {
							_ = duplicate.Complete()
						}
						t.Fatal("canceled feedback replay dispatched the occurrence twice")
					default:
					}
					stages, found, err := f.store.(pipeline.WorkflowEmitFeedbackOwner).ReadWorkflowPublicationStages(f.ctx, f.event.ID(), f.instance)
					if err != nil || !found || stages.Acceptance.EventID() != f.event.ID() {
						t.Fatalf("cancellation lost committed acceptance: %+v %t %v", stages, found, err)
					}
					if reader == "emit-feedback" {
						feedback, found, err := f.store.(pipeline.WorkflowEmitFeedbackOwner).ReadWorkflowEmitFeedback(f.ctx, f.event.ID(), f.instance)
						if err != nil || !found || feedback != original.Feedback {
							t.Fatalf("canceled replay changed immutable feedback: %+v %t %v", feedback, found, err)
						}
					}
					failure := failures.FromError(context.Canceled, "emit-read-test", "physical_join").Failure
					if err := f.handle.Settle(f.ctx, effects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true, "observed_response": "kept"}); err != nil {
						t.Fatal(err)
					}
					command := effects.CanceledTurnCommandForAttempt(f.handle.Attempt(), nil)
					if reason == "turn_timeout" {
						turns, err := f.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(f.ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
						if err != nil || len(turns) != 1 || !turns[0].Attempt.Origin.Same(f.handle.Attempt().Origin) {
							t.Fatalf("timeout recovery lost its original origin: %+v %v", turns, err)
						}
						plan, err := f.publisher.PrepareTurnTimeoutReaction(f.ctx, turns[0])
						if err != nil {
							t.Fatal(err)
						}
						defer func() {
							if err := f.publisher.ReleaseTurnTimeoutReaction(f.ctx, plan); err != nil {
								t.Error(err)
							}
						}()
						command.Publication = plan
					}
					settled, err := f.store.(effects.CanceledTurnStore).CommitCanceledTurn(f.ctx, command)
					if err != nil || settled.Validate() != nil || settled.Delivery.ReasonCode != reason || settled.Delivery.Status != deliverylifecycle.StatusCanceled {
						t.Fatalf("canceled reader lost exact origin settlement: %+v %v", settled, err)
					}
				})
			}
		}
	}
}

func TestCanceledTurnStartupReadCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newEmitReadCancellationFixture(t, backend)
			intent := f.cancel(t, "turn_timeout")
			failure := failures.FromError(context.Canceled, "startup-read-test", "physical_join").Failure
			if err := f.handle.Settle(f.ctx, effects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true}); err != nil {
				t.Fatal(err)
			}
			startupCtx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			baseline := f.db.Stats().InUse
			entered, release := f.probe.arm("canceled-turn", 1, nil, nil)
			defer release()
			defer f.probe.disarm()
			type recoveryResult struct {
				turns []effects.TurnExecutionResult
				err   error
			}
			response := make(chan recoveryResult, 1)
			request := liveExternalEffectRecoveryRequest(time.Now().UTC())
			go func() {
				turns, err := f.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(startupCtx, request)
				response <- recoveryResult{turns: turns, err: err}
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("startup snapshot did not reach the native read boundary")
			}
			cancel()
			select {
			case returned := <-response:
				if !errors.Is(returned.err, context.Canceled) || len(returned.turns) != 0 {
					t.Fatalf("canceled startup reported successful recovery: %+v %v", returned.turns, returned.err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("canceled startup snapshot did not join")
			}
			f.probe.disarm()
			f.probe.mu.Lock()
			active, outside, writes := f.probe.active, f.probe.outside, f.probe.writes
			f.probe.mu.Unlock()
			if active != 0 || outside != 0 || writes != 0 || f.db.Stats().InUse != baseline {
				t.Fatalf("startup snapshot escaped or leaked: active=%d outside=%d writes=%d connections=%d baseline=%d", active, outside, writes, f.db.Stats().InUse, baseline)
			}
			turns, err := f.store.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(f.ctx, request)
			if err != nil || len(turns) != 1 || !turns[0].Attempt.Origin.Same(intent.Origin) {
				t.Fatalf("canceled startup changed the pending recovery: %+v %v", turns, err)
			}
		})
	}
}

func TestWorkflowEmitRootCancellationFixtureBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, _ := newStopCommitStore(t, backend)
			store := selected.(completionSettlementTestStore)
			fixture := newCompletionSettlementFixtureForFlow(t, store, db, backend == "sqlite", agentmemory.Plan{}, "")
			ctx := effects.WithTurnTimeout(fixture.contextFor(fixture.authority), &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.node_emitted"})
			ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "anthropic_api")
			turnCtx, turn := effects.WithTurnExecution(ctx)
			defer func() { _, _ = turn.Finish() }()
			handle := beginObservedCompletionForSettlementTest(t, turnCtx, "anthropic_api", "native-emit-read")
			if _, found := handle.LogicalTurnClock(); !found {
				t.Fatal("native turn fixture omitted its launch clock")
			}
			runID := fixture.authority.Target.RunID
			instance := flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute(".", runID, runID)}
			seedTurnTerminationHeader(t, fixture, instance, runID, time.Now().UTC().Truncate(time.Microsecond))
			registerTestAuthorActivityCatalogForContext(t, selected.(testAuthorActivityCatalogRegistrar), turnCtx)
			publisher, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore))
			if err != nil {
				t.Fatal(err)
			}
			parent := managedCompletionTestEvent(fixture.authority)
			event := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "test.node_emitted", eventtest.Producer(events.EventProducerAgent, fixture.agentID), "store-test", json.RawMessage(`{ "request": "store-test" }`), 1, events.LineageFromEvent(parent), events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
			result, err := publisher.PublishEmit(turnCtx, event, pipeline.WorkflowPublicationStageRequest{Instance: instance, EntityID: runID})
			if err != nil || !result.Accepted || !result.FeedbackCommitted {
				t.Fatalf("native root emit fixture is not admitted: %+v err=%v", result, err)
			}
		})
	}
}
