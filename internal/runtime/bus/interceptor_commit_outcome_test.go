package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

type outcomeTestInterceptor struct {
	outcome runtimepipelineobligation.ExecutionOutcome
	emitted []events.Event
	err     error
	calls   int
}

type interceptorOutcomePipelineOwner struct {
	*terminalReleasePipelineOwner
	settlements map[string]runtimepipelineobligation.Disposition
}

func (o *interceptorOutcomePipelineOwner) Settle(_ context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	o.settlements[claim.EventID()] = disposition
	return runtimepipelineobligation.CommittedSettlement(disposition.Successful()), nil
}

type interceptorOutcomeRoute struct {
	parent string
	child  events.Event
	err    error
	calls  int
}

func (*interceptorOutcomeRoute) Intercept(context.Context, events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	return true, nil, runtimepipelineobligation.Continue(), nil
}

func (i *interceptorOutcomeRoute) InterceptDeliveryRoute(_ context.Context, delivery events.DeliveryEvent, _ events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if delivery.Event().ID() != i.parent {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	i.calls++
	return false, []events.Event{i.child}, runtimepipelineobligation.ExecutionOutcome{Committed: true}, i.err
}

func interceptorOutcomeBus(t *testing.T, store *targetRouteMemoryStore, interceptors ...EventInterceptor) (*EventBus, *interceptorOutcomePipelineOwner) {
	t.Helper()
	owner := &interceptorOutcomePipelineOwner{
		terminalReleasePipelineOwner: newTerminalReleasePipelineOwner(),
		settlements:                  make(map[string]runtimepipelineobligation.Disposition),
	}
	bus, err := newScopedTestEventBus(store, EventBusOptions{PipelineObligations: owner, Interceptors: interceptors})
	if err != nil {
		t.Fatal(err)
	}
	return bus, owner
}

func stageInterceptorOutcome(t *testing.T, bus *EventBus, event events.Event, outcome EventAppendOutcome, proofs ...runtimedelivery.DurableHandoffProof) {
	t.Helper()
	claim, err := bus.claimPipelinePublication(context.Background(), event.ID())
	if err != nil {
		t.Fatal(err)
	}
	bus.stageCommittedOutboxOperation(runtimeengine.EmitIntent{Event: event}, outcome, claim, proofs)
}

func assertInterceptorOutcomeDrained(t *testing.T, bus *EventBus, events ...events.Event) {
	t.Helper()
	bus.mu.RLock()
	defer bus.mu.RUnlock()
	for _, event := range events {
		if count := len(bus.pendingOutboxByID[event.ID()]); count != 0 {
			t.Errorf("event %s retains %d committed operations", event.ID(), count)
		}
	}
}

// These are bus-boundary injections over existing prepared-event fixtures, not
// SQL COMMIT or process-death proofs. Exact-duplicate controls must only drain
// the staged operation, never append or execute the mutation again.
func TestInterceptorCommitForegroundDrainsBeforeTerminalExit(t *testing.T) {
	for _, stop := range []string{"terminal", "retry", "unrelated_error"} {
		t.Run(stop, func(t *testing.T) {
			store := newTargetRouteMemoryStore()
			child := receiverProjectionEvent("foreground-child")
			seedCommittedNoDeliveryForTest(t, store, child)
			cleanupErr := errors.New("acknowledged interceptor cleanup")
			laterErr := errors.New("independent later interceptor failure")
			first := &outcomeTestInterceptor{outcome: runtimepipelineobligation.ExecutionOutcome{Committed: true}, emitted: []events.Event{child}, err: cleanupErr}
			last := &outcomeTestInterceptor{}
			switch stop {
			case "terminal":
				last.outcome = runtimepipelineobligation.DeadLetterExecution("later_terminal", eventBusFailure(laterErr, "test_later_interceptor"))
			case "retry":
				last.outcome = runtimepipelineobligation.ReleaseForRetry("later_not_ready", nil)
			case "unrelated_error":
				last.err = laterErr
			}
			bus, owner := interceptorOutcomeBus(t, store, first, last)
			stageInterceptorOutcome(t, bus, child, EventAppendExactDuplicate)
			parent := receiverProjectionEvent("foreground-parent")
			claim, err := bus.claimPipelinePublication(context.Background(), parent.ID())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := claim.Release(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			projection, err := bus.receiverProjection(context.Background(), parent.DeliveryContext())
			if err != nil {
				t.Fatal(err)
			}
			err = bus.completeCommittedPublishDispatch(context.Background(), parent, RoutePlan{}, claim, projection)
			if !errors.Is(err, cleanupErr) {
				t.Errorf("lost committed cleanup error: %v", err)
			}
			if stop == "unrelated_error" && !errors.Is(err, laterErr) {
				t.Errorf("lost independent failure: %v", err)
			}
			settled, found := owner.settlements[parent.ID()]
			if stop == "retry" {
				if found {
					t.Errorf("retry was terminally settled: %+v", settled)
				}
			} else if !found || settled.Successful() {
				t.Errorf("independent failure promoted to success: found=%t disposition=%+v", found, settled)
			} else if stop == "terminal" {
				want, _ := last.outcome.Disposition()
				if settled.Kind() != want.Kind() || settled.ReasonCode() != want.ReasonCode() {
					t.Errorf("later terminal disposition overwritten: got=%+v want=%+v", settled, want)
				}
			}
			assertInterceptorOutcomeDrained(t, bus, child)
			if owner.releaseCalls[child.ID()] != 1 || first.calls != 1 || last.calls != 1 {
				t.Errorf("release/first/last calls = %d/%d/%d, want 1/1/1", owner.releaseCalls[child.ID()], first.calls, last.calls)
			}
		})
	}
}

func TestInterceptorCommitContinuationRetainsErrorAndHandoff(t *testing.T) {
	store := newTargetRouteMemoryStore()
	child := receiverProjectionEvent("committed-error-child")
	seedCommittedNoDeliveryForTest(t, store, child)
	parent := deliveryContinuationProjectionEvent("committed-error-parent", "custom.continuation_parent")
	cleanupErr := errors.New("committed route cleanup")
	interceptor := &interceptorOutcomeRoute{parent: parent.ID(), child: child, err: cleanupErr}
	bus, owner := interceptorOutcomeBus(t, store, interceptor)
	stageInterceptorOutcome(t, bus, child, EventAppendInserted)
	route := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "workflow-node")),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowInstance: "root"}),
	}
	result := bus.DispatchDeliveryContinuation(hostilePublisherContext(t), parent, route)
	if err := result.Validate(); err != nil || result.Disposition() != runtimedeliverycontinuation.DispatchFatal || !errors.Is(result.Failure(), cleanupErr) {
		t.Errorf("continuation result=%d failure=%v validation=%v", result.Disposition(), result.Failure(), err)
	}
	assertInterceptorOutcomeDrained(t, bus, child)
	if settlement, ok := owner.settlements[child.ID()]; !ok || !settlement.Successful() {
		t.Errorf("committed child was not dispatched and settled: found=%t settlement=%+v", ok, settlement)
	}
	if interceptor.calls != 1 {
		t.Errorf("parent mutation calls=%d, want 1", interceptor.calls)
	}
}

func TestInterceptorCommitIncompleteDeliveryPreservesReleaseError(t *testing.T) {
	for _, failRelease := range []bool{false, true} {
		name := "healthy_release"
		if failRelease {
			name = "independent_release_failure"
		}
		t.Run(name, func(t *testing.T) {
			store := newTargetRouteMemoryStore()
			child := receiverProjectionEvent(name)
			seedCommittedNoDeliveryForTest(t, store, child)
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("missing-child-agent"), AgentIdentity: testAgentRouteIdentity(t, "missing-child-agent", "")}
			ledger, err := events.NewConnectEvaluationLedger(nil)
			if err != nil {
				t.Fatal(err)
			}
			settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
			if err != nil {
				t.Fatal(err)
			}
			store.settlements[child.ID()] = settlement
			store.routes[child.ID()] = []events.DeliveryRoute{route}
			bus, owner := interceptorOutcomeBus(t, store)
			continuations := &continuationCommittedHandoffOwner{}
			if err := bus.SetDeliveryContinuationOwner(continuations); err != nil {
				t.Fatal(err)
			}
			routeIdentity, err := route.Identity()
			if err != nil {
				t.Fatal(err)
			}
			proof, err := runtimedelivery.AdmitDurableHandoffProof(uuid.NewString(), child.ID(), events.EncodeDeliveryRouteIdentity(routeIdentity), bus.deliveryAuthority)
			if err != nil {
				t.Fatal(err)
			}
			releaseErr := errors.New("independent publication claim release failure")
			if failRelease {
				owner.releaseError[child.ID()] = func(int) error { return releaseErr }
			}
			stageInterceptorOutcome(t, bus, child, EventAppendInserted, proof)
			err = (engineDispatcher{bus: bus}).dispatchCommittedInterceptorPublications(context.Background(), []events.Event{child})
			if failRelease {
				if !errors.Is(err, releaseErr) || !errors.Is(err, errAuthoritativeDeliveryIncomplete) {
					t.Errorf("joined delivery/release error lost: %v", err)
				}
			} else if err != nil {
				t.Errorf("healthy transferred incomplete delivery: %v", err)
			}
			if len(continuations.proofs) != 1 || continuations.proofs[0].DeliveryID() != proof.DeliveryID() {
				t.Errorf("exact child handoff not transferred: %+v", continuations.proofs)
			}
			if owner.releaseCalls[child.ID()] != 1 {
				t.Errorf("claim release calls=%d, want 1", owner.releaseCalls[child.ID()])
			}
			if _, settled := owner.settlements[child.ID()]; settled {
				t.Error("incomplete child delivery was incorrectly terminally settled")
			}
			assertInterceptorOutcomeDrained(t, bus, child)
		})
	}
}

func TestInterceptorCommitFirstDeferredFailureStillDrainsSecond(t *testing.T) {
	store := newTargetRouteMemoryStore()
	first, second := receiverProjectionEvent("first"), receiverProjectionEvent("second")
	seedCommittedNoDeliveryForTest(t, store, first)
	seedCommittedNoDeliveryForTest(t, store, second)
	bus, owner := interceptorOutcomeBus(t, store)
	releaseErr := errors.New("first deferred release failure")
	owner.releaseError[first.ID()] = func(int) error { return releaseErr }
	stageInterceptorOutcome(t, bus, first, EventAppendExactDuplicate)
	stageInterceptorOutcome(t, bus, second, EventAppendInserted)
	err := (engineDispatcher{bus: bus}).dispatchCommittedInterceptorPublications(context.Background(), []events.Event{first, second})
	if !errors.Is(err, releaseErr) {
		t.Errorf("lost first deferred failure: %v", err)
	}
	assertInterceptorOutcomeDrained(t, bus, first, second)
	if settlement, ok := owner.settlements[second.ID()]; !ok || !settlement.Successful() {
		t.Errorf("second deferred operation not handed off: found=%t settlement=%+v", ok, settlement)
	}
}

func (i *outcomeTestInterceptor) Intercept(context.Context, events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	i.calls++
	return true, i.emitted, i.outcome, i.err
}

func TestInterceptorCommitErrorProvenance(t *testing.T) {
	cleanupErr := errors.New("acknowledged mutation cleanup")
	laterErr := errors.New("later mutation failed before commit")
	for _, scenario := range []string{"postcommit_only", "later_success", "later_uncommitted_error", "later_invalid_emission", "same_commit_invalid_emission", "later_retry"} {
		t.Run(scenario, func(t *testing.T) {
			eb := &EventBus{}
			evt := receiverProjectionEvent("trigger")
			emitted := receiverProjectionEvent("committed-emission")
			first := &outcomeTestInterceptor{outcome: runtimepipelineobligation.ExecutionOutcome{Committed: true}, emitted: []events.Event{emitted}, err: cleanupErr}
			last := &outcomeTestInterceptor{}
			interceptors := []EventInterceptor{first}
			wantCommitted, wantDeferred := true, 1
			switch scenario {
			case "later_success":
				interceptors = append(interceptors, last)
			case "later_uncommitted_error":
				last.err = laterErr
				interceptors = append(interceptors, last)
				wantCommitted = false
			case "later_invalid_emission":
				last.emitted = []events.Event{{}}
				interceptors = append(interceptors, last)
				wantCommitted = false
			case "same_commit_invalid_emission":
				first.emitted = []events.Event{{}}
				wantCommitted, wantDeferred = false, 0
			case "later_retry":
				last.outcome = runtimepipelineobligation.ReleaseForRetry("later_not_ready", nil)
				interceptors = append(interceptors, last)
				wantCommitted = false
			}
			_, deferred, outcome, err := eb.runInterceptorSet(context.Background(), evt, interceptors)
			if !errors.Is(err, cleanupErr) || outcome.Committed != wantCommitted || len(deferred) != wantDeferred {
				t.Fatalf("outcome=%+v deferred=%d error=%v", outcome, len(deferred), err)
			}
			if wantDeferred == 1 && deferred[0].ID() != emitted.ID() {
				t.Fatal("lost exact prior acknowledged emission")
			}
			if scenario == "later_uncommitted_error" && !errors.Is(err, laterErr) {
				t.Fatalf("lost independent error: %v", err)
			}
			if scenario == "later_retry" {
				if _, retry := outcome.RetryRelease(); !retry {
					t.Fatal("prior commit suppressed later retry disposition")
				}
			}
			if first.calls != 1 || (len(interceptors) == 2 && last.calls != 1) {
				t.Fatal("interceptor mutation replayed or remaining interceptor omitted")
			}
		})
	}
}
