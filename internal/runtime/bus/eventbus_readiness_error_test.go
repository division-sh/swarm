package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
)

func TestCommittedRouteDeferralPreservesJoinedFailure(t *testing.T) {
	independent := errors.New("independent readiness cleanup failed")
	for _, tc := range []struct {
		name string
		err  error
		want runtimedeliverycontinuation.DispatchDisposition
	}{
		{name: "transition", err: ErrCommittedAgentRouteTransition, want: runtimedeliverycontinuation.DispatchDeferred},
		{name: "wrapped transition", err: errors.Join(ErrCommittedAgentRouteTransition), want: runtimedeliverycontinuation.DispatchDeferred},
		{name: "transition and cleanup", err: errors.Join(ErrCommittedAgentRouteTransition, independent), want: runtimedeliverycontinuation.DispatchFatal},
		{name: "nested cleanup", err: errors.Join(ErrCommittedAgentRouteTransition, errors.Join(independent)), want: runtimedeliverycontinuation.DispatchFatal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventBus, err := newScopedTestEventBus(InMemoryEventStore{})
			if err != nil {
				t.Fatal(err)
			}
			eventBus.SetCommittedAgentReadinessFinalizer(CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
				return tc.err
			}))
			evt := deliveryContinuationProjectionEvent("readiness-"+tc.name, events.EventType("custom.readiness_error"))
			route := events.DeliveryRoute{
				Recipient:     events.MustAgentDeliveryRecipient("readiness-agent"),
				AgentIdentity: testAgentRouteIdentity(t, "readiness-agent", ""),
			}
			result := eventBus.DispatchDeliveryContinuation(hostilePublisherContext(t), evt, route)
			if result.Disposition() != tc.want {
				t.Fatalf("disposition = %v, failure=%v; want %v", result.Disposition(), result.Failure(), tc.want)
			}
			if tc.want == runtimedeliverycontinuation.DispatchFatal && !errors.Is(result.Failure(), independent) {
				t.Fatalf("fatal result lost independent failure: %v", result.Failure())
			}
		})
	}
}

func TestCommittedRouteDeferralAllowsOnlyOwningCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	independent := errors.New("independent cleanup failed")
	if !onlyCommittedRouteTransition(ctx, errors.Join(ErrCommittedAgentRouteTransition, context.Canceled)) {
		t.Fatal("owning cancellation obscured a pure route transition")
	}
	if onlyCommittedRouteTransition(ctx, errors.Join(ErrCommittedAgentRouteTransition, context.Canceled, independent)) {
		t.Fatal("owning cancellation obscured independent cleanup failure")
	}
	if onlyCommittedRouteTransition(context.Background(), errors.Join(ErrCommittedAgentRouteTransition, context.Canceled)) {
		t.Fatal("unowned cancellation became transient route progress")
	}
}
