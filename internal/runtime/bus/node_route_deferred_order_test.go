package bus

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type heldNodeRouteOutputProbe struct {
	provider identity.ExecutableNode
	output   events.Event
	held     chan struct{}
	release  chan struct{}
}

func (p *heldNodeRouteOutputProbe) InterceptDeliveryRoute(ctx context.Context, _ events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	if route.Recipient == events.MustNodeDeliveryRecipient(p.provider) {
		return false, []events.Event{p.output}, pipelineobligation.ExecutionOutcome{Committed: true}, nil
	}
	close(p.held)
	select {
	case <-p.release:
		return false, nil, pipelineobligation.Continue(), nil
	case <-ctx.Done():
		return false, nil, pipelineobligation.Continue(), ctx.Err()
	}
}

// This isolates route-loop ordering, not native persistence or fork execution.
func TestInterceptorCommittedOutputWaitsForRemainingNodeRoute(t *testing.T) {
	bus, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatal(err)
	}
	provider, observer := testRootNode(t, "provider"), testRootNode(t, "observer")
	probe := &heldNodeRouteOutputProbe{
		provider: provider, output: receiverProjectionEvent("held-node-output"),
		held: make(chan struct{}), release: make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type routeResult struct {
		outputs []events.Event
		outcome pipelineobligation.ExecutionOutcome
		err     error
	}
	done := make(chan routeResult, 1)
	go func() {
		routes := []events.DeliveryRoute{
			{Recipient: events.MustNodeDeliveryRecipient(provider)},
			{Recipient: events.MustNodeDeliveryRecipient(observer)},
		}
		_, outputs, outcome, err := bus.runNodeDeliveryRouteInterceptors(ctx, receiverProjectionEvent("held-node-input"), routes, []DeliveryRouteInterceptor{probe})
		done <- routeResult{outputs: outputs, outcome: outcome, err: err}
	}()
	select {
	case <-probe.held:
	case result := <-done:
		t.Fatalf("route loop returned before reaching the held observer: %+v", result)
	case <-ctx.Done():
		t.Fatal("route loop did not reach the held observer")
	}
	select {
	case result := <-done:
		t.Fatalf("committed provider output escaped before the observer returned: %+v", result)
	default:
	}
	close(probe.release)
	select {
	case result := <-done:
		if result.err != nil || !result.outcome.Committed || len(result.outputs) != 1 || result.outputs[0].ID() != probe.output.ID() {
			t.Fatalf("route loop lost the exact committed output after observer release: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("route loop did not return after observer release")
	}
}
