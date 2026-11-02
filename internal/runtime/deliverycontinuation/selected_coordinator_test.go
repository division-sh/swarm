package deliverycontinuation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestSelectedContinuationUsesExactSelectedScanAndElection(t *testing.T) {
	normal, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	selected, err := runtimedelivery.NewSelectedExecutionAuthority(normal.SourceArtifact(), uuid.NewString(), uuid.NewString(), 2)
	if err != nil {
		t.Fatal(err)
	}
	event := coordinatorTestEvent("selected-scan")
	route := coordinatorTestAgentRoute(t, "selected-agent")
	id, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	store := &coordinatorTestStore{pages: []runtimedelivery.ContinuationPage{{
		Items: []runtimedelivery.ContinuationItem{{
			DeliveryID: id, Event: event, Disposition: runtimedelivery.ClaimAcquired,
			Snapshot: runtimedelivery.Snapshot{DeliveryID: id, Route: route, Authority: selected, Status: runtimedelivery.StatusPending},
		}}, Exhausted: true,
	}}}
	dispatcher := &coordinatorTestDispatcher{dispatched: make(chan struct{}, 1)}
	dispatchContext := make(chan context.Context, 1)
	releaseDispatch := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseDispatch) })
	defer unblock()
	heldDispatcher := retirementDispatchFunc(func(ctx context.Context, event events.Event, route events.DeliveryRoute) DispatchResult {
		dispatchContext <- ctx
		<-releaseDispatch
		return dispatcher.DispatchDeliveryContinuation(ctx, event, route)
	})
	if _, err := New(store, selected, owner, dispatcher, nil); err == nil {
		t.Fatal("normal constructor accepted selected authority")
	}
	var reconciliations atomic.Int64
	wakes := make(chan struct{}, 1)
	reconcile := func(context.Context) error {
		reconciliations.Add(1)
		select {
		case wakes <- struct{}{}:
		default:
		}
		return nil
	}
	if _, err := NewSelected(store, normal, owner, dispatcher, nil, reconcile); err == nil {
		t.Fatal("selected constructor accepted normal authority")
	}
	coordinator, err := NewSelected(store, selected, owner, heldDispatcher, nil, reconcile)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("selected startup scan: %v", err)
	}
	if reconciliations.Load() == 0 {
		t.Fatal("selected startup skipped barrier recovery")
	}
	defer func() {
		unblock()
		if err := coordinator.Retire(context.Background()); err != nil {
			t.Errorf("retire selected coordinator: %v", err)
		}
	}()
	select {
	case ctx := <-dispatchContext:
		if err := coordinator.Synchronize(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := ctx.Err(); err != nil {
			t.Fatalf("finishing a selected scan cancelled its already-admitted dispatch: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("selected scan did not admit the exact dispatch")
	}
	unblock()
	select {
	case <-dispatcher.dispatched:
	case <-time.After(time.Second):
		t.Fatal("selected scan did not dispatch the exact continuation")
	}
	if dispatcher.callCount() != 1 {
		t.Fatalf("selected startup dispatched %d exact continuations, want 1", dispatcher.callCount())
	}
	if coordinator.Authority() != selected {
		t.Fatal("selected coordinator lost exact execution authority")
	}
	for _, wake := range []func() error{
		func() error { coordinator.Signal(); return nil },
		func() error { return coordinator.Release("terminal-barrier-delivery") },
	} {
		if err := coordinator.Synchronize(context.Background()); err != nil {
			t.Fatal(err)
		}
		select {
		case <-wakes:
		default:
		}
		if err := wake(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-wakes:
		case <-time.After(time.Second):
			t.Fatal("selected wake skipped barrier reconciliation")
		}
	}
}
