package deliverycontinuation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
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
	t.Run("shutdown_fence_drains_accepted_dispatch_before_terminal_result", func(t *testing.T) {
		process := worklifetime.NewProcess()
		selectedOwner, err := process.NewSelectedFork(t.Context(), worklifetime.SelectedForkIdentity{
			ExecutionID: selected.ExecutionID(), RunID: selected.ForkRunID(), Generation: selected.Generation(),
		})
		if err != nil {
			t.Fatal(err)
		}
		entered := make(chan context.Context, 1)
		release := make(chan struct{})
		returned := make(chan error, 1)
		unblock := sync.OnceFunc(func() { close(release) })
		reported := make(chan error, 1)
		store := &coordinatorTestStore{pages: []runtimedelivery.ContinuationPage{{
			Items: []runtimedelivery.ContinuationItem{{
				DeliveryID: id, Event: event, Disposition: runtimedelivery.ClaimAcquired,
				Snapshot: runtimedelivery.Snapshot{DeliveryID: id, Route: route, Authority: selected, Status: runtimedelivery.StatusPending},
			}}, Exhausted: true,
		}}}
		held := retirementDispatchFunc(func(ctx context.Context, _ events.Event, _ events.DeliveryRoute) DispatchResult {
			entered <- ctx
			select {
			case <-release:
			case <-ctx.Done():
			}
			returned <- ctx.Err()
			return Transferred()
		})
		c, err := NewSelected(store, selected, selectedOwner, held, func(_ context.Context, err error) { reported <- err }, func(context.Context) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			unblock()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := c.Retire(ctx); err != nil {
				t.Error(err)
			}
			if err := selectedOwner.RetireAndWait(ctx); err != nil {
				t.Error(err)
			}
			process.Retire()
			if _, err := process.Join(ctx); err != nil {
				t.Error(err)
			}
		})
		if err := c.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		var dispatchCtx context.Context
		select {
		case dispatchCtx = <-entered:
		case <-time.After(time.Second):
			t.Fatal("selected dispatch never acquired its exact lease")
		}
		if err := selectedOwner.Fence(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		refused := c.Synchronize(ctx)
		var refusal *retirementRefusal
		if !errors.As(refused, &refusal) || !errors.Is(refused, errCoordinatorRetired) {
			t.Fatalf("fresh scan lost its typed retirement refusal: %v", refused)
		}
		select {
		case <-dispatchCtx.Done():
			t.Fatal("fresh admission refusal canceled the accepted dispatch")
		case <-c.done:
			t.Fatal("terminal result preceded the accepted dispatch join")
		case <-time.After(10 * time.Millisecond):
		}
		unblock()
		if err := <-returned; err != nil {
			t.Fatalf("accepted dispatch did not complete under its original lease: %v", err)
		}
		if err := c.Retire(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-reported:
			t.Fatalf("retirement refusal became terminal dispatch failure: %v", err)
		default:
		}
	})
}
