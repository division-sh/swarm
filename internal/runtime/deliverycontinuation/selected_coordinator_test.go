package deliverycontinuation

import (
	"context"
	"testing"

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
	if _, err := New(store, coordinatorTestRestarts{}, selected, owner, dispatcher, nil); err == nil {
		t.Fatal("normal constructor accepted selected authority")
	}
	if _, err := NewSelected(store, normal, owner, dispatcher, nil); err == nil {
		t.Fatal("selected constructor accepted normal authority")
	}
	coordinator, err := NewSelected(store, selected, owner, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("selected startup scan: %v", err)
	}
	defer func() {
		if err := coordinator.Retire(context.Background()); err != nil {
			t.Errorf("retire selected coordinator: %v", err)
		}
	}()
	if dispatcher.callCount() != 1 {
		t.Fatalf("selected startup dispatched %d exact continuations, want 1", dispatcher.callCount())
	}
	if coordinator.Authority() != selected {
		t.Fatal("selected coordinator lost exact execution authority")
	}
}
