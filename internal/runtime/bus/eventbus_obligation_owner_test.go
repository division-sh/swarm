package bus_test

import (
	"context"
	"strings"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type ownerDeclaringEventStore struct {
	runtimebus.InMemoryEventStore
}

func (ownerDeclaringEventStore) PipelineObligations() runtimepipelineobligation.Store {
	return nil
}

func TestEventBusDurabilityBoundaryRequiresOneDeclaredOwner(t *testing.T) {
	if _, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{}); err == nil ||
		!strings.Contains(err.Error(), "pipeline obligation owner") {
		t.Fatalf("durable constructor error = %v, want missing-owner rejection", err)
	}

	ephemeral, err := runtimebus.NewEphemeralEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("explicit ephemeral constructor: %v", err)
	}
	if _, err := ephemeral.PipelineWorkPresence(context.Background()); err != nil {
		t.Fatalf("ephemeral work presence: %v", err)
	}

	if _, err := runtimebus.NewEphemeralEventBus(ownerDeclaringEventStore{}); err == nil ||
		!strings.Contains(err.Error(), "selected event store") {
		t.Fatalf("ephemeral selected-store error = %v, want durable-boundary rejection", err)
	}
}
