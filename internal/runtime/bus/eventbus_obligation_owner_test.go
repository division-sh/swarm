package bus_test

import (
	"context"
	"strings"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type ownerDeclaringEventStore struct {
	runtimebus.InMemoryEventStore
}

func (ownerDeclaringEventStore) PipelineObligations() runtimepipelineobligation.Store {
	return nil
}

func TestEventBusDurabilityBoundaryRequiresOneDeclaredOwner(t *testing.T) {
	if _, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{ReceiverExecution: eventreceiver.NormalExecution(), ExecutionPosture: executionposture.Live}); err == nil ||
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

}
